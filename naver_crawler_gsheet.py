
import asyncio
import json
import re
import pandas as pd
from playwright.async_api import async_playwright
from datetime import datetime
import gspread
from oauth2client.service_account import ServiceAccountCredentials
import time

# 구글 스프레드시트 설정
SPREADSHEET_URL = "https://docs.google.com/spreadsheets/d/1hJoa1sjXbFkJlKeSEwQDW2YKOLDXBaaxfP_FyiyhYEQ/edit?gid=0#gid=0"

# 크롤링 대상 브랜드 카테고리 URL 목록
CATEGORY_URLS = [
    "https://smartstore.naver.com/lux_man/category/f8d2479aeafe4bc28c304897192e134e",
    "https://smartstore.naver.com/lux_man/category/f963d2e4d9fb4893a2b6e6c469deea6e",
    "https://smartstore.naver.com/lux_man/category/819042c8ea514a3f852d64404b40dbbc",
    "https://smartstore.naver.com/lux_man/category/3558cdfb63ac450fbf9ff4f411f7c16c",
    "https://smartstore.naver.com/lux_man/category/20c5015a298140a8974baa1f569fe46d",
    "https://smartstore.naver.com/lux_man/category/da077f06b56a4bd68c675828b536a7c5",
    "https://smartstore.naver.com/lux_man/category/96c40e5b814047c89ae5d8a1b1248af7",
    "https://smartstore.naver.com/lux_man/category/4294caa635544960b48c4677dee734ab",
    "https://smartstore.naver.com/lux_man/category/3ffa043b02854897a62bf641112e48a3",
]

def save_to_google_sheets(results):
    if not results:
        print("수집된 데이터가 없어 구글 시트에 기록하지 않습니다.")
        return

    try:
        scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
        creds = ServiceAccountCredentials.from_json_keyfile_name("credentials.json", scope)
        client = gspread.authorize(creds)

        doc = client.open_by_url(SPREADSHEET_URL)
        sheet = doc.get_worksheet(0)

        # 기존 데이터에서 (스토어, 상품코드) 조합 추출해 중복 체크용 집합 생성
        existing_rows = sheet.get_all_values()
        # 헤더 행이 있으면 제외, 없으면 전체 사용
        data_rows = existing_rows[1:] if existing_rows and existing_rows[0][0] in ("스토어", "Store") else existing_rows
        # col 0: 스토어, col 4: 상품코드
        existing_keys = set()
        for row in data_rows:
            if len(row) >= 5:
                existing_keys.add((row[0].strip(), row[4].strip()))
        print(f"기존 시트 데이터: {len(existing_keys)}개 (스토어+상품코드 기준)")

        new_values = []
        skipped = 0
        for item in results:
            store = str(item.get("스토어", ""))
            code = str(item.get("상품코드", ""))
            if (store, code) in existing_keys:
                skipped += 1
                continue
            row = [
                store,
                item.get("브랜드명", ""),
                item.get("할인율", ""),
                item.get("상품명", ""),
                code,
                item.get("할인가", ""),
                item.get("원가", ""),
                item.get("상품상세페이지링크", ""),
                item.get("수집일시", "")
            ]
            new_values.append(list(map(str, row)))
            existing_keys.add((store, code))  # 같은 실행 내 중복도 방지

        print(f"중복 제외: {skipped}개 / 신규 추가 대상: {len(new_values)}개")

        if new_values:
            sheet.append_rows(new_values)
            print(f"구글 스프레드시트에 {len(new_values)}개의 MZ아울렛 신규 상품을 기록했습니다.")
        else:
            print("신규 상품이 없어 시트에 기록하지 않습니다.")
    except Exception as e:
        print(f"구글 스프레드시트 기록 중 에러 발생: {e}")

async def crawl_category(page, category_url):
    """단일 카테고리의 전 상품을 수집"""
    category_id = category_url.rstrip("/").split("/")[-1]
    results = []
    current_page = 1
    total_count = None

    while True:
        url = f"{category_url}?cp={current_page}"
        print(f"  [{current_page}] 페이지 접속 중: {url}")

        try:
            await page.goto(url, wait_until="networkidle", timeout=60000)
            await page.wait_for_timeout(1000)

            content = await page.content()

            start_str = "window.__PRELOADED_STATE__="
            idx = content.find(start_str)
            if idx == -1:
                print("  JSON 데이터를 찾을 수 없습니다. (종료 혹은 차단)")
                break

            start_idx = idx + len(start_str)
            end_idx = content.find("</script>", start_idx)
            json_str = content[start_idx:end_idx].strip()
            if json_str.endswith(";"): json_str = json_str[:-1]

            data = json.loads(json_str)
            # 카테고리 데이터 키를 동적으로 탐색
            category_data = data.get('category', {})
            # 첫 번째 키 값을 사용 (ALL 또는 카테고리 ID 등)
            category_key = next(iter(category_data), None)
            if category_key is None:
                print("  카테고리 데이터 없음.")
                break

            cat_info = category_data[category_key]
            products = cat_info.get('simpleProducts', [])

            if total_count is None:
                total_count = cat_info.get('totalCount', 0)
                print(f"  카테고리 총 상품 개수: {total_count}")

            if not products:
                print(f"  [{current_page}] 페이지에 상품이 없습니다. (수집 완료)")
                break

            for p_item in products:
                p_name = p_item.get('name', '')
                brand = p_name.split(' ')[0] if p_name else ""
                product_code = p_name.split(' ')[-1] if p_name else ""
                original_price = p_item.get('salePrice', 0)
                benefits = p_item.get('benefitsView', {})
                sale_price = benefits.get('discountedSalePrice', 0) or original_price
                discount_rate = benefits.get('discountedRatio', 0)
                p_id = p_item.get('id', '')

                results.append({
                    "스토어": "MZ아울렛",
                    "브랜드명": brand,
                    "할인율": f"{discount_rate}%" if discount_rate else "0%",
                    "상품명": p_name,
                    "상품코드": product_code,
                    "할인가": f"{sale_price:,}" if sale_price else "0",
                    "원가": f"{original_price:,}" if original_price else "0",
                    "상품상세페이지링크": f"https://smartstore.naver.com/lux_man/products/{p_id}",
                    "수집일시": datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                })

            print(f"  현재까지 {len(results)}개 수집됨.")

            # 마지막 페이지 여부 확인
            if current_page * 40 >= total_count:
                print("  카테고리 전 상품 수집 완료.")
                break

            current_page += 1

        except Exception as e:
            print(f"  [{current_page}] 페이지 처리 중 에러: {e}")
            break

    return results

async def crawl_naver_store():
    all_results = []

    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True)
        context = await browser.new_context(
            user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
            viewport={'width': 1920, 'height': 1080}
        )
        page = await context.new_page()

        for i, category_url in enumerate(CATEGORY_URLS, 1):
            print(f"\n[카테고리 {i}/{len(CATEGORY_URLS)}] {category_url}")
            category_results = await crawl_category(page, category_url)
            all_results.extend(category_results)
            print(f"카테고리 {i} 수집 완료: {len(category_results)}개 (누적: {len(all_results)}개)")

        await browser.close()

    print(f"\n전체 수집 완료: 총 {len(all_results)}개")

    if all_results:
        save_to_google_sheets(all_results)
    else:
        print("수집된 데이터가 없습니다.")

if __name__ == "__main__":
    asyncio.run(crawl_naver_store())
