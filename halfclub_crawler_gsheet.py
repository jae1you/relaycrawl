
import asyncio
import json
import re
from playwright.async_api import async_playwright
from datetime import datetime
import gspread
from oauth2client.service_account import ServiceAccountCredentials

# 구글 스프레드시트 설정
SPREADSHEET_URL = "https://docs.google.com/spreadsheets/d/1hJoa1sjXbFkJlKeSEwQDW2YKOLDXBaaxfP_FyiyhYEQ/edit?gid=0#gid=0"

# 크롤링 대상 브랜드 URL 목록 (중복 제거)
BRAND_URLS = [
    "https://www.halfclub.com/brand/BL110209",
    "https://www.halfclub.com/brand/BL301331",
    "https://www.halfclub.com/brand/BL124279",
    "https://www.halfclub.com/brand/BL100889",
    "https://www.halfclub.com/brand/BL100893",
    "https://www.halfclub.com/brand/BL121603",
    "https://www.halfclub.com/brand/BL102456",
    "https://www.halfclub.com/brand/BL309447",
    "https://www.halfclub.com/brand/BL308826",
    "https://www.halfclub.com/brand/BL107280",
    "https://www.halfclub.com/brand/BL102196",
    "https://www.halfclub.com/brand/BL338943",
]

PAGE_SIZE = 40  # 한 번에 가져올 상품 수

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
            print(f"구글 스프레드시트에 {len(new_values)}개의 하프클럽 신규 상품을 기록했습니다.")
        else:
            print("신규 상품이 없어 시트에 기록하지 않습니다.")
    except Exception as e:
        print(f"구글 스프레드시트 기록 중 에러 발생: {e}")

async def fetch_brand_products(page, brand_cd):
    """단일 브랜드의 전 상품을 API로 수집"""
    results = []
    offset = 0
    total_count = None

    while True:
        api_url = (
            f"https://hapix.halfclub.com/searches/prdList/"
            f"?limit={offset},{PAGE_SIZE}&sortSeq=12&siteCd=1&device=pc&brandCd={brand_cd}"
        )
        print(f"    API 호출: offset={offset} / total={total_count}")

        try:
            response = await page.goto(api_url, wait_until="load", timeout=30000)
            content = await page.content()

            # <pre> 태그 안의 JSON 추출
            match = re.search(r'<pre[^>]*>(.*?)</pre>', content, re.DOTALL)
            if not match:
                print("    JSON 응답을 찾을 수 없습니다.")
                break

            data = json.loads(match.group(1))
            hits = data.get('data', {}).get('result', {}).get('hits', {})

            if total_count is None:
                total_count = hits.get('total', {}).get('value', 0)
                print(f"    브랜드 총 상품 수: {total_count}")

            products = hits.get('hits', [])
            if not products:
                print("    더 이상 상품이 없습니다.")
                break

            now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            for item in products:
                src = item.get('_source', {})
                p_name = src.get('prdNm', '')
                brand_nm = src.get('brandNm', '')
                product_code = src.get('prdCd', src.get('pcode', ''))
                norm_price = src.get('normPrc', 0)   # 정상가
                sel_price = src.get('selPrc', 0)     # 판매가(할인전)
                dc_price = src.get('dcPrcMc', 0) or sel_price  # 최종 할인가
                tot_rate = src.get('totRateMc', 0)   # 총 할인율
                p_no = src.get('prdNo', '')

                results.append({
                    "스토어": "하프클럽",
                    "브랜드명": brand_nm,
                    "할인율": f"{tot_rate}%" if tot_rate else "0%",
                    "상품명": p_name,
                    "상품코드": product_code,
                    "할인가": f"{dc_price:,}" if dc_price else "0",
                    "원가": f"{norm_price:,}" if norm_price else "0",
                    "상품상세페이지링크": f"https://www.halfclub.com/product/{p_no}",
                    "수집일시": now
                })

            offset += PAGE_SIZE
            if offset >= total_count:
                print(f"    브랜드 전 상품 수집 완료: {len(results)}개")
                break

            await page.wait_for_timeout(300)

        except Exception as e:
            print(f"    에러 발생 (offset={offset}): {e}")
            break

    return results

async def crawl_halfclub():
    all_results = []

    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True)
        context = await browser.new_context(
            user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
            viewport={'width': 1920, 'height': 1080}
        )
        page = await context.new_page()

        for i, brand_url in enumerate(BRAND_URLS, 1):
            brand_cd = brand_url.rstrip("/").split("/")[-1]
            print(f"\n[브랜드 {i}/{len(BRAND_URLS)}] {brand_cd} ({brand_url})")

            brand_results = await fetch_brand_products(page, brand_cd)
            all_results.extend(brand_results)
            print(f"  브랜드 {brand_cd} 완료: {len(brand_results)}개 (누적: {len(all_results)}개)")

        await browser.close()

    print(f"\n전체 수집 완료: 총 {len(all_results)}개")

    if all_results:
        save_to_google_sheets(all_results)
    else:
        print("수집된 데이터가 없습니다.")

if __name__ == "__main__":
    asyncio.run(crawl_halfclub())
