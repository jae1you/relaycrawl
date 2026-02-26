import asyncio
import json
import re
from playwright.async_api import async_playwright
from datetime import datetime
from gsheet_utils import save_to_google_sheets

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

PAGE_SIZE = 40


async def fetch_brand_products(page, brand_cd):
    results = []
    offset = 0
    total_count = None
    now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    while True:
        api_url = (
            f"https://hapix.halfclub.com/searches/prdList/"
            f"?limit={offset},{PAGE_SIZE}&sortSeq=12&siteCd=1&device=pc&brandCd={brand_cd}"
        )
        print(f"    API 호출: offset={offset} / total={total_count}")

        try:
            await page.goto(api_url, wait_until="load", timeout=30000)
            content = await page.content()

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

            for item in products:
                src = item.get('_source', {})
                norm_price = src.get('normPrc', 0)
                sel_price = src.get('selPrc', 0)
                dc_price = src.get('dcPrcMc', 0) or sel_price
                tot_rate = src.get('totRateMc', 0)
                p_no = src.get('prdNo', '')

                results.append({
                    "스토어": "하프클럽",
                    "브랜드명": src.get('brandNm', ''),
                    "할인율": f"{tot_rate}%" if tot_rate else "0%",
                    "상품명": src.get('prdNm', ''),
                    "상품코드": src.get('prdCd', src.get('pcode', '')),
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
        save_to_google_sheets(all_results, "하프클럽")
    else:
        print("수집된 데이터가 없습니다.")


if __name__ == "__main__":
    asyncio.run(crawl_halfclub())
