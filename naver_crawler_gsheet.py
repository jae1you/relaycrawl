import asyncio
import json
from playwright.async_api import async_playwright
from datetime import datetime
from gsheet_utils import save_to_google_sheets

STORE_URL = "https://smartstore.naver.com/lux_man/category/ALL"


async def crawl_naver_store():
    all_results = []
    current_page = 1
    total_count = None
    now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True)
        context = await browser.new_context(
            user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
            viewport={'width': 1920, 'height': 1080}
        )
        page = await context.new_page()

        while True:
            url = f"{STORE_URL}?cp={current_page}"
            print(f"[{current_page}] 페이지 접속 중: {url}")

            try:
                await page.goto(url, wait_until="networkidle", timeout=60000)
                content = await page.content()

                start_str = "window.__PRELOADED_STATE__="
                idx = content.find(start_str)
                if idx == -1:
                    print("JSON 데이터를 찾을 수 없습니다. (종료 혹은 차단)")
                    break

                json_str = content[idx + len(start_str):content.find("</script>", idx)].strip().rstrip(";")
                data = json.loads(json_str)

                category_data = data.get('category', {})
                category_key = next(iter(category_data), None)
                if category_key is None:
                    print("카테고리 데이터 없음.")
                    break

                cat_info = category_data[category_key]
                products = cat_info.get('simpleProducts', [])

                if total_count is None:
                    total_count = cat_info.get('totalCount', 0)
                    print(f"전체 상품 개수: {total_count}")

                if not products:
                    print(f"[{current_page}] 페이지에 상품이 없습니다. (수집 완료)")
                    break

                for p_item in products:
                    p_name = p_item.get('name', '')
                    original_price = p_item.get('salePrice', 0)
                    benefits = p_item.get('benefitsView', {})
                    sale_price = benefits.get('discountedSalePrice', 0) or original_price
                    discount_rate = benefits.get('discountedRatio', 0)
                    p_id = p_item.get('id', '')

                    all_results.append({
                        "스토어": "MZ아울렛",
                        "브랜드명": p_name.split(' ')[0] if p_name else "",
                        "할인율": f"{discount_rate}%" if discount_rate else "0%",
                        "상품명": p_name,
                        "상품코드": p_name.split(' ')[-1] if p_name else "",
                        "할인가": f"{sale_price:,}" if sale_price else "0",
                        "원가": f"{original_price:,}" if original_price else "0",
                        "상품상세페이지링크": f"https://smartstore.naver.com/lux_man/products/{p_id}",
                        "수집일시": now
                    })

                print(f"현재까지 {len(all_results)}개 수집됨.")

                if current_page * 40 >= total_count:
                    print("전체 상품 수집 완료.")
                    break

                current_page += 1

            except Exception as e:
                print(f"[{current_page}] 페이지 처리 중 에러: {e}")
                break

        await browser.close()

    print(f"\n전체 수집 완료: 총 {len(all_results)}개")

    if all_results:
        save_to_google_sheets(all_results, "MZ아울렛")
    else:
        print("수집된 데이터가 없습니다.")


if __name__ == "__main__":
    asyncio.run(crawl_naver_store())
