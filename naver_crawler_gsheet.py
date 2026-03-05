import asyncio
import json
import logging
from playwright.async_api import async_playwright
from playwright_stealth import Stealth
from datetime import datetime
from gsheet_utils import save_to_google_sheets

# 로그 설정
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)

STORE_URL = "https://smartstore.naver.com/lux_man/category/ALL"


async def crawl_naver_store():
    all_results = []
    current_page = 1
    total_count = None
    now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    logger.info("네이버 MZ아울렛 크롤링 시작...")

    async with async_playwright() as p:
        async with Stealth().use_async(p):
            browser = await p.chromium.launch(headless=True)
            context = await browser.new_context(
                user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
                viewport={'width': 1920, 'height': 1080}
            )
            page = await context.new_page()

            while True:
                url = f"{STORE_URL}?cp={current_page}"
                logger.info(f"[{current_page}] 페이지 접속 중: {url}")

                try:
                    await page.goto(url, wait_until="networkidle", timeout=60000)
                    
                    # 페이지 제목 확인 (차단 여부 확인용)
                    title = await page.title()
                    logger.info(f"페이지 제목: {title}")
                    
                    if "차단" in title or "Forbidden" in title:
                        logger.error("네이버에서 접근이 차단되었습니다.")
                        break

                    content = await page.content()

                    start_str = "window.__PRELOADED_STATE__="
                    idx = content.find(start_str)
                    if idx == -1:
                        logger.warning("JSON 데이터를 찾을 수 없습니다. (종료 혹은 차단 가능성)")
                        # 디버깅을 위해 스크린샷 저장 (CI 환경용)
                        if current_page == 1:
                            await page.screenshot(path="naver_block_debug.png")
                            logger.info("디버그용 스크린샷 저장 완료: naver_block_debug.png")
                        break

                    # JSON 추출 logic 개선: 다음 <script> 태그나 변수 선언 전까지 추출
                    json_part = content[idx + len(start_str):]
                    end_idx = json_part.find("</script>")
                    if end_idx != -1:
                        json_str = json_part[:end_idx].strip().rstrip(";")
                    else:
                        json_str = json_part.strip().rstrip(";")

                    data = json.loads(json_str)

                    category_data = data.get('category', {})
                    category_key = next(iter(category_data), None)
                    if category_key is None:
                        logger.warning("카테고리 데이터가 없습니다.")
                        break

                    cat_info = category_data[category_key]
                    products = cat_info.get('simpleProducts', [])

                    if total_count is None:
                        total_count = cat_info.get('totalCount', 0)
                        logger.info(f"전체 상품 개수: {total_count}")

                    if not products:
                        logger.info(f"[{current_page}] 페이지에 상품이 없습니다. (수집 완료)")
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

                    logger.info(f"현재까지 {len(all_results)}개 수집됨.")

                    if current_page * 40 >= total_count:
                        logger.info("전체 상품 수집 완료.")
                        break

                    current_page += 1
                    await asyncio.sleep(1) # 부하 조절

                except Exception as e:
                    logger.error(f"[{current_page}] 페이지 처리 중 에러: {e}")
                    break

            await browser.close()

    logger.info(f"전체 수집 완료: 총 {len(all_results)}개")

    if all_results:
        save_to_google_sheets(all_results, "MZ아울렛")
    else:
        logger.warning("수집된 데이터가 없습니다.")


if __name__ == "__main__":
    asyncio.run(crawl_naver_store())
