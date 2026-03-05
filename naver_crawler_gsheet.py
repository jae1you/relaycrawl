import asyncio
import json
import logging
import random
import re
from datetime import datetime

from playwright.async_api import async_playwright
from playwright_stealth import Stealth

from gsheet_utils import save_to_google_sheets

# 로그 설정
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)

STORE_URLS = [
    "https://smartstore.naver.com/lux_man/category/ALL",
    "https://m.smartstore.naver.com/lux_man/category/ALL",
    "https://brand.naver.com/lux_man/category/ALL",
]
PRODUCTS_PER_PAGE = 40
MAX_PAGE_RETRY = 6
CODE_RE = re.compile(r'^(?=.*[A-Za-z])(?=.*\d)[A-Za-z\d]{6,20}$')


def _extract_json_object(text, marker):
    marker_idx = text.find(marker)
    if marker_idx == -1:
        return None

    start = text.find("{", marker_idx)
    if start == -1:
        return None

    depth = 0
    in_string = False
    escaped = False

    for i in range(start, len(text)):
        ch = text[i]

        if in_string:
            if escaped:
                escaped = False
            elif ch == "\\":
                escaped = True
            elif ch == '"':
                in_string = False
            continue

        if ch == '"':
            in_string = True
        elif ch == "{":
            depth += 1
        elif ch == "}":
            depth -= 1
            if depth == 0:
                return text[start:i + 1]

    return None


def _walk_dict_paths(data, paths):
    for path in paths:
        node = data
        ok = True
        for key in path:
            if not isinstance(node, dict) or key not in node:
                ok = False
                break
            node = node[key]
        if ok:
            return node
    return None


def _parse_products_from_state(state):
    if not isinstance(state, dict):
        return [], None

    # 최신 스마트스토어 구조: categoryProducts.simpleProducts
    category_products = state.get("categoryProducts", {})
    if isinstance(category_products, dict):
        cp_products = category_products.get("simpleProducts", []) or []
        cp_total = category_products.get("totalCount")
        if isinstance(cp_products, list) and cp_products:
            return cp_products, cp_total

    # 구형 구조: category.{key}.simpleProducts
    category_root = state.get("category", {})
    category_node = category_root.get("A") if isinstance(category_root, dict) and "A" in category_root else None
    if category_node is None and isinstance(category_root, dict) and category_root:
        first_key = next(iter(category_root), None)
        category_node = category_root.get(first_key) if first_key is not None else None

    products = []
    total_count = None

    if isinstance(category_node, dict):
        products = category_node.get("simpleProducts", []) or []
        total_count = category_node.get("totalCount")
        if products:
            return products, total_count

    # 추가 fallback
    fallback_products = _walk_dict_paths(
        state,
        [
            ("smartStore", "category", "product", "list", "content"),
            ("smartStore", "category", "product", "simpleItemList", "content"),
            ("search", "products"),
        ],
    )
    if isinstance(fallback_products, list):
        products = fallback_products

    fallback_total = _walk_dict_paths(
        state,
        [
            ("smartStore", "category", "product", "list", "totalCount"),
            ("smartStore", "category", "product", "simpleItemList", "totalCount"),
        ],
    )
    if isinstance(fallback_total, int):
        total_count = fallback_total

    return products, total_count


def _split_name_fields(name):
    text = (name or "").strip()
    if not text:
        return "", ""

    tokens = text.split()
    brand = tokens[0] if tokens else ""
    code = ""

    if tokens:
        tail = re.sub(r"[^A-Za-z0-9]", "", tokens[-1])
        if CODE_RE.match(tail):
            code = tail

    return brand, code


async def _extract_state(page):
    try:
        state = await page.evaluate("() => window.__PRELOADED_STATE__ || null")
        if isinstance(state, dict):
            return state
    except Exception:
        pass

    content = await page.content()
    json_text = _extract_json_object(content, "window.__PRELOADED_STATE__=")
    if not json_text:
        json_text = _extract_json_object(content, "window.__PRELOADED_STATE__ =")
    if not json_text:
        return None

    try:
        return json.loads(json_text)
    except Exception:
        return None


def _is_blocked_or_error(title, html):
    t = (title or "").lower()
    if any(x in t for x in ["forbidden", "access denied", "에러", "오류", "차단"]):
        return True

    body = (html or "")[:5000].lower()
    return any(x in body for x in ["시스템오류", "forbidden", "captcha", "차단"])


def _build_page_urls(page_no):
    urls = []
    for base in STORE_URLS:
        sep = "&" if "?" in base else "?"
        urls.append(f"{base}{sep}cp={page_no}")
    return urls


async def _new_context(browser):
    context = await browser.new_context(
        user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
        viewport={"width": 1920, "height": 1080},
        locale="ko-KR",
        timezone_id="Asia/Seoul",
        extra_http_headers={
            "Accept-Language": "ko-KR,ko;q=0.9,en-US;q=0.8,en;q=0.7",
            "DNT": "1",
            "Upgrade-Insecure-Requests": "1",
        },
    )
    await context.add_init_script(
        """
        Object.defineProperty(navigator, 'webdriver', { get: () => undefined });
        window.chrome = window.chrome || { runtime: {} };
        Object.defineProperty(navigator, 'languages', { get: () => ['ko-KR', 'ko', 'en-US', 'en'] });
        """
    )
    return context


async def crawl_naver_store():
    all_results = []
    current_page = 1
    total_count = None
    now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    logger.info("네이버 MZ아울렛 크롤링 시작...")

    async with Stealth().use_async(async_playwright()) as p:
        browser = await p.chromium.launch(headless=True)
        context = await _new_context(browser)
        page = await context.new_page()

        while True:
            page_urls = _build_page_urls(current_page)
            logger.info(f"[{current_page}] 페이지 접속 후보: {page_urls}")

            state = None
            html = ""

            for attempt in range(1, MAX_PAGE_RETRY + 1):
                try:
                    # 반복 차단 회피를 위해 주기적으로 컨텍스트/페이지 재생성
                    if attempt in (3, 5):
                        try:
                            await page.close()
                        except Exception:
                            pass
                        try:
                            await context.close()
                        except Exception:
                            pass
                        context = await _new_context(browser)
                        page = await context.new_page()

                    url = page_urls[(attempt - 1) % len(page_urls)]
                    await page.goto(url, wait_until="domcontentloaded", timeout=60000)
                    await page.wait_for_timeout(1200 + random.randint(0, 1400))

                    title = await page.title()
                    html = await page.content()
                    logger.info(f"[{current_page}] 페이지 제목: {title} (시도 {attempt}/{MAX_PAGE_RETRY})")

                    if _is_blocked_or_error(title, html):
                        logger.warning(f"[{current_page}] 오류/차단 페이지 감지")
                        if attempt == MAX_PAGE_RETRY:
                            await page.screenshot(path=f"naver_block_p{current_page}.png")
                            break
                        await asyncio.sleep(2 + attempt)
                        continue

                    state = await _extract_state(page)
                    if state:
                        break

                    logger.warning(f"[{current_page}] PRELOADED_STATE 추출 실패 (시도 {attempt}/{MAX_PAGE_RETRY})")
                    if attempt == MAX_PAGE_RETRY:
                        await page.screenshot(path=f"naver_state_fail_p{current_page}.png")
                        with open(f"naver_state_fail_p{current_page}.html", "w", encoding="utf-8") as f:
                            f.write(html)
                        break

                    await asyncio.sleep(1 + attempt)

                except Exception as e:
                    logger.error(f"[{current_page}] 페이지 로드 에러 (시도 {attempt}/{MAX_PAGE_RETRY}): {e}")
                    if attempt == MAX_PAGE_RETRY:
                        await page.screenshot(path=f"naver_error_p{current_page}.png")
                        break
                    await asyncio.sleep(1 + attempt)

            if not state:
                logger.error(f"[{current_page}] 페이지 데이터를 추출하지 못해 크롤링을 종료합니다.")
                break

            products, parsed_total = _parse_products_from_state(state)

            if total_count is None:
                total_count = parsed_total or 0
                logger.info(f"전체 상품 개수: {total_count}")

            if not products:
                logger.info(f"[{current_page}] 페이지에 상품이 없습니다. (수집 완료)")
                break

            for p_item in products:
                p_name = p_item.get("name", "")
                original_price = p_item.get("salePrice", 0) or 0
                benefits = p_item.get("benefitsView", {}) or {}
                sale_price = benefits.get("discountedSalePrice", 0) or original_price
                discount_rate = benefits.get("discountedRatio", 0) or 0
                p_id = p_item.get("id", "") or p_item.get("productNo", "")

                brand, product_code = _split_name_fields(p_name)

                all_results.append(
                    {
                        "스토어": "MZ아울렛",
                        "브랜드명": brand,
                        "할인율": f"{discount_rate}%" if discount_rate else "0%",
                        "상품명": p_name,
                        "상품코드": product_code,
                        "할인가": f"{int(sale_price):,}" if sale_price else "0",
                        "원가": f"{int(original_price):,}" if original_price else "0",
                        "상품상세페이지링크": f"https://smartstore.naver.com/lux_man/products/{p_id}" if p_id else "",
                        "수집일시": now,
                    }
                )

            logger.info(f"현재까지 {len(all_results)}개 수집됨.")

            if total_count and len(all_results) >= total_count:
                logger.info("전체 상품 수집 완료.")
                break

            if len(products) < PRODUCTS_PER_PAGE:
                logger.info("마지막 페이지 도달로 판단되어 수집을 종료합니다.")
                break

            current_page += 1
            await asyncio.sleep(1)

        await browser.close()

    logger.info(f"전체 수집 완료: 총 {len(all_results)}개")

    if all_results:
        save_to_google_sheets(all_results, "MZ아울렛")
    else:
        logger.warning("수집된 데이터가 없습니다.")


if __name__ == "__main__":
    asyncio.run(crawl_naver_store())

