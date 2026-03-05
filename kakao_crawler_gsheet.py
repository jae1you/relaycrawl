import asyncio
import json
import logging
import os
import re
from datetime import datetime

from openai import OpenAI
from playwright.async_api import async_playwright
from playwright_stealth import Stealth

from gsheet_utils import save_to_google_sheets

# 로그 설정
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)

CATEGORY_URL = "https://store.kakao.com/kolonsaveplaza/category?sort=POPULAR_SALE_COUNT&showMenu=false"
BASE_URL = "https://store.kakao.com"

OPENAI_API_KEY = os.environ.get("OPENAI_API_KEY", "").strip()
openai_client = OpenAI(api_key=OPENAI_API_KEY) if OPENAI_API_KEY else None

CODE_RE = re.compile(r'^(?=.*[A-Za-z])(?=.*[0-9])[A-Za-z0-9]{13,14}$')
CODE_IN_PAREN = re.compile(r'\(([A-Za-z0-9]{13,14})\)')
PREFIX_TAG = re.compile(r'^(\[[^\]]+\]|\([^)]+\))\s*')
PROMO_RE = re.compile(r'%|BEST|특가|기획전|카탈로그', re.IGNORECASE)
DISCOUNT_RATE_RE = re.compile(r'discountRate=(\d+)%')

NOISE_TAGS = {'단독특가', '특가', '할인', '쿠폰', '타임세일', '한정', '추가할인', '신상', '세일'}

SYSTEM_PROMPT = """당신은 패션 쇼핑몰 상품 제목에서 브랜드명, 상품명, 상품코드를 추출하는 전문가입니다.

카카오 세이브프라자(콜론세이브플라자) 상품 제목의 특징:
- 일반적인 형식: "브랜드명 / 상품명 / 상품코드"
- 상품코드는 보통 영문+숫자 혼합 13자리 (예: TLTBW23591MML, LNTAW23151WIX)
- 브랜드명이 없는 경우: "상품명 / 상품코드" 형식
- 상품코드가 없는 경우: "브랜드명 / 상품명" 형식
- 브랜드명 앞에 태그가 붙는 경우: "[단독특가] 브랜드명", "(할인) 브랜드명" 등 → 태그 제거 후 브랜드명만 추출
- 대괄호 안에 브랜드가 있는 경우: "[KOLON SPORT] 상품명 (상품코드)" 형식
- 상품코드가 괄호 안에 있는 경우: "상품명 (JKJDW19412DKH)" 형식
- 제목 끝에 "/정가xxx원" 또는 "단독특가" 같은 부가 정보가 붙는 경우도 있음 → 무시
- 브랜드명이 소문자로 표기되는 경우도 있음 (예: epigram, hideout, iro)

추출 규칙:
1. 브랜드명: 패션 브랜드 이름만 추출 (태그, 할인 문구, 특가 문구 제거)
2. 상품명: 실제 상품을 설명하는 이름 (브랜드명, 상품코드, 태그, 부가 정보 제외)
3. 상품코드: 영문+숫자 혼합 13자리 코드. 없으면 "미기재"
4. 브랜드명이 없으면 "미기재", 상품명이 없으면 "미기재"

반드시 아래 JSON 형식으로만 응답하세요:
{"brand": "브랜드명", "product_name": "상품명", "product_code": "상품코드"}"""


def parse_title_by_logic(full_title):
    title = (full_title or "").strip()
    if not title:
        return ("미기재", "미기재", "미기재")

    if PROMO_RE.search(title):
        return ("미기재", title, "미기재")

    title = re.sub(r'/?\s*정가\s*[\d,]+원.*$', '', title).strip()
    title = re.sub(r'\s*(단독특가|특가|할인)\s*$', '', title).strip()

    paren_code = None
    paren_match = CODE_IN_PAREN.search(title)
    if paren_match:
        paren_code = paren_match.group(1)
        title = (title[:paren_match.start()] + title[paren_match.end():]).strip().rstrip('/').strip()

    parts = [p.strip() for p in title.split('/') if p and p.strip()]
    if not parts:
        return None

    brand = None
    first = parts[0]

    bracket_match = re.match(r'^\[([^\]]+)\]\s*(.*)', first)
    if bracket_match:
        tag_content = bracket_match.group(1).strip()
        remainder = bracket_match.group(2).strip()
        is_noise = tag_content in NOISE_TAGS or (
            bool(re.search(r'[가-힣]', tag_content)) and not bool(re.search(r'[A-Za-z]', tag_content))
        )
        if is_noise:
            if remainder:
                parts[0] = remainder
            else:
                parts = parts[1:]
        else:
            brand = tag_content
            if remainder:
                parts[0] = remainder
            else:
                parts = parts[1:]
    else:
        tag_match = PREFIX_TAG.match(first)
        if tag_match:
            after_tag = first[tag_match.end():].strip()
            if after_tag:
                parts[0] = after_tag
            else:
                parts = parts[1:]

    parts = [p for p in parts if p]
    if not parts:
        return None

    code = paren_code
    if parts and CODE_RE.match(parts[-1]):
        if code is None:
            code = parts[-1]
        parts = parts[:-1]

    if not parts:
        return None

    if brand is None:
        if len(parts) >= 2:
            brand = parts[0]
            product_name = " / ".join(parts[1:])
        else:
            brand = "미기재"
            product_name = parts[0]
    else:
        product_name = " / ".join(parts)

    if brand != "미기재":
        if bool(re.search(r'[가-힣]', brand)) and not bool(re.search(r'[A-Za-z]', brand)):
            return None

    return (brand or "미기재", product_name or "미기재", code or "미기재")


def extract_with_ai(full_title):
    if not openai_client:
        return "미기재", full_title, "미기재"

    try:
        response = openai_client.chat.completions.create(
            model="gpt-4o-mini",
            messages=[
                {"role": "system", "content": SYSTEM_PROMPT},
                {"role": "user", "content": f"다음 상품 제목에서 브랜드명, 상품명, 상품코드를 추출해주세요:\n{full_title}"}
            ],
            temperature=0,
            max_tokens=200
        )
        result = json.loads(response.choices[0].message.content.strip())
        return (
            result.get("brand", "미기재") or "미기재",
            result.get("product_name", "미기재") or "미기재",
            result.get("product_code", "미기재") or "미기재"
        )
    except Exception as e:
        logger.error(f"  AI 추출 실패 ({e}), 원본 제목 사용")
        return "미기재", full_title, "미기재"


def extract_product_info(full_title):
    result = parse_title_by_logic(full_title)
    if result is not None:
        return result

    if not openai_client:
        return "미기재", full_title, "미기재"

    logger.info(f"  → AI 처리: {full_title}")
    return extract_with_ai(full_title)


def parse_discount_rate(tiara_custom):
    match = DISCOUNT_RATE_RE.search(tiara_custom or "")
    return f"{match.group(1)}%" if match else ""


async def crawl_kakao_store():
    results = []
    now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    logger.info("카카오 세이브프라자 크롤링 시작...")

    async with Stealth().use_async(async_playwright()) as p:
        browser = await p.chromium.launch(headless=True)
        context = await browser.new_context(
            user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
            viewport={'width': 1920, 'height': 1080}
        )
        page = await context.new_page()

        logger.info(f"URL 접속 중: {CATEGORY_URL}")
        await page.goto(CATEGORY_URL, wait_until="domcontentloaded", timeout=120000)

        try:
            await page.wait_for_selector("li.ng-star-inserted .item_product", timeout=15000)
        except Exception:
            logger.warning("상품 목록 셀렉터를 찾을 수 없습니다. (로드 지연 혹은 차단)")
            await page.screenshot(path="kakao_list_debug.png")

        logger.info("모든 상품을 로드하기 위해 스크롤 중...")
        last_height = await page.evaluate("document.body.scrollHeight")
        while True:
            await page.evaluate("window.scrollTo(0, document.body.scrollHeight)")
            await asyncio.sleep(2)
            new_height = await page.evaluate("document.body.scrollHeight")
            if new_height == last_height:
                break
            last_height = new_height

        product_elements = await page.query_selector_all("li.ng-star-inserted .item_product")
        logger.info(f"총 {len(product_elements)}개의 상품을 발견했습니다.")

        product_links = []
        for el in product_elements:
            try:
                title_el = await el.query_selector("span.name_product")
                full_title = (await title_el.inner_text()).strip() if title_el else ""

                link_el = await el.query_selector("a.link_product")
                href = await link_el.get_attribute("href") if link_el else ""
                detail_link = BASE_URL + href if href and href.startswith("/") else href

                tiara_el = await el.query_selector("[data-tiara-custom]")
                tiara_custom = await tiara_el.get_attribute("data-tiara-custom") if tiara_el else ""

                if detail_link:
                    product_links.append({
                        "full_title": full_title,
                        "detail_link": detail_link,
                        "discount_rate": parse_discount_rate(tiara_custom)
                    })
            except Exception as e:
                logger.error(f"리스트 항목 추출 중 에러: {e}")

        for count, item in enumerate(product_links, 1):
            if count % 10 == 0:
                logger.info(f"[{count}/{len(product_links)}] 처리 중...")

            brand, product_name, product_code = extract_product_info(item['full_title'])

            try:
                await page.goto(item['detail_link'], wait_until="domcontentloaded", timeout=60000)

                original_price_el = await page.query_selector("div.info_regular span.txt_price")
                original_price = (await original_price_el.inner_text()).replace("정가:", "").strip() if original_price_el else "0"

                discount_price_el = await page.query_selector("div.info_price span.txt_price")
                discount_price = await discount_price_el.inner_text() if discount_price_el else "0"

                discount_rate = item['discount_rate']
                discount_rate_el = await page.query_selector("div.info_price span.txt_sale")
                if discount_rate_el:
                    detail_rate_text = await discount_rate_el.inner_text()
                    if "%" in detail_rate_text:
                        discount_rate = detail_rate_text.split(":")[-1].strip()

                results.append({
                    "스토어": "카카오 세이브프라자",
                    "브랜드명": brand,
                    "할인율": discount_rate,
                    "상품명": product_name,
                    "상품코드": product_code,
                    "할인가": discount_price,
                    "원가": original_price,
                    "상품상세페이지링크": item['detail_link'],
                    "수집일시": now
                })

            except Exception as e:
                logger.error(f"상세 페이지({item['detail_link']}) 추출 에러: {e}")
                results.append({
                    "스토어": "카카오 세이브프라자",
                    "브랜드명": brand,
                    "할인율": item['discount_rate'],
                    "상품명": product_name,
                    "상품코드": product_code,
                    "할인가": "Error",
                    "원가": "Error",
                    "상품상세페이지링크": item['detail_link'],
                    "수집일시": now
                })

        await browser.close()

    if results:
        save_to_google_sheets(results, "카카오 세이브프라자")
    else:
        logger.warning("수집된 데이터가 없습니다.")


if __name__ == "__main__":
    asyncio.run(crawl_kakao_store())

