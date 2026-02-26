
import asyncio
import re
import json
import pandas as pd
from playwright.async_api import async_playwright
from datetime import datetime
import os
import gspread
from oauth2client.service_account import ServiceAccountCredentials
from openai import OpenAI

# 구글 스프레드시트 설정
SPREADSHEET_URL = "https://docs.google.com/spreadsheets/d/1hJoa1sjXbFkJlKeSEwQDW2YKOLDXBaaxfP_FyiyhYEQ/edit?gid=0#gid=0"

OPENAI_API_KEY = os.environ.get("OPENAI_API_KEY", "")
openai_client = OpenAI(api_key=OPENAI_API_KEY)

CODE_RE = re.compile(r'^(?=.*[A-Za-z])(?=.*[0-9])[A-Za-z0-9]{13,14}$')
# 괄호 안 코드 (13~14자리)
CODE_IN_PAREN = re.compile(r'\(([A-Za-z0-9]{13,14})\)')
# 앞쪽 태그 제거: [태그], (태그)
PREFIX_TAG = re.compile(r'^(\[[^\]]+\]|\([^)]+\))\s*')
# 기획전/카탈로그형 제목 감지: 슬래시 없이 대괄호만 있거나 % 포함
PROMO_RE = re.compile(r'%|BEST|특가|기획전|카탈로그', re.IGNORECASE)


def parse_title_by_logic(full_title):
    """
    로직으로 브랜드/상품명/코드 추출.
    확실하게 파싱되면 (brand, product_name, product_code) 반환.
    불확실하면 None 반환 → AI로 넘김.
    """
    title = full_title.strip()

    # 0) 기획전/카탈로그형 제목 감지 → 브랜드/코드 미기재, 원본 제목을 상품명으로
    if PROMO_RE.search(title):
        return ("미기재", title, "미기재")

    # 1) "/정가xxx원" 같은 후미 부가정보 제거
    title = re.sub(r'/?\s*정가\s*[\d,]+원.*$', '', title).strip()
    # "단독특가", "(단독특가)" 등 후미 제거
    title = re.sub(r'\s*(단독특가|특가|할인)\s*$', '', title).strip()

    # 2) 괄호 안 코드 먼저 추출 (있으면)
    paren_code = None
    paren_match = CODE_IN_PAREN.search(title)
    if paren_match:
        paren_code = paren_match.group(1)
        title = title[:paren_match.start()].strip() + title[paren_match.end():].strip()
        title = title.strip().rstrip('/').strip()

    # 3) 슬래시로 분리
    parts = [p.strip() for p in title.split('/') if p.strip()]

    # 4) 앞쪽 파트에서 PREFIX_TAG 제거 후 브랜드 판별
    #    첫 파트가 [브랜드] 형태면 브랜드 추출
    # 한글만으로 이루어진 태그 키워드 (브랜드가 아닌 것들)
    NOISE_TAGS = {'단독특가', '특가', '할인', '쿠폰', '타임세일', '한정', '추가할인', '신상', '세일'}

    brand = None
    if parts:
        first = parts[0]
        bracket_match = re.match(r'^\[([^\]]+)\]\s*(.*)', first)
        if bracket_match:
            tag_content = bracket_match.group(1).strip()
            remainder = bracket_match.group(2).strip()
            # 태그 내용이 노이즈 키워드이거나 한글만이면 → 태그 제거, 브랜드 아님
            is_noise = tag_content in NOISE_TAGS or (
                bool(re.search(r'[가-힣]', tag_content)) and not bool(re.search(r'[A-Za-z]', tag_content))
            )
            if is_noise:
                # 태그 제거 후 remainder가 있으면 해당 파트 교체
                if remainder:
                    parts[0] = remainder
                else:
                    parts.pop(0)
            else:
                # 태그 내용이 영문 브랜드 → 브랜드로 사용
                brand = tag_content
                if remainder:
                    parts[0] = remainder
                else:
                    parts.pop(0)
        else:
            # (태그) 브랜드 형태 처리
            tag_match = PREFIX_TAG.match(first)
            if tag_match:
                after_tag = first[tag_match.end():].strip()
                if after_tag:
                    parts[0] = after_tag
                else:
                    parts.pop(0)

    # 5) 슬래시 파트에서 코드/브랜드/상품명 분리
    code = paren_code  # 괄호 코드 우선

    # 마지막 파트가 코드인지 확인
    if parts and CODE_RE.match(parts[-1]):
        if code is None:
            code = parts[-1]
        parts.pop()

    # 남은 파트 처리
    if not parts:
        # 상품명 없음 → AI로
        return None

    if brand is None:
        if len(parts) >= 2:
            brand = parts[0]
            product_name = " / ".join(parts[1:])
        else:
            # 파트 1개 → 브랜드 없이 상품명만
            brand = "미기재"
            product_name = parts[0]
    else:
        product_name = " / ".join(parts)

    # 브랜드가 한글만 포함하고 짧으면 브랜드가 아닐 수 있음 → AI로
    if brand != "미기재":
        # 브랜드에 한글이 포함되어 있고 영문이 전혀 없으면 불확실
        has_korean = bool(re.search(r'[가-힣]', brand))
        has_english = bool(re.search(r'[A-Za-z]', brand))
        if has_korean and not has_english:
            return None  # AI로 넘김

    return (
        brand or "미기재",
        product_name or "미기재",
        code or "미기재"
    )


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


def extract_with_ai(full_title):
    """로직으로 처리 불가한 케이스만 OpenAI API로 추출"""
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
        content = response.choices[0].message.content.strip()
        result = json.loads(content)
        return (
            result.get("brand", "미기재") or "미기재",
            result.get("product_name", "미기재") or "미기재",
            result.get("product_code", "미기재") or "미기재"
        )
    except Exception as e:
        print(f"  AI 추출 실패 ({e}), 원본 제목 사용")
        return "미기재", full_title, "미기재"


def extract_product_info(full_title):
    """로직 우선 처리, 불확실한 경우만 AI 호출"""
    result = parse_title_by_logic(full_title)
    if result is not None:
        return result
    print(f"  → AI 처리: {full_title}")
    return extract_with_ai(full_title)


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
            new_values.append(list(map(str, item.values())))
            existing_keys.add((store, code))  # 같은 실행 내 중복도 방지

        print(f"중복 제외: {skipped}개 / 신규 추가 대상: {len(new_values)}개")

        if new_values:
            sheet.append_rows(new_values)
            print(f"구글 스프레드시트에 {len(new_values)}개의 카카오 세이브프라자 신규 상품을 기록했습니다.")
        else:
            print("신규 상품이 없어 시트에 기록하지 않습니다.")
    except Exception as e:
        print(f"구글 스프레드시트 기록 중 에러 발생: {e}")
        print("URL이 정확한지, 서비스 계정 이메일이 시트에 공유되었는지 확인해주세요.")

async def crawl_kakao_store():
    base_url = "https://store.kakao.com"
    category_url = "https://store.kakao.com/kolonsaveplaza/category?sort=POPULAR_SALE_COUNT&showMenu=false"

    results = []

    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True)
        context = await browser.new_context(
            user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
        )
        page = await context.new_page()

        print(f"URL 접속 중: {category_url}")
        await page.goto(category_url, wait_until="networkidle")

        print("모든 상품을 로드하기 위해 스크롤 중...")
        last_height = await page.evaluate("document.body.scrollHeight")
        while True:
            await page.evaluate("window.scrollTo(0, document.body.scrollHeight)")
            await asyncio.sleep(2)
            new_height = await page.evaluate("document.body.scrollHeight")
            if new_height == last_height:
                break
            last_height = new_height

        product_elements = await page.query_selector_all("li.ng-star-inserted .item_product")
        print(f"총 {len(product_elements)}개의 상품을 발견했습니다.")

        product_links = []
        for el in product_elements:
            try:
                title_el = await el.query_selector("span.name_product")
                full_title = await title_el.inner_text() if title_el else ""

                link_el = await el.query_selector("a.link_product")
                href = await link_el.get_attribute("href") if link_el else ""
                detail_link = base_url + href if href and href.startswith("/") else href

                if detail_link:
                    product_links.append({
                        "full_title": full_title.strip(),
                        "detail_link": detail_link
                    })
            except Exception as e:
                print(f"리스트 항목 추출 중 에러: {e}")

        count = 0
        for item in product_links:
            count += 1
            print(f"[{count}/{len(product_links)}] 처리 중: {item['full_title']}")

            # 로직 우선 처리, 불확실한 경우만 AI 호출
            brand, product_name, product_code = extract_product_info(item['full_title'])

            try:
                await page.goto(item['detail_link'], wait_until="networkidle")

                original_price_el = await page.query_selector("div.info_regular span.txt_price")
                original_price = await original_price_el.inner_text() if original_price_el else ""
                original_price = original_price.replace("정가:", "").strip()
                if not original_price:
                    original_price = "미기재"

                discount_price_el = await page.query_selector("div.info_price span.txt_price")
                discount_price = await discount_price_el.inner_text() if discount_price_el else ""

                discount_rate_el = await page.query_selector("div.info_price span.txt_sale")
                discount_rate = ""
                if discount_rate_el:
                    discount_rate_text = await discount_rate_el.inner_text()
                    if "%" in discount_rate_text:
                        discount_rate = discount_rate_text.split(":")[-1].strip()
                    else:
                        discount_rate = discount_rate_text.strip()

                results.append({
                    "스토어": "카카오 세이브프라자",
                    "브랜드명": brand,
                    "할인율": discount_rate,
                    "상품명": product_name,
                    "상품코드": product_code,
                    "할인가": discount_price,
                    "원가": original_price,
                    "상품상세페이지링크": item['detail_link'],
                    "수집일시": datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                })

            except Exception as e:
                print(f"상세 페이지({item['detail_link']}) 추출 에러: {e}")
                results.append({
                    "스토어": "카카오 세이브프라자",
                    "브랜드명": brand,
                    "할인율": "N/A",
                    "상품명": product_name,
                    "상품코드": product_code,
                    "할인가": "Error",
                    "원가": "Error",
                    "상품상세페이지링크": item['detail_link'],
                    "수집일시": datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                })

        await browser.close()

    if results:
        save_to_google_sheets(results)
    else:
        print("수집된 데이터가 없습니다.")

if __name__ == "__main__":
    asyncio.run(crawl_kakao_store())
