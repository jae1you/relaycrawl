"""
매일 오전 9시에 네이버(MZ아울렛)와 하프클럽 크롤러를 순차 실행하는 스케줄러.
실행 방법: python scheduler.py
종료 방법: Ctrl+C
"""

import schedule
import time
import asyncio
import logging
from datetime import datetime

# 각 크롤러의 메인 함수 임포트
from naver_crawler_gsheet import crawl_naver_store
from halfclub_crawler_gsheet import crawl_halfclub

# 로그 설정 (콘솔 + 파일 동시 출력)
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.FileHandler("scheduler.log", encoding="utf-8"),
        logging.StreamHandler()
    ]
)
log = logging.getLogger(__name__)


def run_all_crawlers():
    log.info("=" * 60)
    log.info(f"스케줄 실행 시작: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    log.info("=" * 60)

    # 1. 네이버 MZ아울렛 크롤링
    log.info("[1/2] 네이버 MZ아울렛 크롤러 시작")
    try:
        asyncio.run(crawl_naver_store())
        log.info("[1/2] 네이버 MZ아울렛 크롤러 완료")
    except Exception as e:
        log.error(f"[1/2] 네이버 MZ아울렛 크롤러 에러: {e}")

    # 2. 하프클럽 크롤링
    log.info("[2/2] 하프클럽 크롤러 시작")
    try:
        asyncio.run(crawl_halfclub())
        log.info("[2/2] 하프클럽 크롤러 완료")
    except Exception as e:
        log.error(f"[2/2] 하프클럽 크롤러 에러: {e}")

    log.info(f"전체 스케줄 실행 완료: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    log.info("=" * 60)


# 매일 오전 9시에 실행
schedule.every().day.at("09:00").do(run_all_crawlers)

if __name__ == "__main__":
    log.info("스케줄러 시작. 매일 오전 09:00에 크롤러가 자동 실행됩니다.")
    log.info(f"다음 실행 예정: {schedule.next_run()}")

    while True:
        schedule.run_pending()
        time.sleep(30)  # 30초마다 스케줄 체크
