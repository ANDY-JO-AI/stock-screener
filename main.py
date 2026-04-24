"""
Andy Jo Stock AI — 메인 파이프라인 (최적화버전)
DART 분석을 시가총액 필터 통과 종목에만 적용
"""

import logging
import traceback
from datetime import datetime

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[logging.StreamHandler()]
)
log = logging.getLogger(__name__)


def run_pipeline():
    today = datetime.now().strftime("%Y-%m-%d")
    log.info(f"====== Andy Jo Stock AI 파이프라인 시작: {today} ======")

    # STEP 1: 뉴스 + 커뮤니티 수집
    log.info("[STEP 1] 뉴스 · 커뮤니티 수집 시작")
    try:
        from news_engine import fetch_all_news
        news_data = fetch_all_news()
        log.info(f"  → 수집 완료: {len(news_data)}건")
    except Exception:
        log.error(f"  → 뉴스 수집 실패\n{traceback.format_exc()}")
        news_data = []

    # STEP 2: 테마 온도 계산 (뉴스 기반 먼저)
    log.info("[STEP 2] 테마 온도 계산 시작")
    try:
        from theme_engine import calculate_theme_scores
        theme_scores = calculate_theme_scores(news_data)
        log.info(f"  → 테마 계산 완료: {len(theme_scores)}개 테마")
    except Exception:
        log.error(f"  → 테마 계산 실패\n{traceback.format_exc()}")
        theme_scores = {}

    # STEP 3: 시가총액 필터로 후보 종목 추출 (DART 분석 전 축소)
    log.info("[STEP 3] 시가총액 필터 — 후보 종목 추출")
    try:
        from filter_engine import load_kosdaq_universe
        universe = load_kosdaq_universe()
        candidate_codes = universe["code"].tolist()
        log.info(f"  → 후보 종목: {len(candidate_codes)}개 (시총 150-700억)")
    except Exception:
        log.error(f"  → 유니버스 로드 실패\n{traceback.format_exc()}")
        candidate_codes = []

    # STEP 4: DART 공시 분석 (후보 종목만)
    log.info(f"[STEP 4] DART 공시 분석 시작 ({len(candidate_codes)}종목)")
    try:
        from dart_engine import fetch_dart_signals
        dart_signals = fetch_dart_signals(stock_codes=candidate_codes)
        log.info(f"  → 공시 분석 완료: {len(dart_signals)}건")
    except Exception:
        log.error(f"  → DART 분석 실패\n{traceback.format_exc()}")
        dart_signals = {}

    # STEP 5: 종목 필터링 (L0-L6) + 3트랙 분류
    log.info("[STEP 5] 종목 필터링 · 3트랙 분류 시작")
    try:
        from filter_engine import run_pipeline as run_filter
        results = run_filter(news_data, dart_signals, theme_scores)
        ready    = results.get("READY", [])
        buy_now  = results.get("BUY_NOW", [])
        launched = results.get("LAUNCHED", [])
        log.info(f"  → READY {len(ready)}종목 / BUY_NOW {len(buy_now)}종목 / LAUNCHED {len(launched)}종목")
    except Exception:
        log.error(f"  → 필터링 실패\n{traceback.format_exc()}")
        results = {"READY": [], "BUY_NOW": [], "LAUNCHED": []}

    # STEP 6: Google Sheets 저장 (누적)
    log.info("[STEP 6] Google Sheets 저장 시작")
    try:
        from data_store import save_daily_snapshot, save_stock_history, save_theme_daily
        save_daily_snapshot(results, today)
        save_stock_history(results, today)
        save_theme_daily(theme_scores, today)
        log.info("  → 저장 완료")
    except Exception:
        log.error(f"  → 저장 실패\n{traceback.format_exc()}")

    # STEP 7: 완료 보고
    log.info("====== 파이프라인 완료 ======")
    top3 = sorted(theme_scores.items(), key=lambda x: -x[1].get("score", 0))[:3]
    log.info(f"  오늘의 테마 TOP3: {[(t, round(d['score'],1)) for t,d in top3]}")
    log.info(f"  BUY NOW 종목: {[s['name'] for s in buy_now[:5]]}")
    log.info(f"  READY 종목: {len(ready)}개 대기 중")


if __name__ == "__main__":
    run_pipeline()
