"""
Andy Jo Stock AI — 메인 파이프라인
시간여행TV 소형주 선정 기준 완전 구현
목표 실행 시간: 10분 이내
"""

import logging
import traceback
import json
import os
from datetime import datetime

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[logging.StreamHandler()]
)
log = logging.getLogger(__name__)


def run_pipeline():
    today = datetime.now().strftime("%Y-%m-%d")
    log.info(f"{'='*60}")
    log.info(f"Andy Jo Stock AI 파이프라인 시작: {today}")
    log.info(f"{'='*60}")

    # ── STEP 1: 뉴스 수집 ──────────────────────────────────
    log.info("[STEP 1] 뉴스 수집 시작")
    try:
        from news_engine import fetch_all_news
        news_data = fetch_all_news()
        log.info(f"  → 뉴스 수집 완료: {len(news_data)}건")
    except Exception:
        log.error(f"  → 뉴스 수집 실패\n{traceback.format_exc()}")
        news_data = []

    # ── STEP 2: 시장 배치 데이터 조회 ─────────────────────
    log.info("[STEP 2] 시장 배치 데이터 조회 시작")
    try:
        from market_engine import load_market_universe
        universe_df = load_market_universe()
        log.info(f"  → 유니버스 로드 완료: {len(universe_df)}종목")
    except Exception:
        log.error(f"  → 시장 데이터 조회 실패\n{traceback.format_exc()}")
        universe_df = None

    if universe_df is None or universe_df.empty:
        log.error("유니버스 없음 — 파이프라인 중단")
        return

    # ── STEP 3: DART 공시 배치 조회 ───────────────────────
    log.info("[STEP 3] DART 공시 배치 조회 시작")
    try:
        from dart_engine import fetch_disclosure_batch
        disclosure_map = fetch_disclosure_batch(days=14)
        log.info(f"  → 공시 배치 완료: {len(disclosure_map)}개 기업")
    except Exception:
        log.error(f"  → DART 공시 조회 실패\n{traceback.format_exc()}")
        disclosure_map = {}

    # ── STEP 4: L0-L1 즉시탈락 필터 ──────────────────────
    log.info("[STEP 4] L0-L1 즉시탈락 필터 시작")
    try:
        from filter_engine import apply_l0_l1_filter
        candidates_df = apply_l0_l1_filter(universe_df, disclosure_map)
        log.info(f"  → L0-L1 통과: {len(candidates_df)}종목 "
                 f"(탈락: {len(universe_df)-len(candidates_df)}종목)")
    except Exception:
        log.error(f"  → L0-L1 필터 실패\n{traceback.format_exc()}")
        candidates_df = universe_df

    # ── STEP 5: DART 재무 데이터 조회 (통과 종목만) ───────
    log.info(f"[STEP 5] DART 재무 조회 시작 ({len(candidates_df)}종목)")
    try:
        from dart_engine import fetch_financial_batch
        from market_engine import load_corp_code_map
        corp_map = load_corp_code_map()
        financial_map = fetch_financial_batch(
            candidates_df["Code"].tolist(), corp_map
        )
        log.info(f"  → 재무 조회 완료: {len(financial_map)}종목")
    except Exception:
        log.error(f"  → 재무 조회 실패\n{traceback.format_exc()}")
        financial_map = {}

    # ── STEP 6: L2-L6 전체 필터링 ─────────────────────────
    log.info("[STEP 6] L2-L6 필터링 및 트랙 분류 시작")
    try:
        from filter_engine import apply_l2_l6_filter
        results = apply_l2_l6_filter(
            candidates_df, financial_map, disclosure_map, news_data
        )
        ready    = results.get("READY", [])
        buy_now  = results.get("BUY_NOW", [])
        core     = results.get("CORE", [])
        launched = results.get("LAUNCHED", [])
        log.info(f"  → CORE {len(core)} / BUY_NOW {len(buy_now)} / "
                 f"READY {len(ready)} / LAUNCHED {len(launched)}")
    except Exception:
        log.error(f"  → 필터링 실패\n{traceback.format_exc()}")
        results = {"READY": [], "BUY_NOW": [], "CORE": [], "LAUNCHED": []}

    # ── STEP 7: 테마 온도 계산 ────────────────────────────
    log.info("[STEP 7] 테마 온도 계산 시작")
    try:
        from theme_engine import calculate_theme_scores
        theme_scores = calculate_theme_scores(news_data, universe_df)
        top3 = sorted(theme_scores.items(),
                      key=lambda x: -x[1]["score"])[:3]
        log.info(f"  → 테마 TOP3: {[(t, d['score']) for t, d in top3]}")
    except Exception:
        log.error(f"  → 테마 계산 실패\n{traceback.format_exc()}")
        theme_scores = {}

    # ── STEP 8: 구글 시트 저장 ────────────────────────────
    log.info("[STEP 8] 구글 시트 저장 시작")
    try:
        from data_store import save_all
        save_all(results, theme_scores, news_data, today)
        log.info("  → 저장 완료")
    except Exception:
        log.error(f"  → 저장 실패\n{traceback.format_exc()}")

    # ── STEP 9: 알림 발송 ─────────────────────────────────
    log.info("[STEP 9] 알림 발송")
    try:
        from alert import send_daily_summary
        send_daily_summary(results, theme_scores, today)
        log.info("  → 알림 발송 완료")
    except Exception:
        log.warning(f"  → 알림 발송 실패 (치명적 아님)\n{traceback.format_exc()}")

    # ── 완료 리포트 ───────────────────────────────────────
    log.info(f"{'='*60}")
    log.info(f"파이프라인 완료: {today}")
    log.info(f"  CORE     : {len(results.get('CORE', []))}종목")
    log.info(f"  BUY NOW  : {len(results.get('BUY_NOW', []))}종목")
    log.info(f"  READY    : {len(results.get('READY', []))}종목")
    log.info(f"  LAUNCHED : {len(results.get('LAUNCHED', []))}종목")
    log.info(f"  뉴스     : {len(news_data)}건")
    log.info(f"  테마 TOP1: {top3[0][0] if top3 else '없음'}")
    log.info(f"{'='*60}")


if __name__ == "__main__":
    run_pipeline()
