"""
Andy Jo Stock AI — 종목 필터링 엔진 (3트랙 완전 재설계)
L0 우주 → L1 즉시탈락 → L2 재무 → L3 유동성 → L4 DART → L5 테마 → L6 타이밍
결과: READY / BUY_NOW / LAUNCHED 3트랙 분류
"""

import os
import time
import logging
import pandas as pd
from datetime import datetime, timedelta

log = logging.getLogger(__name__)

# ────────────────────────────────────────
# 설정값
# ────────────────────────────────────────
MKTCAP_MIN   = 150   # 시가총액 최소 (억원)
MKTCAP_MAX   = 700   # 시가총액 최대 (억원)
MIN_SCORE    = 5     # 최종 통과 최소 점수
TOP_N        = 50    # 최종 출력 종목 수

# 즉시 탈락 키워드 (공시 제목 기준)
REJECT_KEYWORDS = [
    "전환사채", "신주인수권부사채", "유상증자", "제3자배정",
    "횡령", "배임", "불성실공시", "상장폐지",
    "자본잠식", "워크아웃", "회생절차", "최대주주변경"
]

# 테마 관련 키워드 (L5 채점용)
THEME_KEYWORDS = {
    "방산":     ["방산", "방위", "무기", "미사일", "군수", "K방산"],
    "원전":     ["원전", "원자력", "SMR", "핵융합"],
    "로봇_AI":  ["로봇", "협동로봇", "AI로봇", "휴머노이드"],
    "AI반도체": ["AI반도체", "HBM", "NPU", "파운드리"],
    "2차전지":  ["배터리", "2차전지", "양극재", "음극재", "전고체"],
    "바이오":   ["바이오", "신약", "ADC", "mRNA", "임상"],
    "조선":     ["조선", "LNG선", "컨테이너선", "수주잔고"],
    "건설":     ["재건축", "재개발", "PF", "리츠"],
    "우주항공": ["우주", "위성", "발사체", "누리호"],
    "수소":     ["수소", "연료전지", "그린수소"],
    "가상화폐": ["비트코인", "코인", "블록체인", "가상자산"],
    "게임":     ["게임", "메타버스", "P2E"],
    "엔터":     ["K팝", "아이돌", "엔터", "한류"],
    "반도체장비":["반도체장비", "식각", "증착", "CMP"],
    "자동차":   ["전기차", "자율주행", "전장", "ADAS"],
}


# ────────────────────────────────────────
# L0: KOSDAQ 유니버스 로드
# ────────────────────────────────────────
def load_kosdaq_universe() -> pd.DataFrame:
    """
    KOSDAQ 전종목 로드 → 시가총액 150-700억 필터
    반환: DataFrame [code, name, mktcap]
    """
    try:
        import FinanceDataReader as fdr
        df = fdr.StockListing("KOSDAQ")
        df = df.rename(columns={
            "Code": "code", "Name": "name",
            "Marcap": "mktcap", "Market": "market"
        })
        df["mktcap"] = pd.to_numeric(df["mktcap"], errors="coerce").fillna(0)
        df["mktcap_억"] = df["mktcap"] / 1e8

        filtered = df[
            (df["mktcap_억"] >= MKTCAP_MIN) &
            (df["mktcap_억"] <= MKTCAP_MAX)
        ].copy()

        log.info(f"L0 유니버스: KOSDAQ {len(df)}종목 → 시가총액 필터 후 {len(filtered)}종목")
        return filtered[["code", "name", "mktcap_억"]].reset_index(drop=True)

    except Exception as e:
        log.error(f"L0 유니버스 로드 실패: {e}")
        return pd.DataFrame(columns=["code", "name", "mktcap_억"])


# ────────────────────────────────────────
# L1: 즉시 탈락 필터
# ────────────────────────────────────────
def check_l1_reject(code: str, name: str, news_titles: list) -> tuple:
    """
    즉시 탈락 조건 검사
    반환: (통과여부, 탈락사유)
    """
    # 뉴스 제목 기반 위험 키워드 검사
    for title in news_titles:
        if name in title:
            for kw in REJECT_KEYWORDS:
                if kw in title:
                    return False, f"위험뉴스: {kw}"

    # 종목명 자체 위험 패턴
    danger_patterns = ["스팩", "SPAC", "기업인수"]
    for p in danger_patterns:
        if p in name:
            return False, f"종목명 패턴: {p}"

    return True, ""


# ────────────────────────────────────────
# L2: 재무 점수 (최대 15점)
# ────────────────────────────────────────
def calc_l2_financial_score(code: str) -> tuple:
    """
    재무 점수 계산
    반환: (점수, 상세딕셔너리)
    """
    try:
        import FinanceDataReader as fdr

        today     = datetime.now().strftime("%Y%m%d")
        past_year = (datetime.now() - timedelta(days=365)).strftime("%Y%m%d")
        df = fdr.DataReader(code, past_year, today)

        if df is None or len(df) < 20:
            return 0, {"error": "데이터 부족"}

        score = 0
        detail = {}

        # 1년 수익률 (최대 5점)
        year_return = (df["Close"].iloc[-1] - df["Close"].iloc[0]) / df["Close"].iloc[0] * 100
        detail["year_return"] = round(year_return, 1)
        if year_return > 50:   score += 5
        elif year_return > 20: score += 4
        elif year_return > 0:  score += 3
        elif year_return > -20:score += 1

        # 최근 20일 거래량 증가율 (최대 5점)
        recent_vol = df["Volume"].iloc[-20:].mean()
        old_vol    = df["Volume"].iloc[-60:-20].mean() if len(df) >= 60 else df["Volume"].mean()
        vol_ratio  = recent_vol / old_vol if old_vol > 0 else 1
        detail["vol_ratio"] = round(vol_ratio, 2)
        if vol_ratio > 3:   score += 5
        elif vol_ratio > 2: score += 4
        elif vol_ratio > 1.5: score += 3
        elif vol_ratio > 1: score += 2

        # 52주 저점 대비 위치 (최대 5점)
        low_52w  = df["Low"].min()
        high_52w = df["High"].max()
        current  = df["Close"].iloc[-1]
        if high_52w > low_52w:
            position = (current - low_52w) / (high_52w - low_52w)
        else:
            position = 0.5
        detail["position_52w"] = round(position, 2)
        # 저점 부근(0-30%)일수록 높은 점수 (매수 타이밍)
        if position < 0.15:   score += 5
        elif position < 0.30: score += 4
        elif position < 0.50: score += 3
        elif position < 0.70: score += 2
        else:                 score += 1

        return min(score, 15), detail

    except Exception as e:
        log.debug(f"L2 재무점수 실패 ({code}): {e}")
        return 0, {"error": str(e)}


# ────────────────────────────────────────
# L3: 유동성 검사
# ────────────────────────────────────────
def check_l3_liquidity(code: str) -> tuple:
    """
    최근 20일 평균 거래대금 10억 이상 확인
    반환: (통과여부, 평균거래대금_억)
    """
    try:
        import FinanceDataReader as fdr

        today = datetime.now().strftime("%Y%m%d")
        past  = (datetime.now() - timedelta(days=30)).strftime("%Y%m%d")
        df    = fdr.DataReader(code, past, today)

        if df is None or len(df) < 5:
            return False, 0

        df["turnover"] = df["Close"] * df["Volume"] / 1e8  # 억원
        avg_turnover   = df["turnover"].iloc[-20:].mean()

        return avg_turnover >= 10, round(avg_turnover, 1)

    except Exception as e:
        log.debug(f"L3 유동성 실패 ({code}): {e}")
        return False, 0


# ────────────────────────────────────────
# L4: DART 공시 분석
# ────────────────────────────────────────
def check_l4_dart(code: str, dart_signals: dict) -> tuple:
    """
    dart_engine 결과 활용
    반환: (통과여부, dart_score, 상세)
    """
    signal = dart_signals.get(code, {})
    if not signal:
        # DART 데이터 없으면 조건부 통과 (점수 0)
        return True, 0, {"note": "DART 데이터 없음"}

    dart_pass  = signal.get("dart_pass", True)
    dart_score = signal.get("dart_score", 0)
    reject_reason = signal.get("reject_reason", "")

    return dart_pass, dart_score, signal


# ────────────────────────────────────────
# L5: 테마 점수 (최대 10점)
# ────────────────────────────────────────
def calc_l5_theme_score(code: str, name: str, news_titles: list, theme_scores: dict) -> tuple:
    """
    종목명 기반 테마 매핑 + 테마 온도 점수 계산
    반환: (테마점수, 매핑된테마리스트)
    """
    try:
        from theme_engine import get_stock_themes
        matched_themes = get_stock_themes(code)
    except Exception:
        matched_themes = []

    # 뉴스에서 종목명 언급 횟수로 추가 테마 감지
    name_mentions = sum(1 for t in news_titles if name in t)

    score = 0
    # 테마 매핑된 경우 해당 테마 온도 점수 반영
    for theme in matched_themes:
        theme_data = theme_scores.get(theme, {})
        theme_temp = theme_data.get("score", 0)
        score += theme_temp * 0.8  # 테마 온도 80% 반영

    # 뉴스 언급 보너스 (최대 3점)
    mention_bonus = min(name_mentions * 0.5, 3)
    score += mention_bonus

    return min(round(score, 2), 10), matched_themes


# ────────────────────────────────────────
# L6: 매수 타이밍 체크
# ────────────────────────────────────────
def check_l6_timing(code: str) -> tuple:
    """
    매수 타이밍 조건:
    1. 현재가 ≤ 52주 저점 × 1.5
    2. 최근 5일 거래량 증가 (전일比 1.5배 이상)
    반환: (통과여부, 현재가, 52주저점, 거래량비율)
    """
    try:
        import FinanceDataReader as fdr

        today    = datetime.now().strftime("%Y%m%d")
        past_year= (datetime.now() - timedelta(days=365)).strftime("%Y%m%d")
        df       = fdr.DataReader(code, past_year, today)

        if df is None or len(df) < 10:
            return False, 0, 0, 0

        current   = df["Close"].iloc[-1]
        low_52w   = df["Low"].min()
        threshold = low_52w * 1.5

        # 거래량 조건
        recent_vol = df["Volume"].iloc[-1]
        avg_vol_20 = df["Volume"].iloc[-21:-1].mean()
        vol_ratio  = recent_vol / avg_vol_20 if avg_vol_20 > 0 else 0

        price_ok  = current <= threshold
        volume_ok = vol_ratio >= 1.5

        return (price_ok and volume_ok), current, low_52w, round(vol_ratio, 2)

    except Exception as e:
        log.debug(f"L6 타이밍 실패 ({code}): {e}")
        return False, 0, 0, 0


# ────────────────────────────────────────
# LAUNCHED 트랙 감지
# ────────────────────────────────────────
def check_launched(code: str) -> tuple:
    """
    이미 출발한 종목 감지:
    최근 20일 상승률 30% 이상 + 52주 저점 × 1.5 초과
    반환: (launched여부, 20일수익률)
    """
    try:
        import FinanceDataReader as fdr

        today = datetime.now().strftime("%Y%m%d")
        past  = (datetime.now() - timedelta(days=30)).strftime("%Y%m%d")
        df    = fdr.DataReader(code, past, today)

        if df is None or len(df) < 10:
            return False, 0

        current    = df["Close"].iloc[-1]
        past_price = df["Close"].iloc[0]
        gain_20d   = (current - past_price) / past_price * 100

        return gain_20d >= 30, round(gain_20d, 1)

    except Exception:
        return False, 0


# ────────────────────────────────────────
# 단일 종목 전체 분석
# ────────────────────────────────────────
def analyze_stock(row: pd.Series, news_titles: list, dart_signals: dict, theme_scores: dict) -> dict:
    """
    L0-L6 전체 파이프라인 적용 후 트랙 분류
    """
    code = str(row["code"]).zfill(6)
    name = row["name"]
    mktcap = row["mktcap_억"]

    result = {
        "code":   code,
        "name":   name,
        "mktcap": mktcap,
        "track":  None,
        "total_score": 0,
        "reject_reason": "",
        "themes": [],
        "detail": {}
    }

    # L1: 즉시 탈락
    l1_pass, l1_reason = check_l1_reject(code, name, news_titles)
    if not l1_pass:
        result["reject_reason"] = f"L1: {l1_reason}"
        return result

    # L2: 재무 점수
    l2_score, l2_detail = calc_l2_financial_score(code)
    result["detail"]["l2"] = l2_detail
    if l2_score < 3:
        result["reject_reason"] = f"L2: 재무점수 부족 ({l2_score}점)"
        return result

    # L3: 유동성
    l3_pass, l3_turnover = check_l3_liquidity(code)
    result["detail"]["l3_turnover"] = l3_turnover
    if not l3_pass:
        result["reject_reason"] = f"L3: 유동성 부족 ({l3_turnover}억)"
        return result

    # L4: DART
    l4_pass, l4_score, l4_detail = check_l4_dart(code, dart_signals)
    result["detail"]["l4"] = l4_detail
    if not l4_pass:
        result["reject_reason"] = f"L4: {l4_detail.get('reject_reason', 'DART 탈락')}"
        return result

    # L5: 테마 점수
    l5_score, themes = calc_l5_theme_score(code, name, news_titles, theme_scores)
    result["themes"] = themes
    result["detail"]["l5_theme_score"] = l5_score

    # 총점 계산
    total = l2_score + l4_score + l5_score
    result["total_score"] = round(total, 2)

    # ── 트랙 분류 ──

    # LAUNCHED 체크 (이미 많이 오른 종목)
    launched, gain_20d = check_launched(code)
    result["detail"]["gain_20d"] = gain_20d

    if launched:
        result["track"] = "LAUNCHED"
        return result

    # L6: 타이밍 체크
    l6_pass, current, low_52w, vol_ratio = check_l6_timing(code)
    result["detail"]["l6"] = {
        "current": current,
        "low_52w": low_52w,
        "vol_ratio": vol_ratio,
        "timing_ok": l6_pass
    }

    # BUY_NOW: L6 통과 + 테마점수 5점 이상
    if l6_pass and l5_score >= 5:
        result["track"] = "BUY_NOW"
    # READY: L1-L4 통과, 아직 타이밍 안됨
    elif total >= MIN_SCORE:
        result["track"] = "READY"
    else:
        result["reject_reason"] = f"총점 부족 ({total}점)"

    return result


# ────────────────────────────────────────
# 전체 파이프라인 실행 (메인 함수)
# ────────────────────────────────────────
def run_pipeline(news_data: list, dart_signals: dict, theme_scores: dict) -> dict:
    """
    전체 KOSDAQ 종목 대상 L0-L6 필터링 + 3트랙 분류
    반환: {"READY": [...], "BUY_NOW": [...], "LAUNCHED": [...]}
    """
    # 뉴스 제목 리스트 추출
    news_titles = [a.get("title", "") for a in news_data]

    # L0: 유니버스 로드
    universe = load_kosdaq_universe()
    if universe.empty:
        log.error("유니버스 로드 실패 — 파이프라인 중단")
        return {"READY": [], "BUY_NOW": [], "LAUNCHED": []}

    total = len(universe)
    log.info(f"필터링 시작: {total}종목 대상")

    ready    = []
    buy_now  = []
    launched = []
    rejected = 0

    for i, row in universe.iterrows():
        if i % 30 == 0:
            log.info(f"  진행: {i}/{total} | READY {len(ready)} / BUY_NOW {len(buy_now)} / LAUNCHED {len(launched)}")

        result = analyze_stock(row, news_titles, dart_signals, theme_scores)
        time.sleep(0.2)  # API 부하 방지

        track = result.get("track")
        if track == "BUY_NOW":
            buy_now.append(result)
        elif track == "READY":
            ready.append(result)
        elif track == "LAUNCHED":
            launched.append(result)
        else:
            rejected += 1

    # 점수 기준 정렬
    buy_now.sort(key=lambda x: -x["total_score"])
    ready.sort(key=lambda x: -x["total_score"])
    launched.sort(key=lambda x: -x["detail"].get("gain_20d", 0))

    # TOP N 제한
    buy_now  = buy_now[:TOP_N]
    ready    = ready[:TOP_N]
    launched = launched[:TOP_N]

    log.info(f"필터링 완료 — READY {len(ready)} / BUY_NOW {len(buy_now)} / LAUNCHED {len(launched)} / 탈락 {rejected}")

    return {
        "READY":    ready,
        "BUY_NOW":  buy_now,
        "LAUNCHED": launched
    }
