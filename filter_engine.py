"""
Andy Jo Stock AI — 종목 필터링 엔진 v3
핵심 개선: pykrx 배치 조회로 전면 재설계 (종목별 개별 호출 완전 제거)
"""

import logging
import pandas as pd
from datetime import datetime, timedelta

log = logging.getLogger(__name__)

MKTCAP_MIN = 150
MKTCAP_MAX = 700
MIN_TOTAL_SCORE = 2
TOP_N = 50

REJECT_KEYWORDS = ["횡령", "배임", "불성실공시", "상장폐지", "관리종목", "워크아웃", "회생절차"]
REJECT_NAME_PATTERNS = ["스팩", "SPAC", "리츠", "ETF", "ETN"]

THEME_KEYWORDS = {
    "방산": ["방산", "방위", "무기", "미사일"],
    "원전": ["원전", "원자력", "SMR"],
    "로봇_AI": ["로봇", "AI로봇", "휴머노이드"],
    "AI반도체": ["AI반도체", "HBM", "NPU", "파운드리"],
    "2차전지": ["배터리", "2차전지", "양극재", "음극재"],
    "바이오": ["바이오", "신약", "ADC", "임상"],
    "조선": ["조선", "LNG선", "수주"],
    "우주항공": ["우주", "위성", "발사체"],
    "수소": ["수소", "연료전지"],
    "가상화폐": ["비트코인", "코인", "블록체인"],
    "게임": ["게임", "메타버스"],
    "엔터": ["K팝", "아이돌", "엔터"],
    "반도체장비": ["반도체장비", "식각", "증착"],
    "자동차": ["전기차", "자율주행", "전장"],
    "철강": ["철강", "제철"],
}

# ────────────────────────────────────────
# 1. pykrx 배치 데이터 로드 (핵심 개선)
# ────────────────────────────────────────
def load_market_data_batch():
    """
    pykrx로 KOSDAQ 전종목 데이터를 단 1회 배치 조회
    반환: {종목코드: {price, volume, change, marcap, ...}}
    """
    try:
        from pykrx import stock as krx
        today = datetime.now().strftime("%Y%m%d")
        # 영업일 기준 전날 (주말/공휴일 대비 3일 전)
        prev = (datetime.now() - timedelta(days=3)).strftime("%Y%m%d")

        log.info("pykrx 배치 데이터 조회 시작 (전종목 1회)")

        # 오늘 전종목 시세 (1회 호출)
        df_today = krx.get_market_ohlcv_by_ticker(today, market="KOSDAQ")
        # 시가총액 (1회 호출)
        df_cap = krx.get_market_cap_by_ticker(today, market="KOSDAQ")

        if df_today.empty or df_cap.empty:
            # 전일 데이터로 재시도
            yesterday = (datetime.now() - timedelta(days=1)).strftime("%Y%m%d")
            df_today = krx.get_market_ohlcv_by_ticker(yesterday, market="KOSDAQ")
            df_cap = krx.get_market_cap_by_ticker(yesterday, market="KOSDAQ")

        # 병합
        df = df_today.join(df_cap[["시가총액", "상장주식수"]], how="left")
        df["Marcap_억"] = df["시가총액"] / 1e8
        df["Code"] = df.index

        # 컬럼 정규화
        col_map = {
            "시가": "Open", "고가": "High", "저가": "Low",
            "종가": "Close", "거래량": "Volume", "등락률": "ChangeRatio"
        }
        df = df.rename(columns=col_map)

        # 종목명 추가
        try:
            name_df = krx.get_market_ticker_name_by_ticker(today, market="KOSDAQ")
            # pykrx API 버전에 따라 다름
        except Exception:
            pass

        log.info(f"pykrx 배치 조회 완료: {len(df)}종목")
        return df

    except Exception as e:
        log.error(f"pykrx 배치 조회 실패: {e}")
        return pd.DataFrame()

# ────────────────────────────────────────
# 2. FinanceDataReader 유니버스 로드
# ────────────────────────────────────────
def load_kosdaq_universe():
    try:
        import FinanceDataReader as fdr
        df = fdr.StockListing("KOSDAQ")
        df = df.dropna(subset=["Marcap"])
        df["Marcap_억"] = df["Marcap"] / 1e8
        df["Code"] = df["Code"].astype(str).str.zfill(6)
        filtered = df[
            (df["Marcap_억"] >= MKTCAP_MIN) &
            (df["Marcap_억"] <= MKTCAP_MAX)
        ].copy()
        log.info(f"L0 유니버스: KOSDAQ {len(df)}종목 → 시가총액 필터 후 {len(filtered)}종목")
        return filtered
    except Exception as e:
        log.error(f"유니버스 로드 실패: {e}")
        return pd.DataFrame()

# ────────────────────────────────────────
# 3. 뉴스 기반 종목 언급 맵 사전 생성 (핵심 개선)
#    → 종목별 반복 검색 대신 1회 전처리
# ────────────────────────────────────────
def build_news_mention_map(news_data, universe_df):
    """
    {종목코드: 언급횟수} 딕셔너리 1회 생성
    """
    mention_map = {}
    all_titles = " ".join([a.get("title", "") for a in news_data[:500]])

    for _, row in universe_df.iterrows():
        name = str(row.get("Name", ""))
        code = str(row.get("Code", ""))
        count = all_titles.count(name) if len(name) >= 3 else 0
        if count > 0:
            mention_map[code] = count

    log.info(f"뉴스 언급 종목: {len(mention_map)}개")
    return mention_map

# ────────────────────────────────────────
# 4. 테마 매칭 맵 사전 생성
# ────────────────────────────────────────
def build_theme_match_map(universe_df):
    """
    {종목코드: [매칭 테마, ...]} 딕셔너리 1회 생성
    """
    theme_map = {}
    for _, row in universe_df.iterrows():
        name = str(row.get("Name", ""))
        code = str(row.get("Code", ""))
        matched = []
        for theme, kws in THEME_KEYWORDS.items():
            for kw in kws:
                if kw in name:
                    matched.append(theme)
                    break
        if matched:
            theme_map[code] = matched
    return theme_map

# ────────────────────────────────────────
# 5. 단일 종목 분석 (메모리 처리 — API 호출 없음)
# ────────────────────────────────────────
def analyze_stock(row, dart_signals, theme_scores, mention_map, theme_map, news_data):
    code = str(row.get("Code", "")).zfill(6)
    name = str(row.get("Name", ""))

    result = {
        "code": code, "name": name,
        "mktcap": round(float(row.get("Marcap_억", 0) or 0), 1),
        "price": float(row.get("Close", 0) or 0),
        "change": float(row.get("ChangeRatio", 0) or 0),
        "volume": float(row.get("Volume", 0) or 0),
        "track": None, "total_score": 0,
        "l2_score": 0, "l4_score": 5, "l5_score": 0,
        "themes": [], "reject_reason": ""
    }

    # L1: 즉시 탈락
    for pat in REJECT_NAME_PATTERNS:
        if pat in name:
            result["reject_reason"] = f"패턴탈락:{pat}"
            return result

    # 뉴스 키워드 탈락 (사전 빌드된 mention_map 활용)
    mention_count = mention_map.get(code, 0)

    # L2: 재무점수 (배치 데이터 기반 — API 호출 없음)
    l2_score = 3  # 기본점수
    change = result["change"]
    volume = result["volume"]
    marcap = result["mktcap"]

    if change > 5:
        l2_score += 3
    elif change > 2:
        l2_score += 2
    elif change > 0:
        l2_score += 1
    elif change < -10:
        result["reject_reason"] = f"급락탈락({change:.1f}%)"
        return result

    if volume > 1000000:
        l2_score += 2
    elif volume > 300000:
        l2_score += 1

    result["l2_score"] = l2_score

    # L3: 유동성 (배치 데이터 기반)
    price = result["price"]
    if price > 0:
        turnover = volume * price / 1e8
        if turnover < 1:
            result["reject_reason"] = f"유동성부족({turnover:.1f}억)"
            return result

    # L4: DART
    if dart_signals and code in dart_signals:
        dart = dart_signals[code]
        if not dart.get("pass", True):
            result["reject_reason"] = dart.get("reason", "DART탈락")
            return result
        result["l4_score"] = dart.get("score", 5)

    # L5: 테마 점수
    themes = theme_map.get(code, [])
    l5_score = mention_count * 1.5  # 뉴스 언급 보너스

    for theme in themes:
        theme_data = theme_scores.get(theme, {})
        if isinstance(theme_data, dict):
            t_score = theme_data.get("score", 0)
        else:
            t_score = float(theme_data) if theme_data else 0
        l5_score += t_score * 0.8

    result["l5_score"] = round(min(l5_score, 10), 2)
    result["themes"] = themes

    # 총점
    total = (result["l2_score"] * 0.3) + (result["l4_score"] * 0.3) + (result["l5_score"] * 0.4)
    result["total_score"] = round(total, 2)

    if result["total_score"] < MIN_TOTAL_SCORE:
        result["reject_reason"] = f"점수미달({result['total_score']:.1f})"
        return result

    # 트랙 분류
    if change >= 20:
        result["track"] = "LAUNCHED"
    elif result["total_score"] >= 4 and themes:
        result["track"] = "BUY_NOW"
    else:
        result["track"] = "READY"

    return result

# ────────────────────────────────────────
# 6. 메인 파이프라인
# ────────────────────────────────────────
def run_pipeline(news_data, dart_signals, theme_scores):
    # 유니버스 로드
    universe = load_kosdaq_universe()
    if universe.empty:
        log.error("유니버스 로드 실패")
        return {"READY": [], "BUY_NOW": [], "LAUNCHED": []}

    total = len(universe)

    # 사전 맵 1회 생성 (핵심: 이후 종목별 루프에서 API 호출 없음)
    log.info("뉴스 언급 맵 및 테마 맵 사전 생성 중...")
    mention_map = build_news_mention_map(news_data, universe)
    theme_map = build_theme_match_map(universe)

    log.info(f"필터링 시작: {total}종목 대상")

    ready, buy_now, launched = [], [], []

    for i, (_, row) in enumerate(universe.iterrows()):
        result = analyze_stock(row, dart_signals, theme_scores, mention_map, theme_map, news_data)

        if result["track"] == "READY":
            ready.append(result)
        elif result["track"] == "BUY_NOW":
            buy_now.append(result)
        elif result["track"] == "LAUNCHED":
            launched.append(result)

        if (i + 1) % 30 == 0:
            log.info(f"  진행: {i+1}/{total} | READY {len(ready)} / BUY_NOW {len(buy_now)} / LAUNCHED {len(launched)}")

    # 정렬
    ready.sort(key=lambda x: -x["total_score"])
    buy_now.sort(key=lambda x: -x["total_score"])
    launched.sort(key=lambda x: -x["total_score"])

    ready = ready[:TOP_N]
    buy_now = buy_now[:TOP_N]
    launched = launched[:TOP_N]

    log.info(f"필터링 완료 | READY {len(ready)} / BUY_NOW {len(buy_now)} / LAUNCHED {len(launched)}")
    return {"READY": ready, "BUY_NOW": buy_now, "LAUNCHED": launched}
