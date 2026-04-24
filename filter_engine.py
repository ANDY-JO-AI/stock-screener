"""
Andy Jo Stock AI — 종목 필터링 엔진 v2
핵심 변경: 데이터 조회 실패 시 통과 처리, 필터 기준 완화, 속도 개선
"""

import logging
import time
import pandas as pd
from datetime import datetime, timedelta

log = logging.getLogger(__name__)

# ────────────────────────────────────────
# 상수 정의
# ────────────────────────────────────────
MKTCAP_MIN = 150   # 억원
MKTCAP_MAX = 700   # 억원
MIN_TOTAL_SCORE = 3  # 완화: 5 → 3
TOP_N = 50

# 즉시 탈락 키워드
REJECT_KEYWORDS = [
    "횡령", "배임", "불성실공시", "상장폐지", "관리종목",
    "워크아웃", "회생절차", "영업정지"
]

# 주의 키워드 (탈락 아님, 점수 감점)
WARN_KEYWORDS = [
    "전환사채", "신주인수권부사채", "유상증자", "제3자배정",
    "최대주주변경", "자본잠식"
]

# 테마 키워드 매핑
THEME_KEYWORDS = {
    "방산": ["방산", "방위", "무기", "미사일", "군수"],
    "원전": ["원전", "원자력", "SMR", "핵융합"],
    "로봇_AI": ["로봇", "협동로봇", "AI로봇", "휴머노이드"],
    "AI반도체": ["AI반도체", "HBM", "NPU", "GPU", "파운드리"],
    "2차전지": ["배터리", "2차전지", "양극재", "음극재", "전고체"],
    "바이오": ["바이오", "신약", "ADC", "mRNA", "임상"],
    "조선": ["조선", "LNG선", "컨테이너선", "수주"],
    "우주항공": ["우주", "위성", "발사체", "항공"],
    "수소": ["수소", "연료전지", "그린수소"],
    "가상화폐": ["비트코인", "코인", "블록체인", "가상자산"],
    "게임": ["게임", "메타버스", "P2E"],
    "엔터": ["K팝", "아이돌", "엔터", "한류"],
    "반도체장비": ["반도체장비", "식각", "증착", "포토"],
    "자동차": ["전기차", "자율주행", "전장", "ADAS"],
    "철강": ["철강", "제철", "H빔"],
}

# ────────────────────────────────────────
# 유니버스 로드 (시총 필터)
# ────────────────────────────────────────
def load_kosdaq_universe():
    try:
        import FinanceDataReader as fdr
        df = fdr.StockListing("KOSDAQ")
        df = df.dropna(subset=["Marcap"])
        df["Marcap_억"] = df["Marcap"] / 1e8
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
# L1: 즉시 탈락 필터
# ────────────────────────────────────────
def check_l1_reject(row, news_data):
    name = str(row.get("Name", ""))
    code = str(row.get("Code", ""))

    # 종목명 패턴 탈락
    reject_patterns = ["스팩", "SPAC", "리츠", "ETF", "ETN"]
    for pat in reject_patterns:
        if pat in name:
            return True, f"종목명 패턴: {pat}"

    # 뉴스 키워드 탈락
    for article in news_data[:200]:
        title = article.get("title", "")
        if name in title or code in title:
            for kw in REJECT_KEYWORDS:
                if kw in title:
                    return True, f"뉴스 키워드: {kw}"

    return False, ""

# ────────────────────────────────────────
# L2: 재무 점수 (완화된 기준)
# ────────────────────────────────────────
def calc_l2_financial_score(row):
    score = 0
    reasons = []

    try:
        # 시가총액 적정 범위 (이미 필터됨, 기본 점수 부여)
        marcap = float(row.get("Marcap_억", 0))
        if 150 <= marcap <= 700:
            score += 3
            reasons.append(f"시총 적정({marcap:.0f}억)")

        # 등락률 (당일)
        change = float(row.get("ChangeRatio", 0) or 0)
        if change > 3:
            score += 3
            reasons.append(f"당일 상승({change:.1f}%)")
        elif change > 0:
            score += 1

        # 거래량 (데이터 있을 때만)
        volume = float(row.get("Volume", 0) or 0)
        if volume > 500000:
            score += 2
            reasons.append("거래량 충분")
        elif volume > 100000:
            score += 1

        # 기본 점수 (데이터 없어도 최소 통과)
        if score == 0:
            score = 3
            reasons.append("기본점수")

    except Exception as e:
        score = 3
        reasons.append(f"점수계산오류-기본통과")

    return score, reasons

# ────────────────────────────────────────
# L3: 유동성 필터 (완화)
# ────────────────────────────────────────
def check_l3_liquidity(row):
    try:
        volume = float(row.get("Volume", 0) or 0)
        price = float(row.get("Close", 0) or 0)
        if price <= 0:
            return True, 0  # 데이터 없으면 통과
        turnover = volume * price / 1e8
        if turnover >= 3:  # 완화: 10억 → 3억
            return True, turnover
        return False, turnover
    except Exception:
        return True, 0  # 오류 시 통과

# ────────────────────────────────────────
# L4: DART 공시 점수
# ────────────────────────────────────────
def check_l4_dart(code, dart_signals):
    if not dart_signals or code not in dart_signals:
        return True, 5, "DART 데이터 없음(통과)"  # 완화: 없으면 통과
    signal = dart_signals[code]
    passed = signal.get("pass", True)
    score = signal.get("score", 5)
    reason = signal.get("reason", "")
    return passed, score, reason

# ────────────────────────────────────────
# L5: 테마 점수
# ────────────────────────────────────────
def calc_l5_theme_score(row, news_data, theme_scores):
    name = str(row.get("Name", ""))
    code = str(row.get("Code", ""))
    score = 0
    matched_themes = []

    # 뉴스 언급 점수
    mention_count = 0
    for article in news_data[:300]:
        title = article.get("title", "")
        if name in title or code in title:
            mention_count += 1

    news_bonus = min(mention_count * 2, 6)
    score += news_bonus

    # 테마 온도 점수
    for theme, kws in THEME_KEYWORDS.items():
        for kw in kws:
            if kw in name:
                theme_temp = theme_scores.get(theme, {})
                if isinstance(theme_temp, dict):
                    t_score = theme_temp.get("score", 0)
                else:
                    t_score = float(theme_temp) if theme_temp else 0
                score += t_score * 0.8
                matched_themes.append(theme)
                break

    return round(score, 2), matched_themes

# ────────────────────────────────────────
# L6: 타이밍 체크 (완화)
# ────────────────────────────────────────
def check_l6_timing(row):
    try:
        change = float(row.get("ChangeRatio", 0) or 0)
        # 당일 -10% 이상 급락 시만 탈락
        if change < -10:
            return False, f"당일급락({change:.1f}%)"
        return True, "타이밍OK"
    except Exception:
        return True, "타이밍OK"

# ────────────────────────────────────────
# LAUNCHED 체크
# ────────────────────────────────────────
def check_launched(row):
    try:
        change = float(row.get("ChangeRatio", 0) or 0)
        if change >= 20:
            return True
    except Exception:
        pass
    return False

# ────────────────────────────────────────
# 단일 종목 분석
# ────────────────────────────────────────
def analyze_stock(row, news_data, dart_signals, theme_scores):
    code = str(row.get("Code", ""))
    name = str(row.get("Name", ""))
    result = {
        "code": code, "name": name,
        "mktcap": round(float(row.get("Marcap_억", 0)), 1),
        "price": float(row.get("Close", 0) or 0),
        "change": float(row.get("ChangeRatio", 0) or 0),
        "volume": float(row.get("Volume", 0) or 0),
        "track": None, "total_score": 0,
        "l2_score": 0, "l4_score": 5, "l5_score": 0,
        "themes": [], "reject_reason": ""
    }

    # L1
    rejected, reason = check_l1_reject(row, news_data)
    if rejected:
        result["reject_reason"] = reason
        return result

    # L2
    l2_score, l2_reasons = calc_l2_financial_score(row)
    result["l2_score"] = l2_score

    # L3
    l3_pass, turnover = check_l3_liquidity(row)
    if not l3_pass:
        result["reject_reason"] = f"유동성부족({turnover:.1f}억)"
        return result

    # L4
    l4_pass, l4_score, l4_reason = check_l4_dart(code, dart_signals)
    result["l4_score"] = l4_score
    if not l4_pass:
        result["reject_reason"] = f"DART탈락: {l4_reason}"
        return result

    # L5
    l5_score, themes = calc_l5_theme_score(row, news_data, theme_scores)
    result["l5_score"] = l5_score
    result["themes"] = themes

    # 총점
    total = (l2_score * 0.3) + (l4_score * 0.3) + (l5_score * 0.4)
    result["total_score"] = round(total, 2)

    if result["total_score"] < MIN_TOTAL_SCORE:
        result["reject_reason"] = f"점수미달({result['total_score']:.1f})"
        return result

    # L6
    l6_pass, l6_reason = check_l6_timing(row)
    if not l6_pass:
        result["reject_reason"] = l6_reason
        return result

    # 트랙 분류
    if check_launched(row):
        result["track"] = "LAUNCHED"
    elif result["total_score"] >= 5 and themes:
        result["track"] = "BUY_NOW"
    else:
        result["track"] = "READY"

    return result

# ────────────────────────────────────────
# 메인 파이프라인
# ────────────────────────────────────────
def run_pipeline(news_data, dart_signals, theme_scores):
    universe = load_kosdaq_universe()
    if universe.empty:
        log.error("유니버스 로드 실패")
        return {"READY": [], "BUY_NOW": [], "LAUNCHED": []}

    total = len(universe)
    log.info(f"필터링 시작: {total}종목 대상")

    ready, buy_now, launched = [], [], []

    for i, (_, row) in enumerate(universe.iterrows()):
        result = analyze_stock(row, news_data, dart_signals, theme_scores)

        if result["track"] == "READY":
            ready.append(result)
        elif result["track"] == "BUY_NOW":
            buy_now.append(result)
        elif result["track"] == "LAUNCHED":
            launched.append(result)

        if (i + 1) % 30 == 0:
            log.info(f"  진행: {i+1}/{total} | READY {len(ready)} / BUY_NOW {len(buy_now)} / LAUNCHED {len(launched)}")

    # 점수 정렬
    ready.sort(key=lambda x: -x["total_score"])
    buy_now.sort(key=lambda x: -x["total_score"])
    launched.sort(key=lambda x: -x["total_score"])

    # 상위 N개만
    ready = ready[:TOP_N]
    buy_now = buy_now[:TOP_N]
    launched = launched[:TOP_N]

    log.info(f"필터링 완료 | READY {len(ready)} / BUY_NOW {len(buy_now)} / LAUNCHED {len(launched)}")
    return {"READY": ready, "BUY_NOW": buy_now, "LAUNCHED": launched}
