"""
Andy Jo Stock AI — 테마 온도 계산 엔진
뉴스 신호 40% + 거래량 신호 40% + 주가 신호 20% + 커뮤니티 신호 가산
"""

import os
import logging
import pandas as pd
from datetime import datetime, timedelta

log = logging.getLogger(__name__)

# ────────────────────────────────────────
# 1. 테마-종목 매핑 DB (30개 테마)
# ────────────────────────────────────────
THEME_DB = {
    "방산": {
        "keywords": ["방산", "방위산업", "K방산", "무기", "미사일", "군수"],
        "stocks": ["012450", "047810", "079550", "064350", "010140", "033250", "087010"]
        # 한화에어로스페이스, 한화시스템, LIG넥스원, 현대로템, 빅텍, 퍼스텍, 한국항공우주
    },
    "원전": {
        "keywords": ["원전", "원자력", "SMR", "핵융합", "소형모듈원자로"],
        "stocks": ["034020", "257720", "298040", "090460", "071970"]
        # 두산에너빌리티, 한전기술, 한전KPS, 비에이치아이, 에스에너지
    },
    "로봇_AI": {
        "keywords": ["로봇", "협동로봇", "자율주행로봇", "AI로봇", "휴머노이드"],
        "stocks": ["277810", "108490", "060380", "090355", "348210"]
        # 레인보우로보틱스, 로보티즈, 에스피지, 로보스타, 엑스로보틱스
    },
    "AI반도체": {
        "keywords": ["AI반도체", "HBM", "NPU", "GPU", "엔비디아", "파운드리"],
        "stocks": ["000660", "005930", "058470", "399720", "042700"]
        # SK하이닉스, 삼성전자, 리노공업, 신성에스티, 한미반도체
    },
    "2차전지": {
        "keywords": ["배터리", "2차전지", "양극재", "음극재", "전고체", "리튬"],
        "stocks": ["086520", "373220", "247540", "003670", "006400"]
        # 에코프로, 에코프로비엠, 엘앤에프, POSCO홀딩스, 삼성SDI
    },
    "바이오": {
        "keywords": ["바이오", "신약", "ADC", "mRNA", "임상", "FDA승인"],
        "stocks": ["207940", "068270", "196170", "145720", "950130"]
        # 삼성바이오로직스, 셀트리온, 알테오젠, 일동제약, 엑스바이오텍
    },
    "조선": {
        "keywords": ["조선", "LNG선", "컨테이너선", "수주잔고", "해양플랜트"],
        "stocks": ["009540", "010140", "042660", "005380", "000270"]
        # HD현대중공업, 한국항공우주, 한화오션, 현대차, 기아
    },
    "건설_리츠": {
        "keywords": ["재건축", "재개발", "PF", "리츠", "건설", "분양"],
        "stocks": ["006360", "000720", "375500", "012630", "047040"]
        # GS건설, 현대건설, DL이앤씨, 한화건설, 대우건설
    },
    "우주항공": {
        "keywords": ["우주", "위성", "발사체", "누리호", "항공"],
        "stocks": ["047810", "012450", "099410", "239610"]
        # 한화시스템, 한화에어로스페이스, 이노스페이스, 이수페타시스
    },
    "수소_친환경": {
        "keywords": ["수소", "수소차", "연료전지", "그린수소", "탄소중립"],
        "stocks": ["336260", "298040", "095500", "046890"]
        # 두산퓨얼셀, 한전KPS, 미래엔, 우리산업
    },
    "남북경협": {
        "keywords": ["남북", "경협", "통일", "비핵화", "개성"],
        "stocks": ["003490", "035890", "008260", "041140"]
        # 대한항공, 서울식품, 맥스로텍, 우리들휴브레인
    },
    "가상화폐": {
        "keywords": ["비트코인", "이더리움", "코인", "블록체인", "가상자산"],
        "stocks": ["215600", "030520", "376300", "950200"]
        # 덱스터, 비덴트, 비트코인ETF관련, 코인원
    },
    "게임_메타버스": {
        "keywords": ["게임", "메타버스", "P2E", "NFT게임"],
        "stocks": ["259960", "036570", "251270", "293490"]
        # 크래프톤, 엔씨소프트, 넷마블, 카카오게임즈
    },
    "엔터_K팝": {
        "keywords": ["K팝", "아이돌", "엔터", "음악", "한류"],
        "stocks": ["352820", "041510", "035900", "와이지"]
        # 하이브, SM, JYP, YG
    },
    "의료기기": {
        "keywords": ["의료기기", "디지털헬스", "의료AI", "원격진료"],
        "stocks": ["041830", "228760", "285490", "214430"]
        # 인바디, 오스코텍, 레이, 에이치엘비
    },
    "보안_사이버": {
        "keywords": ["사이버보안", "정보보안", "해킹", "랜섬웨어", "보안"],
        "stocks": ["053800", "067920", "119650", "214270"]
        # 안랩, 이글루시큐리티, 인터리젠, 에이쓰리시큐리티
    },
    "자동차_전장": {
        "keywords": ["전기차", "자율주행", "전장", "ADAS", "자동차부품"],
        "stocks": ["005380", "000270", "012330", "018880", "060980"]
        # 현대차, 기아, 현대모비스, 한온시스템, 삼본정기
    },
    "반도체장비": {
        "keywords": ["반도체장비", "식각", "증착", "CMP", "포토"],
        "stocks": ["084370", "240810", "131970", "036540", "079370"]
        # 유진테크, 원익IPS, 피에스케이, 솔브레인, 코미코
    },
    "트럼프테마": {
        "keywords": ["트럼프", "관세", "미국우선", "IRA", "미국인프라"],
        "stocks": ["005490", "011790", "047810", "012450"]
        # POSCO홀딩스, SKC, 한화에어로스페이스, 한화시스템
    },
    "이재명테마": {
        "keywords": ["이재명", "민주당", "대선", "대권"],
        "stocks": ["000270", "005490", "012630"]
        # 기아, POSCO홀딩스, 한화건설
    },
    "5G_통신": {
        "keywords": ["5G", "통신", "기지국", "네트워크장비"],
        "stocks": ["017670", "030200", "032640", "214450"]
        # SK텔레콤, KT, LG유플러스, 파인디지털
    },
    "금_귀금속": {
        "keywords": ["금", "귀금속", "금값", "골드", "은"],
        "stocks": ["086260", "001790", "019440", "010780"]
        # SK스퀘어, 대한제련, 풍산, 이씨에스
    },
    "제약": {
        "keywords": ["제약", "복제약", "제네릭", "원료의약품"],
        "stocks": ["128940", "145720", "003090", "067000"]
        # 한미약품, 일동제약, 대웅제약, 조아제약
    },
    "화학_소재": {
        "keywords": ["화학", "소재", "폴리머", "정밀화학"],
        "stocks": ["011170", "010955", "096770", "051910"]
        # 롯데케미칼, S-Oil, SK이노베이션, LG화학
    },
    "철강_플랜트": {
        "keywords": ["철강", "플랜트", "제철", "H빔"],
        "stocks": ["005490", "004020", "010060", "002220"]
        # POSCO홀딩스, 현대제철, OCI, 동국제강
    },
    "금융_보험": {
        "keywords": ["금융", "보험", "은행", "증권", "카드"],
        "stocks": ["105560", "055550", "086790", "000810"]
        # KB금융, 신한지주, 하나금융, 삼성화재
    },
    "식품_음료": {
        "keywords": ["식품", "음료", "K푸드", "라면", "음식"],
        "stocks": ["097950", "007310", "003230", "271560"]
        # CJ제일제당, 오뚜기, 롯데칠성, 오리온
    },
    "유통_물류": {
        "keywords": ["유통", "물류", "이커머스", "쿠팡", "택배"],
        "stocks": ["069960", "023530", "117270", "001040"]
        # 현대백화점, 롯데쇼핑, 티웨이항공, CJ
    },
    "미디어_OTT": {
        "keywords": ["OTT", "미디어", "콘텐츠", "드라마", "영화"],
        "stocks": ["034120", "035420", "067160", "259960"]
        # SBS, NAVER, 아프리카TV, 크래프톤
    },
    "미국증시연동": {
        "keywords": ["나스닥", "다우", "S&P", "미국증시", "뉴욕증시"],
        "stocks": ["005930", "000660", "035420", "035720"]
        # 삼성전자, SK하이닉스, NAVER, 카카오
    },
}


# ────────────────────────────────────────
# 2. 뉴스 기반 테마 신호 계산
# ────────────────────────────────────────
def calc_news_signal(news_data: list) -> dict:
    """
    뉴스 제목에서 테마별 언급 빈도 계산
    반환: {테마명: 0-10 점수}
    """
    theme_count = {theme: 0 for theme in THEME_DB}
    total = max(len(news_data), 1)

    for article in news_data:
        title = article.get("title", "")
        for theme, info in THEME_DB.items():
            for kw in info["keywords"]:
                if kw in title:
                    theme_count[theme] += 1
                    break  # 테마당 1회만 카운트

    # 정규화 (최대 10점)
    max_count = max(theme_count.values()) if theme_count.values() else 1
    signals = {}
    for theme, count in theme_count.items():
        signals[theme] = round(count / max_count * 10, 2)

    return signals


# ────────────────────────────────────────
# 3. 거래량 기반 테마 신호 계산
# ────────────────────────────────────────
def calc_volume_signal(theme_db: dict) -> dict:
    """
    테마 구성 종목들의 거래량 변화 계산
    반환: {테마명: 0-10 점수}
    """
    try:
        import FinanceDataReader as fdr
        from pykrx import stock as krx

        today     = datetime.now().strftime("%Y%m%d")
        yesterday = (datetime.now() - timedelta(days=3)).strftime("%Y%m%d")

        signals = {}
        for theme, info in theme_db.items():
            stock_codes = info.get("stocks", [])
            if not stock_codes:
                signals[theme] = 0
                continue

            volume_ratios = []
            for code in stock_codes[:5]:  # 테마당 최대 5종목
                try:
                    df = fdr.DataReader(code, yesterday, today)
                    if len(df) >= 2:
                        recent_vol = df["Volume"].iloc[-1]
                        prev_vol   = df["Volume"].iloc[-2]
                        if prev_vol > 0:
                            volume_ratios.append(recent_vol / prev_vol)
                except Exception:
                    continue

            if volume_ratios:
                avg_ratio = sum(volume_ratios) / len(volume_ratios)
                # 거래량 2배 이상이면 10점, 1배면 0점
                score = min((avg_ratio - 1) * 10, 10)
                signals[theme] = round(max(score, 0), 2)
            else:
                signals[theme] = 0

        return signals

    except Exception as e:
        log.warning(f"거래량 신호 계산 실패: {e}")
        return {theme: 0 for theme in theme_db}


# ────────────────────────────────────────
# 4. 주가 변동 기반 테마 신호 계산
# ────────────────────────────────────────
def calc_price_signal(theme_db: dict) -> dict:
    """
    테마 구성 종목들의 주가 변동률 평균 계산
    반환: {테마명: 0-10 점수}
    """
    try:
        import FinanceDataReader as fdr

        today     = datetime.now().strftime("%Y%m%d")
        yesterday = (datetime.now() - timedelta(days=3)).strftime("%Y%m%d")

        signals = {}
        for theme, info in theme_db.items():
            stock_codes = info.get("stocks", [])
            if not stock_codes:
                signals[theme] = 0
                continue

            change_rates = []
            for code in stock_codes[:5]:
                try:
                    df = fdr.DataReader(code, yesterday, today)
                    if len(df) >= 2:
                        prev_close   = df["Close"].iloc[-2]
                        today_close  = df["Close"].iloc[-1]
                        if prev_close > 0:
                            change_rate = (today_close - prev_close) / prev_close * 100
                            change_rates.append(change_rate)
                except Exception:
                    continue

            if change_rates:
                avg_change = sum(change_rates) / len(change_rates)
                # 등락률 +5% 이상이면 10점, 0%면 5점, -5% 이하면 0점
                score = min(max((avg_change + 5) * 1.0, 0), 10)
                signals[theme] = round(score, 2)
            else:
                signals[theme] = 5  # 데이터 없을 때 중립값

        return signals

    except Exception as e:
        log.warning(f"주가 신호 계산 실패: {e}")
        return {theme: 5 for theme in theme_db}


# ────────────────────────────────────────
# 5. 커뮤니티 신호 추출
# ────────────────────────────────────────
def extract_community_signal(news_data: list) -> dict:
    """
    news_engine에서 계산된 커뮤니티 신호 추출
    반환: {테마명: 신호점수}
    """
    for article in news_data:
        if "community_signals" in article:
            return article["community_signals"]
    return {theme: 0 for theme in THEME_DB}


# ────────────────────────────────────────
# 6. 테마 온도 지수 통합 계산 (메인 함수)
# ────────────────────────────────────────
def calculate_theme_scores(news_data: list) -> dict:
    """
    테마 온도 지수 = 뉴스신호 × 0.35 + 거래량신호 × 0.35 + 주가신호 × 0.15 + 커뮤니티신호 × 0.15
    반환: {테마명: {"score": float, "rank": int, "news": float, "volume": float, "price": float, "community": float}}
    """
    log.info("테마 온도 계산 시작")

    news_sig      = calc_news_signal(news_data)
    volume_sig    = calc_volume_signal(THEME_DB)
    price_sig     = calc_price_signal(THEME_DB)
    community_sig = extract_community_signal(news_data)

    results = {}
    for theme in THEME_DB:
        n = news_sig.get(theme, 0)
        v = volume_sig.get(theme, 0)
        p = price_sig.get(theme, 5)
        c = community_sig.get(theme, 0)

        # 테마 온도 공식
        score = (n * 0.35) + (v * 0.35) + (p * 0.15) + (c * 0.15)

        results[theme] = {
            "score":     round(score, 2),
            "news":      n,
            "volume":    v,
            "price":     p,
            "community": c,
            "stocks":    THEME_DB[theme]["stocks"]
        }

    # 랭킹 부여
    ranked = sorted(results.items(), key=lambda x: -x[1]["score"])
    for rank, (theme, data) in enumerate(ranked, 1):
        results[theme]["rank"] = rank

    top3 = [(t, round(d["score"], 2)) for t, d in ranked[:3]]
    log.info(f"테마 온도 TOP3: {top3}")

    return results


# ────────────────────────────────────────
# 7. 특정 종목의 테마 매핑 조회
# ────────────────────────────────────────
def get_stock_themes(stock_code: str) -> list:
    """
    종목코드 → 해당 종목이 속한 테마 리스트 반환
    """
    matched = []
    for theme, info in THEME_DB.items():
        if stock_code in info.get("stocks", []):
            matched.append(theme)
    return matched


# ────────────────────────────────────────
# 8. 대장주 판별
# ────────────────────────────────────────
def get_leader_stocks(theme: str, theme_scores: dict) -> list:
    """
    테마 내 대장주 TOP3 반환
    기준: 거래량비율 0.5 + 가격변동 0.3 + 시가총액역비율 0.2
    """
    try:
        import FinanceDataReader as fdr

        stock_codes = THEME_DB.get(theme, {}).get("stocks", [])
        if not stock_codes:
            return []

        today     = datetime.now().strftime("%Y%m%d")
        yesterday = (datetime.now() - timedelta(days=3)).strftime("%Y%m%d")

        scored = []
        for code in stock_codes:
            try:
                df = fdr.DataReader(code, yesterday, today)
                if len(df) < 2:
                    continue

                prev_vol   = df["Volume"].iloc[-2]
                today_vol  = df["Volume"].iloc[-1]
                prev_close = df["Close"].iloc[-2]
                today_close= df["Close"].iloc[-1]

                vol_ratio    = today_vol / prev_vol if prev_vol > 0 else 1
                price_change = (today_close - prev_close) / prev_close if prev_close > 0 else 0

                # 시가총액 (간이 계산)
                mktcap = today_close * today_vol

                score = (vol_ratio * 0.5) + (price_change * 100 * 0.3)

                scored.append({
                    "code":   code,
                    "score":  round(score, 2),
                    "change": round(price_change * 100, 2),
                    "volume": today_vol
                })
            except Exception:
                continue

        scored.sort(key=lambda x: -x["score"])
        return scored[:3]

    except Exception as e:
        log.warning(f"대장주 판별 실패 ({theme}): {e}")
        return []
