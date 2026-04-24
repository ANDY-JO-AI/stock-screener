"""
theme_engine.py — 테마 온도 계산 엔진 v3
시간여행TV 기준: 정치/정책/계절/사회 이슈 연계 테마 분류
"""

import logging
import re
from collections import defaultdict
from datetime import datetime

log = logging.getLogger(__name__)

# ────────────────────────────────────────────
# 테마 키워드 맵 (17개 테마)
# ────────────────────────────────────────────
THEME_KEYWORDS = {
    "방산": ["방산", "국방", "무기", "미사일", "드론방어", "K방산", "군수", "전투기", "탄약", "방위산업"],
    "원전": ["원전", "핵발전", "SMR", "소형모듈원자로", "원자력", "우라늄", "핵융합", "원자로"],
    "로봇_AI": ["로봇", "AI", "인공지능", "자율주행", "휴머노이드", "협동로봇", "딥러닝", "ChatGPT", "LLM", "생성형AI"],
    "2차전지": ["2차전지", "배터리", "전기차", "리튬", "양극재", "음극재", "전해질", "배터리셀", "ESS"],
    "바이오": ["바이오", "신약", "임상", "FDA", "항암", "치료제", "의약품", "백신", "유전자치료", "mRNA"],
    "반도체": ["반도체", "HBM", "웨이퍼", "파운드리", "D램", "낸드", "칩", "반도체장비", "노광"],
    "수소": ["수소", "수소차", "수소경제", "연료전지", "수전해", "그린수소", "수소충전"],
    "철강_플랜트": ["철강", "플랜트", "제철", "조선", "강판", "후판", "H형강", "EPC"],
    "금_귀금속": ["금", "귀금속", "은", "구리", "원자재", "골드", "금값", "귀금속시장"],
    "엔터_콘텐츠": ["엔터", "K팝", "아이돌", "콘텐츠", "드라마", "OTT", "영화", "방송", "웹툰"],
    "선거_정치": ["선거", "대선", "총선", "대통령", "정치", "여당", "야당", "공약", "탄핵", "국정"],
    "미중갈등": ["미중", "관세", "무역전쟁", "중국규제", "디커플링", "반도체수출통제", "IRA"],
    "친환경": ["친환경", "ESG", "탄소중립", "태양광", "풍력", "RE100", "탄소배출권", "그린뉴딜"],
    "부동산_건설": ["부동산", "재건축", "재개발", "아파트", "분양", "건설", "PF", "시행사"],
    "의료기기": ["의료기기", "내시경", "수술로봇", "진단키트", "의료AI", "체외진단"],
    "게임": ["게임", "모바일게임", "PC게임", "메타버스", "NFT", "블록체인게임"],
    "해운_물류": ["해운", "물류", "컨테이너", "벌크선", "HMM", "운임", "SCM"]
}

# ────────────────────────────────────────────
# 정치/정책 연계 테마 (가중치 ×1.5)
# ────────────────────────────────────────────
POLITICAL_THEMES = {"방산", "선거_정치", "원전", "친환경", "미중갈등", "부동산_건설"}

# ────────────────────────────────────────────
# 계절/반복 테마 (보너스 +0.5)
# ────────────────────────────────────────────
SEASONAL_THEMES = {
    1:  ["부동산_건설"],
    2:  ["친환경"],
    3:  ["선거_정치", "부동산_건설"],
    4:  ["선거_정치"],
    5:  ["엔터_콘텐츠"],
    6:  ["친환경", "수소"],
    7:  ["엔터_콘텐츠"],
    8:  ["엔터_콘텐츠"],
    9:  ["부동산_건설"],
    10: ["방산", "선거_정치"],
    11: ["2차전지"],
    12: ["2차전지", "반도체"]
}


def _count_theme_mentions(news_list: list, keywords: list) -> int:
    """뉴스 리스트에서 특정 키워드 등장 횟수 합산"""
    count = 0
    for item in news_list:
        title = item.get("title", "") + " " + item.get("summary", "")
        for kw in keywords:
            if kw in title:
                count += 1
                break  # 한 기사에서 같은 테마 중복 카운트 방지
    return count


def _count_stock_theme_match(universe_df, keywords: list) -> int:
    """유니버스 종목명에서 테마 키워드 매칭 종목 수"""
    if universe_df is None or universe_df.empty:
        return 0
    count = 0
    for name in universe_df["Name"].fillna(""):
        for kw in keywords:
            if kw in name:
                count += 1
                break
    return count


def calculate_theme_scores(news_list: list, universe_df=None) -> dict:
    """
    테마별 온도 점수 계산
    반환: {
        "테마명": {
            "score": float,       # 0-10
            "grade": str,         # 🔥🔥 / 🔥 / 📈 / 👀 / 💤
            "news_count": int,
            "stock_count": int,
            "is_political": bool,
            "is_seasonal": bool,
            "keywords": list
        }
    }
    """
    current_month = datetime.now().month
    seasonal_this_month = SEASONAL_THEMES.get(current_month, [])
    results = {}

    log.info(f"[theme_engine] 테마 온도 계산 시작: 뉴스 {len(news_list)}건, 테마 {len(THEME_KEYWORDS)}개")

    for theme_name, keywords in THEME_KEYWORDS.items():
        # 1) 뉴스 언급 횟수
        news_count = _count_theme_mentions(news_list, keywords)

        # 2) 종목명 매칭 수
        stock_count = _count_stock_theme_match(universe_df, keywords)

        # 3) 기본 점수 계산
        #    뉴스 40% + 종목 20% 반영, 최대 10점
        news_score = min(news_count / 5.0, 4.0)   # 뉴스 20건 → 4점 만점
        stock_score = min(stock_count / 10.0, 2.0) # 종목 20개 → 2점 만점
        base_score = news_score + stock_score       # 최대 6점

        # 4) 정치/정책 보너스 (+1.5)
        political_bonus = 1.5 if theme_name in POLITICAL_THEMES and news_count >= 3 else 0.0

        # 5) 계절 보너스 (+0.5)
        seasonal_bonus = 0.5 if theme_name in seasonal_this_month else 0.0

        # 6) 뉴스 급증 보너스 (15건 이상이면 +1.0)
        surge_bonus = 1.0 if news_count >= 15 else (0.5 if news_count >= 8 else 0.0)

        # 7) 최종 점수
        total_score = round(min(base_score + political_bonus + seasonal_bonus + surge_bonus, 10.0), 2)

        # 8) 등급 분류
        if total_score >= 8.0:
            grade = "🔥🔥 과열"
        elif total_score >= 6.0:
            grade = "🔥 활성 (최적진입)"
        elif total_score >= 4.0:
            grade = "📈 형성중"
        elif total_score >= 2.0:
            grade = "👀 워밍업"
        else:
            grade = "💤 미활성"

        results[theme_name] = {
            "score": total_score,
            "grade": grade,
            "news_count": news_count,
            "stock_count": stock_count,
            "is_political": theme_name in POLITICAL_THEMES,
            "is_seasonal": theme_name in seasonal_this_month,
            "keywords": keywords
        }

    # 점수 내림차순 정렬
    results = dict(sorted(results.items(), key=lambda x: -x[1]["score"]))

    # 로그 출력 (TOP 5)
    log.info("[theme_engine] 테마 온도 TOP 5:")
    for i, (theme, data) in enumerate(list(results.items())[:5]):
        log.info(
            f"  {i+1}. {theme}: {data['score']}점 {data['grade']} "
            f"(뉴스 {data['news_count']}건 / 종목매칭 {data['stock_count']}개)"
        )

    return results


def get_top_themes(theme_scores: dict, n: int = 5, min_score: float = 2.0) -> list:
    """
    활성 테마 상위 N개 반환
    반환: [{"theme": str, "score": float, "grade": str, ...}, ...]
    """
    filtered = [
        {"theme": k, **v}
        for k, v in theme_scores.items()
        if v["score"] >= min_score
    ]
    return filtered[:n]


def match_stock_to_themes(stock_name: str, news_list: list, theme_scores: dict) -> dict:
    """
    개별 종목의 테마 매칭 결과 반환
    반환: {
        "matched_themes": list,   # 매칭된 테마명 리스트
        "theme_score": float,     # 최고 테마 점수
        "news_mentions": int,     # 관련 뉴스 언급수
        "theme_grade": str        # 최고 테마 등급
    }
    """
    matched = []
    total_news = 0

    for theme_name, keywords in THEME_KEYWORDS.items():
        # 종목명 직접 매칭
        name_match = any(kw in stock_name for kw in keywords)

        # 뉴스 내 언급
        news_count = _count_theme_mentions(news_list, keywords)

        if name_match or news_count >= 2:
            theme_data = theme_scores.get(theme_name, {})
            matched.append({
                "theme": theme_name,
                "score": theme_data.get("score", 0),
                "grade": theme_data.get("grade", "💤 미활성"),
                "news_count": news_count,
                "name_match": name_match
            })
            total_news += news_count

    # 테마 점수 내림차순 정렬
    matched.sort(key=lambda x: -x["score"])

    if matched:
        top = matched[0]
        return {
            "matched_themes": [m["theme"] for m in matched],
            "theme_score": top["score"],
            "news_mentions": total_news,
            "theme_grade": top["grade"]
        }
    else:
        return {
            "matched_themes": [],
            "theme_score": 0.0,
            "news_mentions": 0,
            "theme_grade": "💤 미활성"
        }
