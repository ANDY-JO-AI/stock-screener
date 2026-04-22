# theme_engine.py
import datetime
import re
from collections import defaultdict
from data_store import save_theme_data, load_news_data

# 테마 키워드 사전 (시간여행TV 기준 + 확장)
THEME_DICT = {
    "방산":        ["방산", "방위산업", "K방산", "무기", "탄약", "레이더", "유도탄",
                    "한화에어로", "LIG넥스원", "빅텍", "퍼스텍", "휴니드"],
    "로봇/AI":     ["로봇", "AI", "인공지능", "자율주행", "협동로봇", "스마트팩토리",
                    "두산로보틱스", "레인보우로보틱스", "뉴로메카"],
    "2차전지":     ["2차전지", "배터리", "전기차", "ESS", "양극재", "음극재",
                    "에코프로", "포스코퓨처엠", "엘앤에프", "천보"],
    "바이오":      ["바이오", "신약", "임상", "FDA", "항암", "치료제", "백신",
                    "셀트리온", "삼성바이오", "한미약품", "유한양행"],
    "반도체":      ["반도체", "HBM", "파운드리", "웨이퍼", "메모리", "낸드",
                    "SK하이닉스", "삼성전자", "한미반도체", "리노공업"],
    "조선":        ["조선", "LNG선", "컨테이너선", "해양플랜트",
                    "HD현대중공업", "한화오션", "삼성중공업"],
    "원전":        ["원전", "SMR", "핵융합", "원자력", "체코원전",
                    "두산에너빌리티", "한전기술", "보성파워텍"],
    "정치테마":    ["대선", "총선", "대통령", "후보", "여당", "야당",
                    "국민의힘", "민주당", "정책"],
    "대북/통일":   ["대북", "통일", "남북", "비핵화", "개성공단", "철도", "도로"],
    "미세먼지":    ["미세먼지", "공기청정", "마스크", "황사", "대기오염"],
    "조류독감":    ["조류독감", "AI 바이러스", "구제역", "살처분", "수산물"],
    "저출산":      ["저출산", "출산율", "인구감소", "육아", "보육"],
    "에너지":      ["태양광", "풍력", "수소", "신재생에너지", "탄소중립"],
    "건설":        ["건설", "재건축", "재개발", "부동산", "아파트", "건축"],
    "엔터/미디어": ["엔터", "K-POP", "드라마", "영화", "웹툰", "하이브", "SM", "JYP"],
    "화폐/핀테크": ["화폐개혁", "핀테크", "간편결제", "ATM", "디지털화폐", "CBDC"],
    "키오스크":    ["키오스크", "최저임금", "무인", "자동화", "비대면"],
    "우주/항공":   ["우주", "위성", "발사체", "항공", "드론", "UAM"],
}


def classify_news(news_list: list) -> dict:
    """뉴스 리스트를 테마별로 분류"""
    theme_news = defaultdict(list)

    for news in news_list:
        title = news.get("title", "")
        desc  = news.get("desc", "")
        text  = title + " " + desc

        for theme, keywords in THEME_DICT.items():
            for kw in keywords:
                if kw in text:
                    theme_news[theme].append(news)
                    break  # 같은 뉴스를 테마에 중복 추가 방지

    return dict(theme_news)


def find_leader_stock(theme: str, candidates_df=None) -> str:
    """테마별 대장주 선정 — 후보종목 중 테마 연관 + 점수 최고"""
    if candidates_df is None or candidates_df.empty:
        # 기본 대장주 사전
        default_leaders = {
            "방산":        "한화에어로스페이스",
            "로봇/AI":     "두산로보틱스",
            "2차전지":     "에코프로",
            "바이오":      "셀트리온",
            "반도체":      "SK하이닉스",
            "조선":        "HD현대중공업",
            "원전":        "두산에너빌리티",
            "에너지":      "한화솔루션",
            "건설":        "GS건설",
            "엔터/미디어": "하이브",
        }
        return default_leaders.get(theme, "—")

    # 후보종목 중 해당 테마 연관 종목 찾기
    kw_list = THEME_DICT.get(theme, [])
    for kw in kw_list:
        mask = candidates_df["name"].str.contains(kw, na=False)
        filtered = candidates_df[mask]
        if not filtered.empty:
            # 총점 가장 높은 종목
            leader_row = filtered.sort_values(
                "total_score", ascending=False
            ).iloc[0]
            return leader_row["name"]
    return "—"


def build_theme_report(news_list: list, candidates_df=None) -> list:
    """테마 리포트 생성 (저장용)"""
    theme_news = classify_news(news_list)

    # 뉴스 건수 기준 상위 테마 정렬
    sorted_themes = sorted(
        theme_news.items(), key=lambda x: len(x[1]), reverse=True
    )

    result = []
    for theme, news_items in sorted_themes[:15]:
        leader = find_leader_stock(theme, candidates_df)

        # 시너지 뉴스: 같은 테마에서 2개 이상 다른 소스 보도
        sources = list(set(n.get("source", "") for n in news_items))
        synergy = len(sources) >= 2

        result.append({
            "theme":       theme,
            "count":       len(news_items),
            "leader":      leader,
            "synergy":     synergy,
            "sources":     sources[:5],
            "top_news":    [
                {"title": n["title"], "url": n.get("url",""), "source": n.get("source","")}
                for n in news_items[:5]
            ],
            "keywords":    THEME_DICT.get(theme, [])[:5],
            "updated_at":  datetime.datetime.now().strftime("%Y-%m-%d %H:%M"),
        })

    save_theme_data(result)
    print(f"[THEME] {len(result)}개 테마 저장 완료")
    return result


if __name__ == "__main__":
    news = load_news_data()
    if not news:
        print("[THEME] 뉴스 없음 — news_engine.py 먼저 실행")
    else:
        report = build_theme_report(news)
        for t in report[:5]:
            print(f"[{t['theme']}] {t['count']}건 | 대장주: {t['leader']} | 시너지: {t['synergy']}")
