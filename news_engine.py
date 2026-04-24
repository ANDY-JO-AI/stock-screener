"""
Andy Jo Stock AI — 뉴스 + 커뮤니티 수집 엔진
언론 25개 채널 + 커뮤니티 5개 = 하루 1,000건 이상 목표
"""

import os
import time
import logging
import hashlib
import requests
import feedparser
from datetime import datetime, timedelta
from bs4 import BeautifulSoup

log = logging.getLogger(__name__)

# ────────────────────────────────────────
# 1. 언론 RSS 소스 25개
# ────────────────────────────────────────
RSS_SOURCES = [
    # 기존 10개
    {"name": "한국경제",        "url": "https://www.hankyung.com/feed/all-news"},
    {"name": "한국경제-증권",    "url": "https://www.hankyung.com/feed/finance"},
    {"name": "매일경제",        "url": "https://www.mk.co.kr/rss/40300001/"},
    {"name": "매일경제-증권",    "url": "https://www.mk.co.kr/rss/30100041/"},
    {"name": "연합뉴스-증권",    "url": "https://www.yna.co.kr/economy/stock/1/rss.xml"},
    {"name": "머니투데이",       "url": "https://www.mt.co.kr/rss/mainnews.xml"},
    {"name": "이데일리",        "url": "https://www.edaily.co.kr/rss/edailyrss.xml"},
    {"name": "뉴시스-금융",     "url": "https://newsis.com/rss/finance.rss"},
    {"name": "네이버금융-증권",  "url": "https://finance.naver.com/news/rss.naver?mode=LSS2D&section_id=101&section_id2=258"},
    {"name": "네이버금융-시황",  "url": "https://finance.naver.com/news/rss.naver?mode=LSS2D&section_id=101&section_id2=259"},
    # 추가 15개
    {"name": "서울경제",        "url": "https://www.sedaily.com/RSS/Economy"},
    {"name": "서울경제-증권",    "url": "https://www.sedaily.com/RSS/Stock"},
    {"name": "헤럴드경제",       "url": "https://biz.heraldcorp.com/rss.php?ct=010000000000"},
    {"name": "파이낸셜뉴스",     "url": "https://www.fnnews.com/rss/fn_realnews.xml"},
    {"name": "아시아경제",       "url": "https://www.asiae.co.kr/rss/all.htm"},
    {"name": "이투데이",        "url": "https://www.etoday.co.kr/rss/rss.xml"},
    {"name": "조선비즈",        "url": "https://biz.chosun.com/arc/outboundfeeds/rss/"},
    {"name": "뉴스1-증권",      "url": "https://www.news1.kr/rss/Stock.xml"},
    {"name": "전자신문",        "url": "https://www.etnews.com/rss.xml"},
    {"name": "비즈니스포스트",   "url": "https://www.businesspost.co.kr/BP?command=rss&mode=0"},
    {"name": "데일리안-경제",    "url": "https://dailian.co.kr/rss/rss_economy.xml"},
    {"name": "글로벌이코노믹",   "url": "https://www.g-enews.com/rss/rss.xml"},
    {"name": "뉴스핌",          "url": "https://www.newspim.com/rss/finance.xml"},
    {"name": "인포스탁데일리",   "url": "https://www.infostockdaily.co.kr/rss/allArticle.xml"},
    {"name": "더벨",            "url": "https://www.thebell.co.kr/rss/rss.xml"},
]

# ────────────────────────────────────────
# 2. 커뮤니티 소스 5개
# ────────────────────────────────────────
COMMUNITY_SOURCES = [
    {
        "name": "디시_주식갤",
        "url": "https://gall.dcinside.com/board/lists/?id=stock_new1",
        "type": "dcinside"
    },
    {
        "name": "디시_코스닥갤",
        "url": "https://gall.dcinside.com/board/lists/?id=kosdaq",
        "type": "dcinside"
    },
    {
        "name": "클리앙_투자",
        "url": "https://www.clien.net/service/board/invest",
        "type": "clien"
    },
    {
        "name": "네이버_시황토론",
        "url": "https://finance.naver.com/board/list.naver?boardType=A001&code=market",
        "type": "naver_talk"
    },
    {
        "name": "종목토론_코스닥",
        "url": "https://finance.naver.com/discussion/listBoard.naver?typeId=20",
        "type": "naver_talk"
    },
]

# ────────────────────────────────────────
# 3. 종목명 태깅용 키워드 사전
# ────────────────────────────────────────
THEME_KEYWORDS = {
    "방산":     ["한화에어로스페이스", "한화시스템", "LIG넥스원", "현대로템", "빅텍", "퍼스텍", "방위산업", "K방산"],
    "원전":     ["두산에너빌리티", "한전기술", "한전KPS", "비에이치아이", "원자력", "SMR", "핵융합"],
    "로봇":     ["레인보우로보틱스", "로보티즈", "에스피지", "로봇", "협동로봇", "자율주행로봇"],
    "AI반도체": ["SK하이닉스", "삼성전자", "리노공업", "HBM", "엔비디아", "AI반도체", "NPU"],
    "2차전지":  ["에코프로", "POSCO홀딩스", "엘앤에프", "배터리", "양극재", "음극재", "전고체"],
    "바이오":   ["삼성바이오로직스", "셀트리온", "알테오젠", "ADC", "mRNA", "신약", "임상"],
    "조선":     ["HD현대중공업", "삼성중공업", "한화오션", "LNG선", "컨테이너선", "수주잔고"],
    "건설":     ["GS건설", "현대건설", "DL이앤씨", "재건축", "재개발", "PF", "리츠"],
    "우주항공": ["한화에어로스페이스", "KAI", "이노스페이스", "누리호", "위성", "우주"],
    "수소":     ["두산퓨얼셀", "효성중공업", "수소", "수소차", "연료전지", "그린수소"],
    "남북경협": ["개성공단", "남북", "경협", "통일", "비핵화"],
    "가상화폐": ["비트코인", "이더리움", "코인", "블록체인", "업비트", "빗썸"],
    "게임":     ["크래프톤", "엔씨소프트", "넷마블", "카카오게임즈", "게임", "메타버스"],
    "엔터":     ["하이브", "SM", "JYP", "YG", "K팝", "아이돌", "콘텐츠"],
    "의료기기": ["인바디", "오스코텍", "레이", "의료기기", "디지털헬스", "의료AI"],
    "보안":     ["안랩", "이글루", "사이버보안", "정보보안", "해킹"],
    "자동차":   ["현대차", "기아", "현대모비스", "전기차", "자율주행", "전장"],
    "반도체장비":["유진테크", "원익IPS", "피에스케이", "반도체장비", "식각", "증착"],
    "트럼프테마": ["트럼프", "관세", "미국우선", "IRA", "인프라"],
    "이재명테마": ["이재명", "민주당", "대선"],
}


# ────────────────────────────────────────
# 4. RSS 수집 함수
# ────────────────────────────────────────
def fetch_rss(source: dict) -> list:
    """단일 RSS 소스에서 기사 수집"""
    articles = []
    try:
        headers = {"User-Agent": "Mozilla/5.0 (compatible; AndyJoStockAI/1.0)"}
        resp = requests.get(source["url"], headers=headers, timeout=10)
        resp.encoding = resp.apparent_encoding

        feed = feedparser.parse(resp.text)
        for entry in feed.entries[:30]:  # 소스당 최대 30건
            title = entry.get("title", "").strip()
            link  = entry.get("link", "").strip()
            pub   = entry.get("published", datetime.now().isoformat())
            if title and link:
                articles.append({
                    "title":  title,
                    "url":    link,
                    "source": source["name"],
                    "type":   "news",
                    "published": pub,
                    "id": hashlib.md5(link.encode()).hexdigest()
                })
    except Exception as e:
        log.debug(f"RSS 수집 실패 ({source['name']}): {e}")
    return articles


# ────────────────────────────────────────
# 5. 커뮤니티 크롤링 함수
# ────────────────────────────────────────
def fetch_dcinside(source: dict) -> list:
    """디시인사이드 갤러리 글 목록 크롤링"""
    articles = []
    try:
        headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
            "Referer": "https://gall.dcinside.com/"
        }
        resp = requests.get(source["url"], headers=headers, timeout=10)
        soup = BeautifulSoup(resp.text, "html.parser")

        rows = soup.select("tr.ub-content")
        for row in rows[:20]:
            title_tag = row.select_one("td.gall_tit a")
            if not title_tag:
                continue
            title = title_tag.get_text(strip=True)
            link  = "https://gall.dcinside.com" + title_tag.get("href", "")

            # 추천수·조회수 추출
            recommend = row.select_one("td.gall_recommend")
            view      = row.select_one("td.gall_count")
            rec_val   = int(recommend.get_text(strip=True) or 0) if recommend else 0
            view_val  = int(view.get_text(strip=True).replace(",", "") or 0) if view else 0

            if title and link and rec_val >= 0:
                articles.append({
                    "title":     title,
                    "url":       link,
                    "source":    source["name"],
                    "type":      "community",
                    "recommend": rec_val,
                    "view":      view_val,
                    "published": datetime.now().isoformat(),
                    "id": hashlib.md5(link.encode()).hexdigest()
                })
    except Exception as e:
        log.debug(f"디시 크롤링 실패 ({source['name']}): {e}")
    return articles


def fetch_clien(source: dict) -> list:
    """클리앙 투자게시판 크롤링"""
    articles = []
    try:
        headers = {"User-Agent": "Mozilla/5.0 (compatible; AndyJoStockAI/1.0)"}
        resp = requests.get(source["url"], headers=headers, timeout=10)
        soup = BeautifulSoup(resp.text, "html.parser")

        posts = soup.select("div.list_item")
        for post in posts[:20]:
            title_tag = post.select_one("span.subject_fixed")
            link_tag  = post.select_one("a.list_subject")
            if not title_tag or not link_tag:
                continue
            title = title_tag.get_text(strip=True)
            link  = "https://www.clien.net" + link_tag.get("href", "")

            like_tag = post.select_one("span.symph_count")
            like_val = int(like_tag.get_text(strip=True) or 0) if like_tag else 0

            articles.append({
                "title":     title,
                "url":       link,
                "source":    source["name"],
                "type":      "community",
                "recommend": like_val,
                "view":      0,
                "published": datetime.now().isoformat(),
                "id": hashlib.md5(link.encode()).hexdigest()
            })
    except Exception as e:
        log.debug(f"클리앙 크롤링 실패 ({source['name']}): {e}")
    return articles


def fetch_naver_talk(source: dict) -> list:
    """네이버 금융 토론방 크롤링"""
    articles = []
    try:
        headers = {
            "User-Agent": "Mozilla/5.0 (compatible; AndyJoStockAI/1.0)",
            "Referer": "https://finance.naver.com/"
        }
        resp = requests.get(source["url"], headers=headers, timeout=10)
        soup = BeautifulSoup(resp.text, "html.parser")

        rows = soup.select("table.type2 tr")
        for row in rows[:20]:
            title_tag = row.select_one("td.title a")
            if not title_tag:
                continue
            title = title_tag.get_text(strip=True)
            link  = "https://finance.naver.com" + title_tag.get("href", "")

            articles.append({
                "title":     title,
                "url":       link,
                "source":    source["name"],
                "type":      "community",
                "recommend": 0,
                "view":      0,
                "published": datetime.now().isoformat(),
                "id": hashlib.md5(link.encode()).hexdigest()
            })
    except Exception as e:
        log.debug(f"네이버 토론방 크롤링 실패 ({source['name']}): {e}")
    return articles


# ────────────────────────────────────────
# 6. 커뮤니티 신호 점수 계산
# ────────────────────────────────────────
def calc_community_signal(articles: list) -> dict:
    """
    커뮤니티 글에서 테마별 신호 점수 계산
    공식: 언급빈도 × 0.5 + 추천수비율 × 0.3 + 조회수급증 × 0.2
    반환: {테마명: 신호점수}
    """
    theme_count = {theme: 0 for theme in THEME_KEYWORDS}
    theme_rec   = {theme: 0 for theme in THEME_KEYWORDS}

    for article in articles:
        if article.get("type") != "community":
            continue
        title = article.get("title", "")
        rec   = article.get("recommend", 0)

        for theme, keywords in THEME_KEYWORDS.items():
            for kw in keywords:
                if kw in title:
                    theme_count[theme] += 1
                    theme_rec[theme]   += rec
                    break

    total_articles = max(len(articles), 1)
    signals = {}
    for theme in THEME_KEYWORDS:
        freq_score = min(theme_count[theme] / total_articles * 100, 10)
        rec_score  = min(theme_rec[theme] / 100, 10)
        signals[theme] = round(freq_score * 0.5 + rec_score * 0.3, 2)

    return signals


# ────────────────────────────────────────
# 7. 종목명 태깅
# ────────────────────────────────────────
def tag_stocks(articles: list) -> list:
    """기사 제목에서 테마·종목명 태깅"""
    for article in articles:
        title  = article.get("title", "")
        tagged = []
        for theme, keywords in THEME_KEYWORDS.items():
            for kw in keywords:
                if kw in title:
                    tagged.append(theme)
                    break
        article["themes"] = list(set(tagged))
    return articles


# ────────────────────────────────────────
# 8. 중복 제거
# ────────────────────────────────────────
def deduplicate(articles: list) -> list:
    """URL 기반 중복 제거"""
    seen = set()
    result = []
    for a in articles:
        aid = a.get("id", "")
        if aid and aid not in seen:
            seen.add(aid)
            result.append(a)
    return result


# ────────────────────────────────────────
# 9. 전체 수집 메인 함수
# ────────────────────────────────────────
def fetch_all_news() -> list:
    """
    언론 RSS 25개 + 커뮤니티 5개 전체 수집
    반환: 태깅된 기사 리스트
    """
    all_articles = []

    # RSS 수집
    log.info(f"RSS 수집 시작: {len(RSS_SOURCES)}개 소스")
    for source in RSS_SOURCES:
        articles = fetch_rss(source)
        all_articles.extend(articles)
        log.debug(f"  {source['name']}: {len(articles)}건")
        time.sleep(0.5)

    # 커뮤니티 수집
    log.info(f"커뮤니티 수집 시작: {len(COMMUNITY_SOURCES)}개 소스")
    for source in COMMUNITY_SOURCES:
        if source["type"] == "dcinside":
            articles = fetch_dcinside(source)
        elif source["type"] == "clien":
            articles = fetch_clien(source)
        else:
            articles = fetch_naver_talk(source)

        all_articles.extend(articles)
        log.debug(f"  {source['name']}: {len(articles)}건")
        time.sleep(1.0)

    # 중복 제거 + 태깅
    unique   = deduplicate(all_articles)
    tagged   = tag_stocks(unique)

    # 커뮤니티 신호 계산
    community_signals = calc_community_signal(tagged)

    log.info(f"뉴스 수집 완료: 총 {len(tagged)}건 (중복 제거 후)")
    log.info(f"커뮤니티 신호 상위 3개: {sorted(community_signals.items(), key=lambda x: -x[1])[:3]}")

    # community_signals를 tagged 리스트에 메타로 첨부
    for article in tagged:
        article["community_signals"] = community_signals

    return tagged


# ────────────────────────────────────────
# 10. 뉴스 데이터 로드 (data_store 연동)
# ────────────────────────────────────────
def load_news_data() -> list:
    try:
        from data_store import load_news_data as _load
        return _load()
    except Exception:
        return []
