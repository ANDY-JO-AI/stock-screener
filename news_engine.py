# news_engine.py
import requests
import xml.etree.ElementTree as ET
import datetime
import re
from data_store import save_news_data

RSS_SOURCES = [
    # 한국경제
    {"url": "https://www.hankyung.com/feed/all-news",           "source": "한국경제"},
    {"url": "https://www.hankyung.com/feed/finance",            "source": "한국경제-증권"},
    # 매일경제
    {"url": "https://www.mk.co.kr/rss/40300001/",               "source": "매일경제"},
    {"url": "https://www.mk.co.kr/rss/30100041/",               "source": "매일경제-증권"},
    # 연합뉴스
    {"url": "https://www.yna.co.kr/economy/stock/1/rss.xml",    "source": "연합뉴스"},
    # 머니투데이
    {"url": "https://www.mt.co.kr/rss/mainnews.xml",            "source": "머니투데이"},
    # 이데일리
    {"url": "https://www.edaily.co.kr/rss/edailyrss.xml",       "source": "이데일리"},
    # 뉴시스
    {"url": "https://newsis.com/rss/finance.rss",               "source": "뉴시스"},
    # 네이버금융 뉴스
    {"url": "https://finance.naver.com/news/rss.naver?mode=LSS2D&section_id=101&section_id2=258",
                                                                 "source": "네이버금융-증권"},
    {"url": "https://finance.naver.com/news/rss.naver?mode=LSS2D&section_id=101&section_id2=259",
                                                                 "source": "네이버금융-시황"},
]

HEADERS = {"User-Agent": "Mozilla/5.0 (compatible; StockBot/1.0)"}


def fetch_rss(url: str, source: str) -> list:
    """RSS 피드 파싱 — 인코딩 오류 방어 처리 포함"""
    items = []
    try:
        r = requests.get(url, headers=HEADERS, timeout=10)
        # XML 파싱 전 불량 문자 제거
        content = r.content
        # BOM 제거
        if content.startswith(b'\xef\xbb\xbf'):
            content = content[3:]
        # 인코딩 선언 강제 UTF-8로 교체
        content = content.replace(b'encoding="euc-kr"', b'encoding="utf-8"')
        content = content.replace(b'encoding="EUC-KR"', b'encoding="utf-8"')
        # 제어문자 제거 (XML 파싱 오류 원인)
        import re as _re
        content_str = content.decode("utf-8", errors="replace")
        content_str = _re.sub(r'[\x00-\x08\x0b\x0c\x0e-\x1f]', '', content_str)
        content = content_str.encode("utf-8")

        root = ET.fromstring(content)
        for item in root.findall(".//item")[:20]:
            title   = item.findtext("title",       "").strip()
            link    = item.findtext("link",         "").strip()
            pubdate = item.findtext("pubDate",      "").strip()
            desc    = item.findtext("description",  "").strip()
            desc    = re.sub(r"<[^>]+>", "", desc)[:200]
            if title:
                items.append({
                    "title":   title,
                    "url":     link,
                    "pub":     pubdate,
                    "desc":    desc,
                    "source":  source,
                    "fetched": datetime.datetime.now().strftime("%Y-%m-%d %H:%M"),
                })
    except Exception as e:
        print(f"[NEWS] RSS 오류 {source}: {e}")
    return items


def tag_stocks(news_list: list, stock_names: list) -> list:
    """뉴스 제목에 종목명 태깅"""
    for news in news_list:
        tagged = []
        for name in stock_names:
            if len(name) >= 2 and name in news["title"]:
                tagged.append(name)
        news["tagged_stocks"] = tagged
    return news_list


def deduplicate(news_list: list) -> list:
    """제목 기준 중복 제거"""
    seen = set()
    result = []
    for n in news_list:
        key = n["title"][:30]
        if key not in seen:
            seen.add(key)
            result.append(n)
    return result


def fetch_all_news(stock_names: list = None) -> list:
    """전체 뉴스 수집 + 태깅 + 저장"""
    all_news = []
    for src in RSS_SOURCES:
        items = fetch_rss(src["url"], src["source"])
        all_news.extend(items)
        print(f"[NEWS] {src['source']}: {len(items)}건")

    all_news = deduplicate(all_news)
    print(f"[NEWS] 중복제거 후 총 {len(all_news)}건")

    if stock_names:
        all_news = tag_stocks(all_news, stock_names)

    save_news_data(all_news)
    return all_news


def load_news_data() -> list:
    """저장된 뉴스 데이터 로드 (filter_engine에서 호출)"""
    from data_store import load_news_data as _load
    return _load()
if __name__ == "__main__":
    news = fetch_all_news()
    print(f"총 {len(news)}건 수집")
    for n in news[:5]:
        print(f"[{n['source']}] {n['title']}")
