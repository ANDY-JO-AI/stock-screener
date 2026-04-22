import os, json, requests
from datetime import datetime
from bs4 import BeautifulSoup
import gspread
from google.oauth2.service_account import Credentials

SHEETS_ID = "1OqRboKwx7X0-3W67_raZyV2ZjcQ_G7ylHilf7SgAU88"
SCOPES = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]

THEMES = {
    "방산":    ["방산","무기","전투기","미사일","K-방산","방위산업"],
    "로봇/AI": ["로봇","AI","인공지능","자율주행","옵티머스"],
    "2차전지": ["배터리","2차전지","전기차","양극재","음극재","리튬"],
    "반도체":  ["반도체","HBM","파운드리","메모리","DRAM"],
    "바이오":  ["바이오","신약","임상","FDA","항암"],
    "에너지":  ["원유","가스","LPG","석유","에너지","태양광","원전"],
    "건설":    ["건설","부동산","아파트","재건축"],
    "조선":    ["조선","LNG선","해운","컨테이너"],
    "게임":    ["게임","메타버스","NFT","모바일게임"],
    "화장품":  ["화장품","K뷰티","코스메틱","뷰티"],
}

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/124.0.0.0 Safari/537.36"
    )
}


def get_gsheet():
    cred_json = os.environ.get("GOOGLE_CREDENTIALS", "")
    if not cred_json:
        return None
    try:
        creds = Credentials.from_service_account_info(
            json.loads(cred_json), scopes=SCOPES
        )
        gc = gspread.authorize(creds)
        return gc.open_by_key(SHEETS_ID)
    except Exception as e:
        print(f"[Sheets 오류] {e}")
        return None


def fetch_rss(url, source):
    items = []
    try:
        r = requests.get(url, headers=HEADERS, timeout=10)
        soup = BeautifulSoup(r.content, "xml")
        for item in soup.find_all("item")[:15]:
            title = item.find("title")
            link  = item.find("link")
            if title and link:
                items.append({
                    "title":  title.text.strip(),
                    "url":    link.text.strip(),
                    "source": source
                })
    except Exception as e:
        print(f"[{source} RSS 오류] {e}")
    return items


def fetch_all_news():
    news = []
    sources = [
        ("https://rss.hankyung.com/economy/stocks.xml",     "한국경제"),
        ("https://rss.hankyung.com/economy/finance.xml",    "한국경제"),
        ("https://www.mk.co.kr/rss/40300001/",              "매일경제"),
        ("https://www.yna.co.kr/rss/economy.xml",           "연합뉴스"),
    ]
    for url, src in sources:
        items = fetch_rss(url, src)
        news.extend(items)
        print(f"  [{src}] {len(items)}건")
    return news


def fetch_krx_upper():
    stocks = []
    try:
        today = datetime.now().strftime("%Y%m%d")
        url   = "https://data.krx.co.kr/comm/bldAttendant/getJsonData.cmd"
        hdrs  = {**HEADERS, "Referer": "https://data.krx.co.kr"}
        payload = {
            "bld": "dbms/MDC/STAT/standard/MDCSTAT01501",
            "locale": "ko_KR", "mktId": "ALL",
            "trdDd": today, "share": "1", "money": "1", "csvxls_isNo": "false",
        }
        r = requests.post(url, data=payload, headers=hdrs, timeout=15)
        for item in r.json().get("OutBlock_1", []):
            try:
                chg = float(str(item.get("FLUC_RT", "0")).replace(",", ""))
                if chg >= 10:
                    stocks.append({
                        "code":        item.get("ISU_SRT_CD", ""),
                        "name":        item.get("ISU_ABBRV", ""),
                        "price":       item.get("CLSPRC", ""),
                        "change_rate": chg,
                        "volume":      item.get("ACC_TRDVOL", ""),
                    })
            except:
                continue
        stocks.sort(key=lambda x: x["change_rate"], reverse=True)
    except Exception as e:
        print(f"[KRX 오류] {e}")
    return stocks[:30]


def fetch_dart_today():
    items = []
    try:
        api_key = os.environ.get("DART_API_KEY", "")
        today   = datetime.now().strftime("%Y%m%d")
        url = (f"https://opendart.fss.or.kr/api/list.json"
               f"?crtfc_key={api_key}&bgn_de={today}&end_de={today}"
               f"&page_count=40&sort=rdt&sort_mth=desc")
        r = requests.get(url, timeout=15)
        for item in r.json().get("list", []):
            items.append({
                "corp":  item.get("corp_name", ""),
                "title": item.get("report_nm", ""),
                "date":  item.get("rcept_dt", ""),
                "url":   f"https://dart.fss.or.kr/dsaf001/main.do?rcpNo={item.get('rcept_no','')}",
            })
    except Exception as e:
        print(f"[DART 오류] {e}")
    return items[:30]


def auto_reason(stock_name, news_list):
    """급등주 이름으로 관련 뉴스 자동 매칭 → 이유 추출"""
    for news in news_list:
        if stock_name in news.get("title", ""):
            title = news["title"]
            # 핵심 이유 추출 (종목명 뒤 내용)
            idx = title.find(stock_name)
            reason = title[idx + len(stock_name):].strip(" ,.-·")
            if reason:
                return reason[:40]
    return "당일 뉴스 매칭 없음"


def classify_themes(news_list):
    theme_counts = {t: 0 for t in THEMES}
    theme_news   = {t: [] for t in THEMES}
    for news in news_list:
        title = news.get("title", "")
        for theme, keywords in THEMES.items():
            for kw in keywords:
                if kw in title:
                    theme_counts[theme] += 1
                    if len(theme_news[theme]) < 3:
                        theme_news[theme].append({
                            "title":  title,
                            "url":    news.get("url", ""),
                            "source": news.get("source", "")
                        })
                    break
    active = {
        t: {"count": c, "news": theme_news[t]}
        for t, c in theme_counts.items() if c > 0
    }
    return dict(sorted(active.items(), key=lambda x: x[1]["count"], reverse=True))


def run_market_report():
    print("=== ANDY JO's STOCK AI 시황 리포트 시작 ===")
    now = datetime.now().strftime("%Y-%m-%d %H:%M")

    print("[1] 뉴스 수집...")
    news = fetch_all_news()
    print(f"    총 {len(news)}건")

    print("[2] KRX 급등·상한가...")
    upper = fetch_krx_upper()
    print(f"    {len(upper)}건")

    # 급등주별 이유 자동 매칭
    for s in upper:
        s["reason"] = auto_reason(s["name"], news)

    print("[3] DART 공시...")
    dart = fetch_dart_today()
    print(f"    {len(dart)}건")

    print("[4] 테마 분류...")
    themes = classify_themes(news)
    print(f"    활성 테마: {list(themes.keys())}")

    report = {
        "generated_at":       now,
        "themes":             themes,
        "upper_limit_stocks": upper,
        "dart_disclosures":   dart,
        "news":               news[:25],
    }

    print("[5] Sheets 저장...")
    try:
        sh = get_gsheet()
        if sh:
            try:
                ws = sh.worksheet("시황리포트")
            except:
                ws = sh.add_worksheet(title="시황리포트", rows=10, cols=2)
            ws.clear()
            ws.update("A1", [["generated_at", now]])
            ws.update("A2", [["report_json", json.dumps(report, ensure_ascii=False)]])
            print("[5] 저장 완료")
    except Exception as e:
        print(f"[5 오류] {e}")

    print("=== 시황 리포트 완료 ===")
    return report


if __name__ == "__main__":
    run_market_report()
