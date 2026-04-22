import requests
from bs4 import BeautifulSoup
import pandas as pd
import json, time, os
from datetime import datetime, timedelta
from concurrent.futures import ThreadPoolExecutor, as_completed

try:
    import FinanceDataReader as fdr
except:
    fdr = None

from data_store import save_market_report

TODAY = datetime.today()
TODAY_STR = TODAY.strftime('%Y-%m-%d')
HEADERS = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"}

# ──────────────────────────────────────────
# 1. KRX 상한가/급등주 수집
# ──────────────────────────────────────────
def fetch_krx_top_movers():
    result = {"상한가": [], "급등주": []}
    try:
        if fdr is None:
            return result
        for mkt in ["KOSPI", "KOSDAQ"]:
            df = fdr.StockListing(mkt)
            if df is None or df.empty:
                continue
            df.columns = [c.strip() for c in df.columns]
            chg_col = next((c for c in df.columns if "등락" in c or "Change" in c or "change" in c), None)
            name_col = next((c for c in df.columns if "종목명" in c or "Name" in c), None)
            code_col = next((c for c in df.columns if "코드" in c or "Code" in c or "Symbol" in c), None)
            if not all([chg_col, name_col, code_col]):
                continue
            df["_chg"] = pd.to_numeric(df[chg_col], errors="coerce").fillna(0)
            df["_name"] = df[name_col].astype(str)
            df["_code"] = df[code_col].astype(str).str.zfill(6)
            df["_mkt"] = mkt
            upper = df[df["_chg"] >= 29.0]
            for _, r in upper.iterrows():
                result["상한가"].append({
                    "종목명": r["_name"], "코드": r["_code"],
                    "등락률": r["_chg"], "시장": mkt
                })
            top = df[df["_chg"] >= 5.0].nlargest(20, "_chg")
            for _, r in top.iterrows():
                result["급등주"].append({
                    "종목명": r["_name"], "코드": r["_code"],
                    "등락률": r["_chg"], "시장": mkt
                })
    except Exception as e:
        print(f"[KRX 수집 오류] {e}")
    return result

# ──────────────────────────────────────────
# 2. 네이버 금융 뉴스 크롤링
# ──────────────────────────────────────────
def fetch_naver_news(stock_code: str, stock_name: str) -> list:
    news_list = []
    try:
        url = f"https://finance.naver.com/item/news_news.nhn?code={stock_code}&page=1"
        r = requests.get(url, headers=HEADERS, timeout=10)
        soup = BeautifulSoup(r.text, "lxml")
        rows = soup.select("table.type5 tr")
        for row in rows[:3]:
            title_tag = row.select_one("td.title a")
            if title_tag:
                news_list.append(title_tag.get_text(strip=True))
    except Exception as e:
        print(f"[네이버 뉴스 {stock_name}] {e}")
    return news_list

# ──────────────────────────────────────────
# 3. 매일경제 증권 시황 크롤링
# ──────────────────────────────────────────
def fetch_mk_market():
    result = []
    try:
        url = "https://stock.mk.co.kr/news/marketNews"
        r = requests.get(url, headers=HEADERS, timeout=10)
        soup = BeautifulSoup(r.text, "lxml")
        articles = soup.select("ul.news_list li")[:5]
        for a in articles:
            title = a.select_one("a")
            if title:
                result.append(title.get_text(strip=True))
    except Exception as e:
        print(f"[매일경제 크롤링 오류] {e}")
    return result

# ──────────────────────────────────────────
# 4. 한국경제 증권 크롤링
# ──────────────────────────────────────────
def fetch_hankyung_market():
    result = []
    try:
        url = "https://www.hankyung.com/finance/stock-market"
        r = requests.get(url, headers=HEADERS, timeout=10)
        soup = BeautifulSoup(r.text, "lxml")
        articles = soup.select("ul.news-list li")[:5]
        for a in articles:
            title = a.select_one("a")
            if title:
                result.append(title.get_text(strip=True))
    except Exception as e:
        print(f"[한국경제 크롤링 오류] {e}")
    return result

# ──────────────────────────────────────────
# 5. 연합뉴스 경제 크롤링
# ──────────────────────────────────────────
def fetch_yonhap_market():
    result = []
    try:
        url = "https://www.yna.co.kr/economy/stock"
        r = requests.get(url, headers=HEADERS, timeout=10)
        soup = BeautifulSoup(r.text, "lxml")
        articles = soup.select("div.item-box01 strong.tit-news")[:5]
        for a in articles:
            result.append(a.get_text(strip=True))
    except Exception as e:
        print(f"[연합뉴스 크롤링 오류] {e}")
    return result

# ──────────────────────────────────────────
# 6. DART 당일 공시 수집
# ──────────────────────────────────────────
def fetch_dart_today():
    DART_API_KEY = os.environ.get("DART_API_KEY", "7d2191837b9373fc6f049fd6fa30d7678f2f96f6")
    result = {"호재": [], "악재": [], "중립": []}
    try:
        url = "https://opendart.fss.or.kr/api/list.json"
        params = {
            "crtfc_key": DART_API_KEY,
            "bgn_de": TODAY.strftime('%Y%m%d'),
            "end_de": TODAY.strftime('%Y%m%d'),
            "page_count": 100
        }
        r = requests.get(url, params=params, timeout=15)
        data = r.json()
        if data.get("status") == "000":
            for item in data.get("list", []):
                title = item.get("report_nm", "")
                corp = item.get("corp_name", "")
                entry = {"종목명": corp, "공시": title}
                bad_kw = ["전환사채", "신주인수권", "유상증자", "전환가액 조정", "사모"]
                good_kw = ["계약", "수주", "MOU", "공급", "납품", "자사주", "배당"]
                if any(k in title for k in bad_kw):
                    result["악재"].append(entry)
                elif any(k in title for k in good_kw):
                    result["호재"].append(entry)
                else:
                    result["중립"].append(entry)
    except Exception as e:
        print(f"[DART 당일 공시 오류] {e}")
    return result

# ──────────────────────────────────────────
# 7. 시장 지수 수집
# ──────────────────────────────────────────
def fetch_index_summary():
    result = {}
    try:
        if fdr is None:
            return result
        for name, code in [("KOSPI", "KS11"), ("KOSDAQ", "KQ11")]:
            df = fdr.DataReader(code,
                (TODAY - timedelta(days=5)).strftime('%Y-%m-%d'),
                TODAY.strftime('%Y-%m-%d'))
            if df is not None and len(df) >= 2:
                close = df["Close"].iloc[-1]
                prev  = df["Close"].iloc[-2]
                chg   = (close - prev) / prev * 100
                result[name] = {
                    "지수": round(close, 2),
                    "등락률": round(chg, 2),
                    "전일": round(prev, 2)
                }
    except Exception as e:
        print(f"[지수 수집 오류] {e}")
    return result

# ──────────────────────────────────────────
# 8. 테마 자동 분류
# ──────────────────────────────────────────
THEME_KEYWORDS = {
    "🏛️정치/선거": ["선거", "대선", "총선", "정권", "국회", "여당", "야당"],
    "🛡️방산/안보": ["방산", "방위", "무기", "군", "안보", "국방", "미사일", "나토", "전쟁"],
    "🤖AI/로봇": ["인공지능", "AI", "로봇", "자동화", "옵티머스", "휴머노이드"],
    "⚡에너지/전력": ["에너지", "전력", "태양광", "배터리", "ESS", "원전", "유가", "석유"],
    "💊바이오/헬스": ["바이오", "신약", "임상", "의료", "제약", "치료제", "백신"],
    "🚗자동차/전기차": ["전기차", "자동차", "2차전지", "배터리", "EV"],
    "📡IT/통신": ["IT", "통신", "5G", "6G", "반도체", "클라우드", "데이터센터"],
    "🏗️건설/부동산": ["건설", "부동산", "재건축", "인프라", "토목"],
    "💰금융/핀테크": ["금융", "은행", "핀테크", "코인", "블록체인"],
    "🌾에너지/화학": ["화학", "소재", "철강", "비철금속", "LPG", "가스관", "강관"],
}

def classify_themes(news_texts: list) -> list:
    found = []
    combined = " ".join(news_texts)
    for theme, kws in THEME_KEYWORDS.items():
        if any(kw in combined for kw in kws):
            found.append(theme)
    return found

# ──────────────────────────────────────────
# 9. 시간외 특징주 수집
# ──────────────────────────────────────────
def fetch_after_market():
    result = {"상승": [], "하락": []}
    try:
        url = "https://finance.naver.com/sise/sise_quant.nhn?sosok=1"
        r = requests.get(url, headers=HEADERS, timeout=10)
        soup = BeautifulSoup(r.text, "lxml")
        rows = soup.select("table.type_2 tr")
        for row in rows[:10]:
            cols = row.select("td")
            if len(cols) >= 4:
                name = cols[1].get_text(strip=True)
                chg_text = cols[4].get_text(strip=True).replace(",", "").replace("%", "")
                try:
                    chg = float(chg_text)
                    if chg > 0:
                        result["상승"].append({"종목명": name, "등락률": chg})
                    elif chg < 0:
                        result["하락"].append({"종목명": name, "등락률": chg})
                except:
                    pass
    except Exception as e:
        print(f"[시간외 수집 오류] {e}")
    return result

# ──────────────────────────────────────────
# 메인 리포트 생성
# ──────────────────────────────────────────
def run_market_report():
    print("=== 시황 리포트 생성 시작 ===")
    report = {}

    with ThreadPoolExecutor(max_workers=6) as ex:
        f_krx    = ex.submit(fetch_krx_top_movers)
        f_index  = ex.submit(fetch_index_summary)
        f_dart   = ex.submit(fetch_dart_today)
        f_mk     = ex.submit(fetch_mk_market)
        f_hk     = ex.submit(fetch_hankyung_market)
        f_yh     = ex.submit(fetch_yonhap_market)

    krx_data    = f_krx.result()
    index_data  = f_index.result()
    dart_data   = f_dart.result()
    mk_news     = f_mk.result()
    hk_news     = f_hk.result()
    yh_news     = f_yh.result()

    all_news = mk_news + hk_news + yh_news
    themes_today = classify_themes(all_news)

    # 급등주별 뉴스 추가 (상위 10개만)
    top_stocks = krx_data.get("급등주", [])[:10]
    for stock in top_stocks:
        news = fetch_naver_news(stock["코드"], stock["종목명"])
        stock["뉴스"] = news
        stock["테마"] = classify_themes(news + [stock["종목명"]])
        time.sleep(0.2)

    report["분석일"]       = TODAY_STR
    report["KOSPI지수"]    = json.dumps(index_data.get("KOSPI", {}), ensure_ascii=False)
    report["KOSDAQ지수"]   = json.dumps(index_data.get("KOSDAQ", {}), ensure_ascii=False)
    report["오늘테마"]     = json.dumps(themes_today, ensure_ascii=False)
    report["상한가"]       = json.dumps(krx_data.get("상한가", []), ensure_ascii=False)
    report["급등주"]       = json.dumps(top_stocks, ensure_ascii=False)
    report["호재공시"]     = json.dumps(dart_data.get("호재", [])[:10], ensure_ascii=False)
    report["악재공시"]     = json.dumps(dart_data.get("악재", [])[:10], ensure_ascii=False)
    report["시황뉴스"]     = json.dumps(all_news[:10], ensure_ascii=False)

    after = fetch_after_market()
    report["시간외상승"]   = json.dumps(after.get("상승", [])[:10], ensure_ascii=False)
    report["시간외하락"]   = json.dumps(after.get("하락", [])[:10], ensure_ascii=False)

    save_market_report(report)
    print(f"=== 시황 리포트 완료 | 테마 {len(themes_today)}개 | 급등주 {len(top_stocks)}개 ===")
    return report

if __name__ == "__main__":
    run_market_report()
