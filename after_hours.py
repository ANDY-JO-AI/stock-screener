# after_hours.py — 시간외 특징주 수집 + 텔레그램 발송
import os, requests, json, datetime
import pandas as pd
from data_store import get_gsheet

TELEGRAM_TOKEN   = os.environ.get("TELEGRAM_TOKEN", "")
TELEGRAM_CHAT_ID = os.environ.get("TELEGRAM_CHAT_ID", "")
DART_KEY         = os.environ.get("DART_API_KEY", "")

def fetch_dart_after_hours():
    """오늘 DART 공시 (장 마감 후)"""
    today = datetime.date.today().strftime("%Y%m%d")
    url = "https://opendart.fss.or.kr/api/list.json"
    params = {
        "crtfc_key": DART_KEY,
        "bgn_de": today, "end_de": today,
        "pblntf_ty": "A",
        "page_no": 1, "page_count": 40,
    }
    try:
        r = requests.get(url, params=params, timeout=10)
        data = r.json()
        if data.get("status") == "000":
            return data.get("list", [])
    except Exception as e:
        print(f"[DART] error: {e}")
    return []

def fetch_naver_finance_news():
    """네이버 금융 RSS"""
    import xml.etree.ElementTree as ET
    feeds = [
        "https://finance.naver.com/news/rss.naver?mode=LSS2D&section_id=101&section_id2=258",
        "https://finance.naver.com/news/rss.naver?mode=LSS2D&section_id=101&section_id2=259",
    ]
    items = []
    for url in feeds:
        try:
            r = requests.get(url, timeout=8, headers={"User-Agent":"Mozilla/5.0"})
            root = ET.fromstring(r.content)
            for item in root.findall(".//item")[:10]:
                title = item.findtext("title","").strip()
                link  = item.findtext("link","").strip()
                pub   = item.findtext("pubDate","").strip()
                items.append({"title": title, "link": link, "pub": pub, "source": "네이버금융"})
        except:
            pass
    return items

def fetch_krx_after_hours():
    """KRX 시간외 거래 특이 종목 (등락률 기준)"""
    url = "http://data.krx.co.kr/comm/bldAttendant/getJsonData.cmd"
    params = {
        "bld": "dbms/MDC/STAT/standard/MDCSTAT01501",
        "mktId": "ALL",
        "trdDd": datetime.date.today().strftime("%Y%m%d"),
        "share": "1",
        "money": "1",
        "csvxls_isNo": "false",
    }
    try:
        r = requests.post(url, data=params, timeout=10,
                          headers={"User-Agent":"Mozilla/5.0","Referer":"http://data.krx.co.kr"})
        data = r.json()
        stocks = data.get("OutBlock_1", [])
        df = pd.DataFrame(stocks)
        if df.empty:
            return []
        df["FLUC_RT"] = pd.to_numeric(df.get("FLUC_RT", 0), errors="coerce").fillna(0)
        df = df[df["FLUC_RT"].abs() >= 5].sort_values("FLUC_RT", ascending=False)
        return df[["ISU_NM","FLUC_RT","ACC_TRDVOL","ACC_TRDVAL"]].head(20).to_dict("records")
    except Exception as e:
        print(f"[KRX] error: {e}")
        return []

def send_telegram(message):
    if not TELEGRAM_TOKEN or not TELEGRAM_CHAT_ID:
        print("[TG] 토큰 미설정 — 텔레그램 발송 생략")
        return
    url = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage"
    payload = {"chat_id": TELEGRAM_CHAT_ID, "text": message, "parse_mode": "HTML"}
    try:
        r = requests.post(url, json=payload, timeout=10)
        print(f"[TG] 발송: {r.status_code}")
    except Exception as e:
        print(f"[TG] 오류: {e}")

def save_after_hours(data):
    try:
        gc = get_gsheet()
        sh = gc.open_by_key("1OqRboKwx7X0-3W67_raZyV2ZjcQ_G7ylHilf7SgAU88")
        try:
            ws = sh.worksheet("시간외특징주")
        except:
            ws = sh.add_worksheet("시간외특징주", rows=100, cols=10)
        ws.clear()
        ws.update("A1", [["업데이트시간", datetime.datetime.now().strftime("%Y-%m-%d %H:%M")]])
        ws.update("A2", [["종류", "종목명/제목", "등락률/상세", "링크"]])
        rows = []
        for item in data:
            rows.append([item.get("type",""), item.get("name",""), item.get("detail",""), item.get("link","")])
        if rows:
            ws.update(f"A3", rows)
        print(f"[AH] 시간외특징주 {len(rows)}건 저장")
    except Exception as e:
        print(f"[AH] 저장 오류: {e}")

def run_after_hours():
    now = datetime.datetime.now()
    print(f"[AH] 시간외 특징주 수집 시작: {now.strftime('%H:%M')}")
    
    all_data = []
    msg_lines = [f"<b>🔔 ANDY JO STOCK AI — 시간외 특징주</b>\n{now.strftime('%Y-%m-%d %H:%M')} 기준\n"]
    
    # 1) KRX 등락률 상위
    krx = fetch_krx_after_hours()
    msg_lines.append("📊 <b>시간외 등락률 상위</b>")
    for s in krx[:10]:
        name = s.get("ISU_NM","")
        rate = s.get("FLUC_RT",0)
        sign = "+" if float(rate) > 0 else ""
        msg_lines.append(f"  {name} {sign}{rate}%")
        all_data.append({"type":"등락률","name":name,"detail":f"{sign}{rate}%","link":""})
    
    # 2) DART 공시
    dart = fetch_dart_after_hours()
    msg_lines.append("\n📋 <b>오늘의 DART 공시 (주요)</b>")
    important_types = ["유상증자","무상증자","합병","분할","전환사채","자기주식","영업양수도","최대주주"]
    filtered_dart = [d for d in dart if any(kw in d.get("report_nm","") for kw in important_types)]
    for d in filtered_dart[:8]:
        corp = d.get("corp_name","")
        rpt  = d.get("report_nm","")
        msg_lines.append(f"  [{corp}] {rpt}")
        all_data.append({"type":"공시","name":corp,"detail":rpt,
                         "link":f"https://dart.fss.or.kr/dsaf001/main.do?rcpNo={d.get('rcept_no','')}"})
    
    # 3) 네이버 금융 뉴스
    naver_news = fetch_naver_finance_news()
    msg_lines.append("\n📰 <b>네이버 금융 주요 뉴스</b>")
    for n in naver_news[:5]:
        msg_lines.append(f"  · {n['title']}")
        all_data.append({"type":"뉴스","name":n["title"],"detail":n.get("pub",""),"link":n["link"]})
    
    save_after_hours(all_data)
    
    msg = "\n".join(msg_lines)
    send_telegram(msg)
    print("[AH] 완료")

if __name__ == "__main__":
    run_after_hours()
