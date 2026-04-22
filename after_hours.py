# after_hours.py
import os, requests, json, datetime
import xml.etree.ElementTree as ET
import pandas as pd
from data_store import save_after_hours, load_candidates

DART_KEY         = os.environ.get("DART_API_KEY", "")
TELEGRAM_TOKEN   = os.environ.get("TELEGRAM_TOKEN", "")
TELEGRAM_CHAT_ID = os.environ.get("TELEGRAM_CHAT_ID", "")
HEADERS          = {"User-Agent": "Mozilla/5.0 (compatible; StockBot/1.0)"}

BAD_KW  = ["유상증자", "전환사채", "신주인수권", "횡령", "배임", "감사의견"]
GOOD_KW = ["자사주취득", "계약체결", "수주", "특허", "임상", "허가", "독점공급"]


def fetch_dart_after_hours() -> list:
    today  = datetime.date.today().strftime("%Y%m%d")
    url    = "https://opendart.fss.or.kr/api/list.json"
    params = {
        "crtfc_key":  DART_KEY,
        "bgn_de":     today,
        "end_de":     today,
        "pblntf_ty":  "A",
        "page_no":    1,
        "page_count": 40,
    }
    try:
        r = requests.get(url, params=params, timeout=10)
        data = r.json()
        if data.get("status") == "000":
            return data.get("list", [])
    except Exception as e:
        print(f"[AH] DART 오류: {e}")
    return []


def fetch_naver_finance_news() -> list:
    feeds = [
        "https://finance.naver.com/news/rss.naver?mode=LSS2D&section_id=101&section_id2=258",
        "https://finance.naver.com/news/rss.naver?mode=LSS2D&section_id=101&section_id2=259",
    ]
    items = []
    for url in feeds:
        try:
            r = requests.get(url, headers=HEADERS, timeout=8)
            root = ET.fromstring(r.content)
            for item in root.findall(".//item")[:15]:
                title = item.findtext("title", "").strip()
                link  = item.findtext("link",  "").strip()
                pub   = item.findtext("pubDate", "").strip()
                if title:
                    items.append({
                        "title": title, "link": link,
                        "pub": pub, "source": "네이버금융"
                    })
        except:
            pass
    return items


def fetch_krx_after_hours() -> list:
    url  = "http://data.krx.co.kr/comm/bldAttendant/getJsonData.cmd"
    body = {
        "bld":         "dbms/MDC/STAT/standard/MDCSTAT01501",
        "mktId":       "ALL",
        "trdDd":       datetime.date.today().strftime("%Y%m%d"),
        "share":       "1",
        "money":       "1",
        "csvxls_isNo": "false",
    }
    try:
        r = requests.get(url, params=body, headers=HEADERS, timeout=10)
        items = r.json().get("OutBlock_1", [])
        df = pd.DataFrame(items)
        if df.empty:
            return []
        df["FLUC_RT"] = pd.to_numeric(df.get("FLUC_RT", 0), errors="coerce").fillna(0)
        df = df[df["FLUC_RT"].abs() >= 5].sort_values("FLUC_RT", ascending=False)
        return df[["ISU_NM", "FLUC_RT", "ACC_TRDVOL"]].head(20).to_dict("records")
    except Exception as e:
        print(f"[AH] KRX 오류: {e}")
        return []


def send_telegram(message: str):
    if not TELEGRAM_TOKEN or not TELEGRAM_CHAT_ID:
        print("[TG] 토큰 미설정 — 생략")
        return
    url     = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage"
    payload = {
        "chat_id":    TELEGRAM_CHAT_ID,
        "text":       message,
        "parse_mode": "HTML",
    }
    try:
        r = requests.post(url, json=payload, timeout=10)
        print(f"[TG] 발송: {r.status_code}")
    except Exception as e:
        print(f"[TG] 오류: {e}")


def run_after_hours():
    now = datetime.datetime.now()
    print(f"[AH] 시작: {now.strftime('%H:%M')}")

    all_data = []
    msg = [f"<b>🔔 ANDY JO STOCK AI</b>\n{now.strftime('%Y-%m-%d %H:%M')} 시간외 특징주\n"]

    # KRX 등락률
    krx = fetch_krx_after_hours()
    msg.append("📊 <b>시간외 등락률 상위</b>")
    for s in krx[:10]:
        nm = s.get("ISU_NM", "")
        rt = s.get("FLUC_RT", 0)
        sign = "+" if float(rt) > 0 else ""
        msg.append(f"  {nm} {sign}{rt}%")
        all_data.append({
            "type": "등락률", "name": nm,
            "detail": f"{sign}{rt}%", "link": "", "flag": "📊"
        })

    # DART 공시
    dart = fetch_dart_after_hours()
    important = [
        d for d in dart
        if any(k in d.get("report_nm", "") for k in BAD_KW + GOOD_KW)
    ]
    msg.append("\n📋 <b>주요 DART 공시</b>")
    for d in important[:8]:
        corp = d.get("corp_name", "")
        rpt  = d.get("report_nm", "")
        rcpt = d.get("rcept_no", "")
        flag = "⚠️" if any(k in rpt for k in BAD_KW) else "✅"
        msg.append(f"  {flag} [{corp}] {rpt}")
        all_data.append({
            "type": "공시", "name": corp, "detail": rpt,
            "link": f"https://dart.fss.or.kr/dsaf001/main.do?rcpNo={rcpt}",
            "flag": flag,
        })

    # 후보종목 중 공시 매칭
    try:
        candidates = load_candidates()
        if not candidates.empty and "name" in candidates.columns:
            cand_names = candidates["name"].tolist()
            matched = [
                d for d in dart
                if d.get("corp_name", "") in cand_names
            ]
            if matched:
                msg.append("\n⭐ <b>후보종목 공시 알림</b>")
                for d in matched[:5]:
                    corp = d.get("corp_name", "")
                    rpt  = d.get("report_nm", "")
                    flag = "⚠️" if any(k in rpt for k in BAD_KW) else "✅"
                    msg.append(f"  {flag} [{corp}] {rpt}")
    except:
        pass

    # 네이버 금융 뉴스
    news = fetch_naver_finance_news()
    msg.append("\n📰 <b>네이버 금융 뉴스</b>")
    for n in news[:5]:
        msg.append(f"  · {n['title']}")
        all_data.append({
            "type": "뉴스", "name": n["title"],
            "detail": n.get("pub", ""), "link": n["link"], "flag": "📰"
        })

    save_after_hours(all_data)
    send_telegram("\n".join(msg))
    print(f"[AH] 완료: {len(all_data)}건")


if __name__ == "__main__":
    run_after_hours()
