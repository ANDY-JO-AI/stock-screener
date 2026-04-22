# market_report.py
import os, requests, json, datetime, time
import xml.etree.ElementTree as ET
import pandas as pd
from data_store import save_market_report
from news_engine import fetch_all_news
from theme_engine import build_theme_report

DART_KEY = os.environ.get("DART_API_KEY", "")
HEADERS  = {"User-Agent": "Mozilla/5.0 (compatible; StockBot/1.0)"}


def fetch_krx_upper() -> list:
    """KRX 상한가·급등 종목"""
    url = "http://data.krx.co.kr/comm/bldAttendant/getJsonData.cmd"
    body = {
        "bld":        "dbms/MDC/STAT/standard/MDCSTAT01501",
        "mktId":      "ALL",
        "trdDd":      datetime.date.today().strftime("%Y%m%d"),
        "share":      "1",
        "money":      "1",
        "csvxls_isNo":"false",
    }
    try:
        r = requests.post(url, data=body, headers=HEADERS, timeout=10)
        items = r.json().get("OutBlock_1", [])
        df = pd.DataFrame(items)
        if df.empty:
            return []
        df["FLUC_RT"] = pd.to_numeric(df.get("FLUC_RT", 0), errors="coerce").fillna(0)
        df = df[df["FLUC_RT"] >= 10].sort_values("FLUC_RT", ascending=False)
        result = []
        for _, row in df.head(30).iterrows():
            result.append({
                "name":        row.get("ISU_NM", ""),
                "code":        row.get("ISU_SRT_CD", ""),
                "change_rate": float(row.get("FLUC_RT", 0)),
                "price":       row.get("TDD_CLSPRC", ""),
                "volume":      row.get("ACC_TRDVOL", ""),
                "reason":      "",
            })
        return result
    except Exception as e:
        print(f"[MR] KRX 오류: {e}")
        return []


def fetch_dart_today() -> list:
    """오늘의 DART 공시"""
    today = datetime.date.today().strftime("%Y%m%d")
    url   = "https://opendart.fss.or.kr/api/list.json"
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
            items = data.get("list", [])
            result = []
            for d in items:
                rpt = d.get("report_nm", "")
                bad_kw  = ["유상증자", "전환사채", "신주인수권", "횡령", "배임", "감사의견"]
                good_kw = ["자사주취득", "계약체결", "수주", "특허", "임상", "허가"]
                flag = "⚠️" if any(k in rpt for k in bad_kw) else \
                       "✅" if any(k in rpt for k in good_kw) else "📋"
                result.append({
                    "corp":  d.get("corp_name", ""),
                    "title": rpt,
                    "url":   f"https://dart.fss.or.kr/dsaf001/main.do?rcpNo={d.get('rcept_no','')}",
                    "flag":  flag,
                    "time":  d.get("rcept_dt", ""),
                })
            return result
    except Exception as e:
        print(f"[MR] DART 오류: {e}")
    return []


def fetch_index_summary() -> dict:
    """KOSPI / KOSDAQ 지수 (네이버 금융 RSS)"""
    summary = {}
    try:
        import FinanceDataReader as fdr
        today = datetime.date.today()
        start = today - datetime.timedelta(days=5)
        kospi  = fdr.DataReader("KS11", start)
        kosdaq = fdr.DataReader("KQ11", start)
        usd    = fdr.DataReader("USD/KRW", start)

        def last_row(df):
            return df.iloc[-1] if not df.empty else None

        kp = last_row(kospi)
        kq = last_row(kosdaq)
        fx = last_row(usd)

        summary = {
            "kospi":  {
                "close":  float(kp["Close"]) if kp is not None else 0,
                "change": float(kp["Change"]) if kp is not None else 0,
            },
            "kosdaq": {
                "close":  float(kq["Close"]) if kq is not None else 0,
                "change": float(kq["Change"]) if kq is not None else 0,
            },
            "usd_krw": float(fx["Close"]) if fx is not None else 0,
        }
    except Exception as e:
        print(f"[MR] 지수 오류: {e}")
    return summary


def run_market_report():
    now = datetime.datetime.now().strftime("%Y-%m-%d %H:%M")
    print(f"[MR] 시황 리포트 생성: {now}")

    # 뉴스 수집
    news = fetch_all_news()

    # 테마 분류
    themes_raw = build_theme_report(news)
    themes = {t["theme"]: t for t in themes_raw}

    # 급등·상한가
    upper = fetch_krx_upper()

    # DART 공시
    dart = fetch_dart_today()

    # 지수
    index = fetch_index_summary()

    report = {
        "generated_at":        now,
        "news":                news[:100],
        "themes":              themes,
        "upper_limit_stocks":  upper,
        "dart_disclosures":    dart,
        "index_summary":       index,
    }

    save_market_report(report)
    print(f"[MR] 완료: 뉴스{len(news)}건, 테마{len(themes)}개, 급등{len(upper)}종목")


if __name__ == "__main__":
    run_market_report()
