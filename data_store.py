import gspread
from google.oauth2.service_account import Credentials
import pandas as pd
import json, os

SHEETS_ID = "1OqRboKwx7X0-3W67_raZyV2ZjcQ_G7ylHilf7SgAU88"

SCOPES = [
    "https://spreadsheets.google.com/feeds",
    "https://www.googleapis.com/auth/drive"
]

def get_sheet_client():
    creds_raw = os.environ.get("GOOGLE_CREDENTIALS")
    if not creds_raw:
        raise ValueError("GOOGLE_CREDENTIALS 환경변수 없음")
    creds_dict = json.loads(creds_raw)
    creds = Credentials.from_service_account_info(creds_dict, scopes=SCOPES)
    return gspread.authorize(creds)

def save_candidates(df: pd.DataFrame):
    """후보 종목 → 시트1 저장"""
    try:
        gc = get_sheet_client()
        sh = gc.open_by_key(SHEETS_ID)
        ws = sh.get_worksheet(0)
        ws.clear()
        if df.empty:
            print("[Sheets] 후보 없음")
            return
        data = [df.columns.tolist()] + df.astype(str).values.tolist()
        ws.update(data)
        print(f"[Sheets] 후보 {len(df)}개 저장 완료")
    except Exception as e:
        print(f"[Sheets 후보 저장 오류] {e}")

def save_market_report(report: dict):
    """시황 리포트 → 시트2 저장"""
    try:
        gc = get_sheet_client()
        sh = gc.open_by_key(SHEETS_ID)
        # 시트2 없으면 생성
        try:
            ws = sh.get_worksheet(1)
        except:
            ws = sh.add_worksheet(title="시황리포트", rows=500, cols=10)
        ws.clear()
        rows = []
        for key, value in report.items():
            rows.append([key, str(value)])
        ws.update(rows)
        print("[Sheets] 시황 리포트 저장 완료")
    except Exception as e:
        print(f"[Sheets 시황 저장 오류] {e}")

def load_candidates() -> pd.DataFrame:
    """시트1 → 후보 종목 로드"""
    try:
        creds_raw = _get_creds_raw()
        if not creds_raw:
            return pd.DataFrame()
        creds_dict = json.loads(creds_raw) if isinstance(creds_raw, str) else dict(creds_raw)
        creds = Credentials.from_service_account_info(creds_dict, scopes=SCOPES)
        gc = gspread.authorize(creds)
        sh = gc.open_by_key(SHEETS_ID)
        ws = sh.get_worksheet(0)
        data = ws.get_all_records()
        return pd.DataFrame(data)
    except Exception as e:
        print(f"[Sheets 후보 로드 오류] {e}")
        return pd.DataFrame()

def load_market_report() -> dict:
    """시트2 → 시황 리포트 로드"""
    try:
        creds_raw = _get_creds_raw()
        if not creds_raw:
            return {}
        creds_dict = json.loads(creds_raw) if isinstance(creds_raw, str) else dict(creds_raw)
        creds = Credentials.from_service_account_info(creds_dict, scopes=SCOPES)
        gc = gspread.authorize(creds)
        sh = gc.open_by_key(SHEETS_ID)
        ws = sh.get_worksheet(1)
        data = ws.get_all_values()
        return {row[0]: row[1] for row in data if len(row) >= 2}
    except Exception as e:
        print(f"[Sheets 시황 로드 오류] {e}")
        return {}

def _get_creds_raw():
    """Streamlit secrets 또는 환경변수에서 크레덴셜 로드"""
    try:
        import streamlit as st
        return st.secrets.get("GOOGLE_CREDENTIALS")
    except:
        return os.environ.get("GOOGLE_CREDENTIALS")
