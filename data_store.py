import os, json
import gspread
from google.oauth2.service_account import Credentials

SHEETS_ID = "1OqRboKwx7X0-3W67_raZyV2ZjcQ_G7ylHilf7SgAU88"
SCOPES = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]

def get_gsheet():
    cred_json = os.environ.get("GOOGLE_CREDENTIALS", "")
    if not cred_json:
        print("[Sheets] GOOGLE_CREDENTIALS 없음")
        return None
    try:
        creds = Credentials.from_service_account_info(
            json.loads(cred_json), scopes=SCOPES
        )
        gc = gspread.authorize(creds)
        return gc.open_by_key(SHEETS_ID)
    except Exception as e:
        print(f"[Sheets 연결 오류] {e}")
        return None

def save_candidates(df):
    try:
        sh = get_gsheet()
        if not sh:
            return
        try:
            ws = sh.worksheet("후보종목")
        except:
            ws = sh.add_worksheet(title="후보종목", rows=500, cols=20)
        ws.clear()
        if df.empty:
            print("[Sheets] 저장할 후보 없음")
            return
        # code 컬럼 앞자리 0 보존: 텍스트 강제 지정
        df = df.copy()
        df["code"] = "'" + df["code"].astype(str).str.zfill(6)
        rows = [df.columns.tolist()] + df.values.tolist()
        ws.update(rows)
        print(f"[Sheets] 후보종목 {len(df)}건 저장 완료")
    except Exception as e:
        print(f"[save_candidates 오류] {e}")

def load_candidates():
    try:
        sh = get_gsheet()
        if not sh:
            return None
        ws = sh.worksheet("후보종목")
        data = ws.get_all_records()
        return data if data else None
    except Exception as e:
        print(f"[load_candidates 오류] {e}")
        return None

def load_market_report():
    try:
        sh = get_gsheet()
        if not sh:
            return None
        ws = sh.worksheet("시황리포트")
        cell = ws.acell("B2").value
        return json.loads(cell) if cell else None
    except Exception as e:
        print(f"[load_market_report 오류] {e}")
        return None
