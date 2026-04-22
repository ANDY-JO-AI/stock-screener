# data_store.py
import os, json, time
import gspread
from google.oauth2.service_account import Credentials
import pandas as pd

SHEETS_ID = "1OqRboKwx7X0-3W67_raZyV2ZjcQ_G7ylHilf7SgAU88"
SCOPES = [
    "https://spreadsheets.google.com/feeds",
    "https://www.googleapis.com/auth/drive"
]

WORKSHEETS = {
    "후보종목":    {"rows": 200,  "cols": 30},
    "시황리포트":  {"rows": 10,   "cols": 5},
    "테마데이터":  {"rows": 100,  "cols": 10},
    "시간외특징주":{"rows": 100,  "cols": 10},
    "현재가":      {"rows": 200,  "cols": 10},
    "뉴스데이터":  {"rows": 500,  "cols": 8},
}


def get_gsheet():
    """Google Sheets 클라이언트 반환"""
    raw = os.environ.get("GOOGLE_CREDENTIALS", "")
    if not raw:
        raise ValueError("GOOGLE_CREDENTIALS 환경변수 없음")
    cred_info = json.loads(raw)
    creds = Credentials.from_service_account_info(cred_info, scopes=SCOPES)
    gc = gspread.authorize(creds)
    return gc


def get_or_create_worksheet(sh, name):
    """워크시트 없으면 자동 생성"""
    try:
        return sh.worksheet(name)
    except gspread.WorksheetNotFound:
        cfg = WORKSHEETS.get(name, {"rows": 100, "cols": 10})
        return sh.add_worksheet(name, rows=cfg["rows"], cols=cfg["cols"])


def save_candidates(df: pd.DataFrame):
    """후보종목 저장"""
    try:
        gc = get_gsheet()
        sh = gc.open_by_key(SHEETS_ID)
        ws = get_or_create_worksheet(sh, "후보종목")
        ws.clear()

        # score_breakdown, score_detail 컬럼은 JSON 문자열로 변환
        df = df.copy()
        for col in ["score_breakdown", "score_detail", "weight_info", "theme"]:
            if col in df.columns:
                df[col] = df[col].apply(
                    lambda x: json.dumps(x, ensure_ascii=False)
                    if isinstance(x, (dict, list)) else str(x)
                )

        # code 앞자리 보호 (leading zero)
        if "code" in df.columns:
            df["code"] = "'" + df["code"].astype(str).str.zfill(6)

        headers = df.columns.tolist()
        rows = df.values.tolist()
        ws.update("A1", [headers] + rows)
        print(f"[DS] 후보종목 {len(df)}건 저장 완료")
    except Exception as e:
        print(f"[DS] save_candidates 오류: {e}")
        raise


def load_candidates() -> pd.DataFrame:
    """후보종목 로드"""
    try:
        gc = get_gsheet()
        sh = gc.open_by_key(SHEETS_ID)
        ws = get_or_create_worksheet(sh, "후보종목")
        data = ws.get_all_records()
        if not data:
            return pd.DataFrame()
        df = pd.DataFrame(data)
        if "code" in df.columns:
            df["code"] = df["code"].astype(str).str.lstrip("'").str.zfill(6)
        return df
    except Exception as e:
        print(f"[DS] load_candidates 오류: {e}")
        return pd.DataFrame()


def save_market_report(report: dict):
    """시황리포트 저장"""
    try:
        gc = get_gsheet()
        sh = gc.open_by_key(SHEETS_ID)
        ws = get_or_create_worksheet(sh, "시황리포트")
        ws.clear()
        ws.update("A1", [["key", "value"]])
        ws.update("A2", [["generated_at", report.get("generated_at", "")]])
        ws.update("B2", [[ json.dumps(report, ensure_ascii=False) ]])
        print("[DS] 시황리포트 저장 완료")
    except Exception as e:
        print(f"[DS] save_market_report 오류: {e}")
        raise


def load_market_report() -> dict:
    """시황리포트 로드"""
    try:
        gc = get_gsheet()
        sh = gc.open_by_key(SHEETS_ID)
        ws = get_or_create_worksheet(sh, "시황리포트")
        cell = ws.acell("B2").value
        return json.loads(cell) if cell else {}
    except Exception as e:
        print(f"[DS] load_market_report 오류: {e}")
        return {}


def save_theme_data(data: list):
    """테마데이터 저장"""
    try:
        gc = get_gsheet()
        sh = gc.open_by_key(SHEETS_ID)
        ws = get_or_create_worksheet(sh, "테마데이터")
        ws.clear()
        ws.update("A1", [["updated_at", pd.Timestamp.now().strftime("%Y-%m-%d %H:%M")]])
        ws.update("A2", [[ json.dumps(data, ensure_ascii=False) ]])
        print(f"[DS] 테마데이터 {len(data)}건 저장 완료")
    except Exception as e:
        print(f"[DS] save_theme_data 오류: {e}")
        raise


def load_theme_data() -> list:
    """테마데이터 로드"""
    try:
        gc = get_gsheet()
        sh = gc.open_by_key(SHEETS_ID)
        ws = get_or_create_worksheet(sh, "테마데이터")
        cell = ws.acell("A2").value
        return json.loads(cell) if cell else []
    except Exception as e:
        print(f"[DS] load_theme_data 오류: {e}")
        return []


def save_after_hours(data: list):
    """시간외특징주 저장"""
    try:
        gc = get_gsheet()
        sh = gc.open_by_key(SHEETS_ID)
        ws = get_or_create_worksheet(sh, "시간외특징주")
        ws.clear()
        ws.update("A1", [["updated_at", pd.Timestamp.now().strftime("%Y-%m-%d %H:%M")]])
        ws.update("A2", [[ json.dumps(data, ensure_ascii=False) ]])
        print(f"[DS] 시간외특징주 {len(data)}건 저장 완료")
    except Exception as e:
        print(f"[DS] save_after_hours 오류: {e}")
        raise


def load_after_hours() -> list:
    """시간외특징주 로드"""
    try:
        gc = get_gsheet()
        sh = gc.open_by_key(SHEETS_ID)
        ws = get_or_create_worksheet(sh, "시간외특징주")
        cell = ws.acell("A2").value
        return json.loads(cell) if cell else []
    except Exception as e:
        print(f"[DS] load_after_hours 오류: {e}")
        return []


def save_price_data(data: list):
    """현재가 저장"""
    try:
        gc = get_gsheet()
        sh = gc.open_by_key(SHEETS_ID)
        ws = get_or_create_worksheet(sh, "현재가")
        ws.clear()
        ws.update("A1", [["updated_at", pd.Timestamp.now().strftime("%Y-%m-%d %H:%M")]])
        ws.update("A2", [[ json.dumps(data, ensure_ascii=False) ]])
        print(f"[DS] 현재가 {len(data)}건 저장 완료")
    except Exception as e:
        print(f"[DS] save_price_data 오류: {e}")
        raise


def load_price_data() -> list:
    """현재가 로드"""
    try:
        gc = get_gsheet()
        sh = gc.open_by_key(SHEETS_ID)
        ws = get_or_create_worksheet(sh, "현재가")
        cell = ws.acell("A2").value
        return json.loads(cell) if cell else []
    except Exception as e:
        print(f"[DS] load_price_data 오류: {e}")
        return []


def save_news_data(data: list):
    """뉴스데이터 저장"""
    try:
        gc = get_gsheet()
        sh = gc.open_by_key(SHEETS_ID)
        ws = get_or_create_worksheet(sh, "뉴스데이터")
        ws.clear()
        ws.update("A1", [["updated_at", pd.Timestamp.now().strftime("%Y-%m-%d %H:%M")]])
        ws.update("A2", [[ json.dumps(data, ensure_ascii=False) ]])
        print(f"[DS] 뉴스데이터 {len(data)}건 저장 완료")
    except Exception as e:
        print(f"[DS] save_news_data 오류: {e}")
        raise


def load_news_data() -> list:
    """뉴스데이터 로드"""
    try:
        gc = get_gsheet()
        sh = gc.open_by_key(SHEETS_ID)
        ws = get_or_create_worksheet(sh, "뉴스데이터")
        cell = ws.acell("A2").value
        return json.loads(cell) if cell else []
    except Exception as e:
        print(f"[DS] load_news_data 오류: {e}")
        return []
