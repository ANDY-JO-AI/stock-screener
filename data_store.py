"""
data_store.py — Google Sheets 저장 엔진 v3
시간여행TV 기준 컬럼 설계 완전 반영
"""

import logging
import os
import json
import time
from datetime import datetime

import gspread
from google.oauth2.service_account import Credentials

log = logging.getLogger(__name__)

# ────────────────────────────────────────────
# Google Sheets 설정
# ────────────────────────────────────────────
SCOPES = [
    "https://spreadsheets.google.com/feeds",
    "https://www.googleapis.com/auth/drive"
]
SPREADSHEET_ID = os.environ.get("GOOGLE_SHEET_ID", "")

# 탭 이름
TAB_TODAY   = "TODAY"
TAB_HISTORY = "HISTORY"
TAB_ALERTS  = "ALERTS"
TAB_THEME   = "THEME"

# TODAY 탭 헤더
HEADER_TODAY = [
    "날짜", "종목명", "코드", "트랙",
    "재무점수", "테마점수", "테마명",
    "현재가", "등락률", "시총(억)",
    "52주위치", "부채비율", "유보율",
    "최대주주지분", "CB/BW여부",
    "선정이유", "주의사항", "수동확인항목",
    "네이버링크", "DART링크"
]

# HISTORY 탭 헤더
HEADER_HISTORY = [
    "종목코드", "종목명", "최초진입일", "최초트랙",
    "현재트랙", "트랙변경이력",
    "최초가격", "현재가격", "수익률(%)",
    "최고점수", "최근업데이트"
]

# ALERTS 탭 헤더
HEADER_ALERTS = [
    "일시", "종목코드", "종목명",
    "이벤트", "설명", "트랙변경전", "트랙변경후"
]

# THEME 탭 헤더
HEADER_THEME = [
    "날짜", "테마명", "점수", "등급",
    "뉴스건수", "종목매칭수",
    "정치연계", "계절테마"
]


def _get_client():
    """Google Sheets 인증 클라이언트 반환"""
    creds_json = os.environ.get("GOOGLE_CREDENTIALS_JSON", "")
    if not creds_json:
        raise ValueError("GOOGLE_CREDENTIALS_JSON 환경변수가 설정되지 않았습니다.")
    creds_dict = json.loads(creds_json)
    creds = Credentials.from_service_account_info(creds_dict, scopes=SCOPES)
    return gspread.authorize(creds)


def _get_or_create_sheet(spreadsheet, tab_name: str, header: list):
    """탭이 없으면 생성 후 헤더 삽입, 있으면 그대로 반환"""
    try:
        ws = spreadsheet.worksheet(tab_name)
    except gspread.WorksheetNotFound:
        ws = spreadsheet.add_worksheet(title=tab_name, rows=5000, cols=len(header))
        ws.append_row(header, value_input_option="RAW")
        log.info(f"  → 탭 생성: {tab_name}")
    return ws


def _safe_append(ws, rows: list, batch_size: int = 50):
    """대량 데이터 안전 삽입 (rate limit 대응)"""
    for i in range(0, len(rows), batch_size):
        batch = rows[i:i + batch_size]
        ws.append_rows(batch, value_input_option="RAW")
        if i + batch_size < len(rows):
            time.sleep(1)


def save_today(spreadsheet, results: dict, today: str):
    """TODAY 탭에 오늘 결과 저장"""
    ws = _get_or_create_sheet(spreadsheet, TAB_TODAY, HEADER_TODAY)

    rows = []
    all_stocks = (
        [(s, "CORE")     for s in results.get("CORE", [])] +
        [(s, "BUY_NOW")  for s in results.get("BUY_NOW", [])] +
        [(s, "READY")    for s in results.get("READY", [])] +
        [(s, "LAUNCHED") for s in results.get("LAUNCHED", [])]
    )

    for stock, track in all_stocks:
        fin = stock.get("financial", {})
        rows.append([
            today,
            stock.get("name", ""),
            stock.get("code", ""),
            track,
            stock.get("fin_score", 0),
            stock.get("theme_score", 0),
            ", ".join(stock.get("themes", [])),
            stock.get("price", 0),
            stock.get("change_ratio", 0),
            stock.get("marcap_억", 0),
            stock.get("price_52w_position", ""),
            fin.get("debt_ratio", ""),
            fin.get("reserve_ratio", ""),
            stock.get("major_holder_pct", ""),
            "Y" if stock.get("has_cb_bw") else "N",
            stock.get("reason", ""),
            "; ".join(stock.get("warnings", [])),
            "; ".join(stock.get("manual_checks", [])),
            stock.get("naver_url", ""),
            stock.get("dart_url", "")
        ])

    if rows:
        _safe_append(ws, rows)
        log.info(f"  → TODAY 저장: {len(rows)}행")
    else:
        log.warning("  → TODAY 저장할 데이터 없음")


def save_history(spreadsheet, results: dict, today: str):
    """HISTORY 탭 누적 업데이트 (신규 종목 추가 / 기존 종목 트랙 갱신)"""
    ws = _get_or_create_sheet(spreadsheet, TAB_HISTORY, HEADER_HISTORY)

    # 기존 데이터 로드
    existing = ws.get_all_records()
    existing_map = {row["종목코드"]: (i + 2, row) for i, row in enumerate(existing)}

    all_stocks = (
        [(s, "CORE")     for s in results.get("CORE", [])] +
        [(s, "BUY_NOW")  for s in results.get("BUY_NOW", [])] +
        [(s, "READY")    for s in results.get("READY", [])] +
        [(s, "LAUNCHED") for s in results.get("LAUNCHED", [])]
    )

    new_rows = []
    update_cells = []

    for stock, track in all_stocks:
        code = stock.get("code", "")
        price = stock.get("price", 0)

        if code in existing_map:
            row_idx, old_row = existing_map[code]
            old_track = old_row.get("현재트랙", "")
            old_price = old_row.get("최초가격", price)

            # 수익률 계산
            try:
                ret = round((price - float(old_price)) / float(old_price) * 100, 2) if old_price else 0
            except Exception:
                ret = 0

            # 트랙 변경 이력
            history_str = old_row.get("트랙변경이력", "")
            if old_track != track:
                history_str += f" → {today}:{track}"

            update_cells.append({
                "row": row_idx,
                "현재트랙": track,
                "트랙변경이력": history_str,
                "현재가격": price,
                "수익률(%)": ret,
                "최근업데이트": today
            })
        else:
            new_rows.append([
                code,
                stock.get("name", ""),
                today,           # 최초진입일
                track,           # 최초트랙
                track,           # 현재트랙
                "",              # 트랙변경이력
                price,           # 최초가격
                price,           # 현재가격
                0,               # 수익률
                stock.get("fin_score", 0),
                today
            ])

    # 신규 종목 추가
    if new_rows:
        _safe_append(ws, new_rows)
        log.info(f"  → HISTORY 신규 추가: {len(new_rows)}종목")

    # 기존 종목 업데이트
    for upd in update_cells:
        row_idx = upd["row"]
        ws.update_cell(row_idx, HEADER_HISTORY.index("현재트랙") + 1,     upd["현재트랙"])
        ws.update_cell(row_idx, HEADER_HISTORY.index("트랙변경이력") + 1,  upd["트랙변경이력"])
        ws.update_cell(row_idx, HEADER_HISTORY.index("현재가격") + 1,      upd["현재가격"])
        ws.update_cell(row_idx, HEADER_HISTORY.index("수익률(%)") + 1,     upd["수익률(%)"])
        ws.update_cell(row_idx, HEADER_HISTORY.index("최근업데이트") + 1,  upd["최근업데이트"])
        time.sleep(0.3)

    log.info(f"  → HISTORY 업데이트: {len(update_cells)}종목")


def save_alerts(spreadsheet, results: dict, today: str):
    """ALERTS 탭: BUY_NOW / CORE 진입 종목 알림 기록"""
    ws = _get_or_create_sheet(spreadsheet, TAB_ALERTS, HEADER_ALERTS)

    rows = []
    now_str = datetime.now().strftime("%Y-%m-%d %H:%M")

    for track in ["CORE", "BUY_NOW"]:
        for stock in results.get(track, []):
            rows.append([
                now_str,
                stock.get("code", ""),
                stock.get("name", ""),
                f"{track} 진입",
                stock.get("reason", ""),
                "",
                track
            ])

    if rows:
        _safe_append(ws, rows)
        log.info(f"  → ALERTS 저장: {len(rows)}건")


def save_themes(spreadsheet, theme_scores: dict, today: str):
    """THEME 탭: 오늘 테마 온도 저장"""
    ws = _get_or_create_sheet(spreadsheet, TAB_THEME, HEADER_THEME)

    rows = []
    for theme_name, data in theme_scores.items():
        rows.append([
            today,
            theme_name,
            data.get("score", 0),
            data.get("grade", ""),
            data.get("news_count", 0),
            data.get("stock_count", 0),
            "Y" if data.get("is_political") else "N",
            "Y" if data.get("is_seasonal") else "N"
        ])

    if rows:
        _safe_append(ws, rows)
        log.info(f"  → THEME 저장: {len(rows)}개 테마")


def save_all(results: dict, theme_scores: dict, news_list: list, today: str):
    """전체 저장 통합 실행"""
    log.info("[data_store] Google Sheets 저장 시작")

    try:
        client = _get_client()
        spreadsheet = client.open_by_key(SPREADSHEET_ID)
    except Exception as e:
        log.error(f"  → Sheets 연결 실패: {e}")
        return

    save_today(spreadsheet, results, today)
    time.sleep(1)
    save_history(spreadsheet, results, today)
    time.sleep(1)
    save_alerts(spreadsheet, results, today)
    time.sleep(1)
    save_themes(spreadsheet, theme_scores, today)

    total = sum(len(v) for v in results.values())
    log.info(f"[data_store] 저장 완료 — 총 {total}종목 / {len(theme_scores)}테마")
