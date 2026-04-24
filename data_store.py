"""
Andy Jo Stock AI — Google Sheets 데이터 저장 엔진
누적 저장 (덮어쓰기 없음) + 4개 시트 관리
DAILY_SNAPSHOT / STOCK_HISTORY / ALERT_LOG / THEME_DAILY
"""

import os
import logging
import gspread
from datetime import datetime
from google.oauth2.service_account import Credentials

log = logging.getLogger(__name__)

# ────────────────────────────────────────
# Google Sheets 연결 설정
# ────────────────────────────────────────
SCOPES = [
    "https://spreadsheets.google.com/feeds",
    "https://www.googleapis.com/auth/drive"
]

SHEET_ID = os.environ.get("GOOGLE_SHEET_ID", "")

# 시트 탭 이름
TAB_DAILY    = "DAILY_SNAPSHOT"
TAB_HISTORY  = "STOCK_HISTORY"
TAB_ALERT    = "ALERT_LOG"
TAB_THEME    = "THEME_DAILY"
TAB_NEWS     = "NEWS_DATA"


def get_client():
    """Google Sheets 클라이언트 반환"""
    import json

    creds_json = os.environ.get("GOOGLE_CREDENTIALS", "")
    if not creds_json:
        raise ValueError("GOOGLE_CREDENTIALS 환경변수 없음")

    creds_dict = json.loads(creds_json)
    creds = Credentials.from_service_account_info(creds_dict, scopes=SCOPES)
    return gspread.authorize(creds)


def get_or_create_sheet(client, sheet_id: str, tab_name: str):
    """시트 탭 없으면 자동 생성"""
    wb = client.open_by_key(sheet_id)
    try:
        return wb.worksheet(tab_name)
    except gspread.WorksheetNotFound:
        ws = wb.add_worksheet(title=tab_name, rows=10000, cols=30)
        log.info(f"새 시트 탭 생성: {tab_name}")
        return ws


# ────────────────────────────────────────
# 헤더 초기화
# ────────────────────────────────────────
HEADERS = {
    TAB_DAILY: [
        "날짜", "종목코드", "종목명", "트랙",
        "총점", "시가총액(억)", "테마",
        "L2재무점수", "L4DART점수", "L5테마점수",
        "현재가", "52주저점", "거래량비율", "20일수익률",
        "탈락사유"
    ],
    TAB_HISTORY: [
        "종목코드", "종목명", "최초진입일", "최초트랙",
        "현재트랙", "트랙변경이력",
        "최고점수", "최고점수일", "최근업데이트",
        "누적거래일수", "BUY_NOW전환일", "LAUNCHED전환일"
    ],
    TAB_ALERT: [
        "날짜", "시각", "종목코드", "종목명",
        "이벤트유형", "상세내용"
    ],
    TAB_THEME: [
        "날짜", "테마명", "온도점수",
        "뉴스신호", "거래량신호", "주가신호", "커뮤니티신호",
        "랭킹", "관련종목수"
    ],
    TAB_NEWS: [
        "날짜", "제목", "URL", "소스", "타입", "테마태그"
    ]
}


def ensure_header(ws, tab_name: str):
    """헤더 행이 없으면 첫 행에 헤더 추가"""
    try:
        first_row = ws.row_values(1)
        if not first_row:
            ws.append_row(HEADERS[tab_name])
            log.info(f"헤더 추가: {tab_name}")
    except Exception as e:
        log.warning(f"헤더 확인 실패 ({tab_name}): {e}")


# ────────────────────────────────────────
# 1. DAILY_SNAPSHOT 저장 (매일 누적)
# ────────────────────────────────────────
def save_daily_snapshot(results: dict, today: str):
    """
    매일 전체 스냅샷 저장 — 절대 덮어쓰지 않고 행 추가만
    """
    if not SHEET_ID:
        log.warning("GOOGLE_SHEET_ID 없음 — 저장 건너뜀")
        return

    try:
        client = get_client()
        ws = get_or_create_sheet(client, SHEET_ID, TAB_DAILY)
        ensure_header(ws, TAB_DAILY)

        rows = []
        for track, stocks in results.items():
            for s in stocks:
                detail = s.get("detail", {})
                l6     = detail.get("l6", {})
                rows.append([
                    today,
                    s.get("code", ""),
                    s.get("name", ""),
                    track,
                    s.get("total_score", 0),
                    s.get("mktcap", 0),
                    ", ".join(s.get("themes", [])),
                    detail.get("l2", {}).get("year_return", ""),
                    s.get("detail", {}).get("l4", {}).get("dart_score", 0),
                    detail.get("l5_theme_score", 0),
                    l6.get("current", ""),
                    l6.get("low_52w", ""),
                    l6.get("vol_ratio", ""),
                    detail.get("gain_20d", ""),
                    s.get("reject_reason", "")
                ])

        if rows:
            ws.append_rows(rows, value_input_option="USER_ENTERED")
            log.info(f"DAILY_SNAPSHOT 저장: {len(rows)}행 추가 ({today})")

    except Exception as e:
        log.error(f"DAILY_SNAPSHOT 저장 실패: {e}")


# ────────────────────────────────────────
# 2. STOCK_HISTORY 저장 (종목별 이력 카드)
# ────────────────────────────────────────
def save_stock_history(results: dict, today: str):
    """
    종목별 이력 카드 업데이트
    - 신규 종목: 새 행 추가
    - 기존 종목: 현재트랙 + 변경이력 업데이트
    """
    if not SHEET_ID:
        return

    try:
        client = get_client()
        ws = get_or_create_sheet(client, SHEET_ID, TAB_HISTORY)
        ensure_header(ws, TAB_HISTORY)

        # 기존 데이터 로드
        existing = ws.get_all_records()
        code_to_row = {}
        for i, row in enumerate(existing, start=2):  # 헤더가 1행이므로 2행부터
            code_to_row[str(row.get("종목코드", ""))] = i

        now_str = datetime.now().strftime("%Y-%m-%d %H:%M")

        for track, stocks in results.items():
            for s in stocks:
                code = s.get("code", "")
                name = s.get("name", "")
                score = s.get("total_score", 0)

                if code in code_to_row:
                    # 기존 종목 업데이트
                    row_num = code_to_row[code]
                    existing_row = existing[row_num - 2]
                    prev_track = existing_row.get("현재트랙", "")

                    # 트랙 변경 감지 → 이력 기록
                    history = existing_row.get("트랙변경이력", "")
                    if prev_track != track:
                        history += f" → {today}:{track}"
                        # ALERT 기록
                        save_alert(
                            code, name,
                            f"트랙변경: {prev_track}→{track}",
                            f"점수: {score}",
                            today
                        )

                    # BUY_NOW / LAUNCHED 전환일 기록
                    buy_now_date = existing_row.get("BUY_NOW전환일", "")
                    launched_date = existing_row.get("LAUNCHED전환일", "")
                    if track == "BUY_NOW" and not buy_now_date:
                        buy_now_date = today
                    if track == "LAUNCHED" and not launched_date:
                        launched_date = today

                    # 최고점수 업데이트
                    prev_best = float(existing_row.get("최고점수", 0) or 0)
                    best_score = max(prev_best, score)
                    best_date = existing_row.get("최고점수일", "") if prev_best >= score else today

                    ws.update(f"E{row_num}", [[track]])
                    ws.update(f"F{row_num}", [[history]])
                    ws.update(f"G{row_num}", [[best_score]])
                    ws.update(f"H{row_num}", [[best_date]])
                    ws.update(f"I{row_num}", [[now_str]])
                    ws.update(f"K{row_num}", [[buy_now_date]])
                    ws.update(f"L{row_num}", [[launched_date]])

                else:
                    # 신규 종목 추가
                    ws.append_row([
                        code, name, today, track,
                        track,          # 현재트랙
                        f"{today}:{track}",  # 트랙변경이력
                        score, today,   # 최고점수, 최고점수일
                        now_str,        # 최근업데이트
                        1,              # 누적거래일수
                        today if track == "BUY_NOW" else "",
                        today if track == "LAUNCHED" else ""
                    ])
                    # 신규 진입 알림
                    save_alert(code, name, f"신규진입: {track}", f"점수: {score}", today)

        log.info(f"STOCK_HISTORY 업데이트 완료 ({today})")

    except Exception as e:
        log.error(f"STOCK_HISTORY 저장 실패: {e}")


# ────────────────────────────────────────
# 3. ALERT_LOG 저장
# ────────────────────────────────────────
def save_alert(code: str, name: str, event: str, detail: str, today: str):
    """중요 이벤트 알림 로그 저장"""
    if not SHEET_ID:
        return

    try:
        client = get_client()
        ws = get_or_create_sheet(client, SHEET_ID, TAB_ALERT)
        ensure_header(ws, TAB_ALERT)

        now_str = datetime.now().strftime("%H:%M:%S")
        ws.append_row([today, now_str, code, name, event, detail])

    except Exception as e:
        log.debug(f"ALERT_LOG 저장 실패: {e}")


# ────────────────────────────────────────
# 4. THEME_DAILY 저장
# ────────────────────────────────────────
def save_theme_daily(theme_scores: dict, today: str):
    """테마 온도 일별 누적 저장"""
    if not SHEET_ID:
        return

    try:
        client = get_client()
        ws = get_or_create_sheet(client, SHEET_ID, TAB_THEME)
        ensure_header(ws, TAB_THEME)

        rows = []
        for theme, data in theme_scores.items():
            rows.append([
                today,
                theme,
                data.get("score", 0),
                data.get("news", 0),
                data.get("volume", 0),
                data.get("price", 0),
                data.get("community", 0),
                data.get("rank", 0),
                len(data.get("stocks", []))
            ])

        if rows:
            ws.append_rows(rows, value_input_option="USER_ENTERED")
            log.info(f"THEME_DAILY 저장: {len(rows)}개 테마 ({today})")

    except Exception as e:
        log.error(f"THEME_DAILY 저장 실패: {e}")


# ────────────────────────────────────────
# 5. NEWS_DATA 저장
# ────────────────────────────────────────
def save_news_data(news_data: list, today: str = None):
    """수집된 뉴스 저장 (최근 100건만)"""
    if not SHEET_ID:
        return

    if not today:
        today = datetime.now().strftime("%Y-%m-%d")

    try:
        client = get_client()
        ws = get_or_create_sheet(client, SHEET_ID, TAB_NEWS)
        ensure_header(ws, TAB_NEWS)

        rows = []
        for article in news_data[:100]:
            rows.append([
                today,
                article.get("title", ""),
                article.get("url", ""),
                article.get("source", ""),
                article.get("type", "news"),
                ", ".join(article.get("themes", []))
            ])

        if rows:
            ws.append_rows(rows, value_input_option="USER_ENTERED")
            log.info(f"NEWS_DATA 저장: {len(rows)}건")

    except Exception as e:
        log.error(f"NEWS_DATA 저장 실패: {e}")


# ────────────────────────────────────────
# 6. 데이터 로드 함수 (app.py 연동)
# ────────────────────────────────────────
def load_daily_snapshot(days: int = 30) -> list:
    """최근 N일 DAILY_SNAPSHOT 로드"""
    try:
        client = get_client()
        ws = get_or_create_sheet(client, SHEET_ID, TAB_DAILY)
        records = ws.get_all_records()
        return records
    except Exception as e:
        log.error(f"DAILY_SNAPSHOT 로드 실패: {e}")
        return []


def load_stock_history() -> list:
    """STOCK_HISTORY 전체 로드"""
    try:
        client = get_client()
        ws = get_or_create_sheet(client, SHEET_ID, TAB_HISTORY)
        return ws.get_all_records()
    except Exception as e:
        log.error(f"STOCK_HISTORY 로드 실패: {e}")
        return []


def load_theme_daily(days: int = 30) -> list:
    """최근 N일 THEME_DAILY 로드"""
    try:
        client = get_client()
        ws = get_or_create_sheet(client, SHEET_ID, TAB_THEME)
        return ws.get_all_records()
    except Exception as e:
        log.error(f"THEME_DAILY 로드 실패: {e}")
        return []


def load_news_data() -> list:
    """NEWS_DATA 로드"""
    try:
        client = get_client()
        ws = get_or_create_sheet(client, SHEET_ID, TAB_NEWS)
        return ws.get_all_records()
    except Exception as e:
        log.error(f"NEWS_DATA 로드 실패: {e}")
        return []
