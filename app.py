"""
app.py — Andy Jo 주식 AI 대시보드 v3
시간여행TV 기준 종목선정 결과 시각화
Streamlit 기반
"""

import streamlit as st
import gspread
import pandas as pd
import json
import os
from google.oauth2.service_account import Credentials
from datetime import datetime

# ────────────────────────────────────────────
# 페이지 설정
# ────────────────────────────────────────────
st.set_page_config(
    page_title="Andy Jo 주식 AI",
    page_icon="📈",
    layout="wide",
    initial_sidebar_state="expanded"
)

SPREADSHEET_ID = os.environ.get("GOOGLE_SHEET_ID", "")
SCOPES = [
    "https://spreadsheets.google.com/feeds",
    "https://www.googleapis.com/auth/drive"
]

# ────────────────────────────────────────────
# 트랙 색상 / 아이콘
# ────────────────────────────────────────────
TRACK_STYLE = {
    "CORE":     {"icon": "🔴", "label": "CORE",     "color": "#ff4b4b"},
    "BUY_NOW":  {"icon": "🟠", "label": "BUY NOW",  "color": "#ffa500"},
    "READY":    {"icon": "🟡", "label": "READY",    "color": "#ffd700"},
    "LAUNCHED": {"icon": "🚀", "label": "LAUNCHED", "color": "#888888"},
}

GRADE_COLOR = {
    "🔥🔥 과열":        "#ff4b4b",
    "🔥 활성 (최적진입)": "#ffa500",
    "📈 형성중":        "#ffd700",
    "👀 워밍업":        "#4fc3f7",
    "💤 미활성":        "#888888",
}


# ────────────────────────────────────────────
# Google Sheets 연결
# ────────────────────────────────────────────
@st.cache_resource(ttl=300)
def get_spreadsheet():
    creds_json = os.environ.get("GOOGLE_CREDENTIALS_JSON", "")
    if not creds_json:
        return None
    creds = Credentials.from_service_account_info(json.loads(creds_json), scopes=SCOPES)
    client = gspread.authorize(creds)
    return client.open_by_key(SPREADSHEET_ID)


@st.cache_data(ttl=300)
def load_tab(tab_name: str) -> pd.DataFrame:
    try:
        ss = get_spreadsheet()
        if ss is None:
            return pd.DataFrame()
        ws = ss.worksheet(tab_name)
        data = ws.get_all_records()
        return pd.DataFrame(data)
    except Exception as e:
        st.warning(f"{tab_name} 탭 로드 실패: {e}")
        return pd.DataFrame()


# ────────────────────────────────────────────
# 사이드바
# ────────────────────────────────────────────
def render_sidebar(today_df: pd.DataFrame, theme_df: pd.DataFrame):
    with st.sidebar:
        st.image("https://via.placeholder.com/200x60?text=Andy+Jo+AI", width=200)
        st.markdown("---")

        # 오늘 날짜
        today = datetime.now().strftime("%Y-%m-%d")
        st.markdown(f"**📅 기준일:** {today}")

        # 종목 수 요약
        if not today_df.empty:
            today_data = today_df[today_df["날짜"] == today] if "날짜" in today_df.columns else today_df
            core_n    = len(today_data[today_data["트랙"] == "CORE"])
            buy_n     = len(today_data[today_data["트랙"] == "BUY_NOW"])
            ready_n   = len(today_data[today_data["트랙"] == "READY"])
            launch_n  = len(today_data[today_data["트랙"] == "LAUNCHED"])

            st.markdown("### 오늘 결과")
            st.metric("🔴 CORE",     core_n)
            st.metric("🟠 BUY NOW",  buy_n)
            st.metric("🟡 READY",    ready_n)
            st.metric("🚀 LAUNCHED", launch_n)

        # TOP 테마
        if not theme_df.empty:
            today_theme = theme_df[theme_df["날짜"] == today] if "날짜" in theme_df.columns else theme_df
            if not today_theme.empty:
                top = today_theme.sort_values("점수", ascending=False).iloc[0]
                st.markdown("---")
                st.markdown(f"**🔥 TOP 테마:** {top['테마명']}")
                st.markdown(f"점수: **{top['점수']}점** {top.get('등급','')}")

        st.markdown("---")
        st.markdown("**점수 기준표**")
        st.markdown("""
| 점수 | 등급 |
|------|------|
| 8-10 | 🔴 CORE (강력매수) |
| 6-7.9 | 🟠 BUY NOW |
| 4-5.9 | 🟡 READY (관심) |
| 2-3.9 | ⚪ 관망 |
""")
        st.markdown("**테마 온도 기준**")
        st.markdown("""
| 점수 | 상태 |
|------|------|
| ≥8 | 🔥🔥 과열 |
| 6-7.9 | 🔥 활성 (최적진입) |
| 4-5.9 | 📈 형성중 |
| 2-3.9 | 👀 워밍업 |
| <2 | 💤 미활성 |
""")


# ────────────────────────────────────────────
# 종목 카드 렌더링
# ────────────────────────────────────────────
def render_stock_card(row: pd.Series):
    track = row.get("트랙", "READY")
    style = TRACK_STYLE.get(track, TRACK_STYLE["READY"])
    icon  = style["icon"]
    color = style["color"]

    name     = row.get("종목명", "")
    code     = row.get("코드", "")
    price    = row.get("현재가", 0)
    change   = row.get("등락률", 0)
    marcap   = row.get("시총(억)", 0)
    fin_sc   = row.get("재무점수", 0)
    theme_sc = row.get("테마점수", 0)
    theme_nm = row.get("테마명", "")
    reason   = row.get("선정이유", "")
    warnings = row.get("주의사항", "")
    manual   = row.get("수동확인항목", "")
    naver_url= row.get("네이버링크", f"https://finance.naver.com/item/main.nhn?code={code}")
    dart_url = row.get("DART링크", f"https://dart.fss.or.kr/dsab007/detailSearch.do?textCrpNm={name}")
    debt     = row.get("부채비율", "")
    reserve  = row.get("유보율", "")
    holder   = row.get("최대주주지분", "")
    cb_bw    = row.get("CB/BW여부", "N")
    pos52    = row.get("52주위치", "")

    change_color = "#ff4b4b" if float(change or 0) > 0 else "#4fc3f7"
    change_str   = f"+{change}%" if float(change or 0) > 0 else f"{change}%"
    cb_badge     = "⚠️CB/BW" if cb_bw == "Y" else ""

    with st.container():
        st.markdown(
            f"""
            <div style="border-left: 4px solid {color}; padding: 12px 16px;
                        background:#1e1e2e; border-radius:8px; margin-bottom:12px;">
                <div style="display:flex; justify-content:space-between; align-items:center;">
                    <span style="font-size:18px; font-weight:bold; color:{color};">
                        {icon} {name} <span style="font-size:13px; color:#888;">({code})</span>
                    </span>
                    <span style="font-size:16px; font-weight:bold; color:{change_color};">
                        {price:,}원 &nbsp; {change_str}
                    </span>
                </div>
                <div style="margin-top:6px; font-size:13px; color:#aaa;">
                    시총 {marcap}억 &nbsp;|&nbsp; 재무점수 <b style="color:#ffd700;">{fin_sc}점</b>
                    &nbsp;|&nbsp; 테마점수 <b style="color:#ffa500;">{theme_sc}점</b>
                    &nbsp;|&nbsp; 테마: {theme_nm}
                    &nbsp; {cb_badge}
                </div>
            </div>
            """,
            unsafe_allow_html=True
        )

        with st.expander(f"📋 {name} 상세 보기"):
            col1, col2 = st.columns(2)
            with col1:
                st.markdown("**자동 검증 항목**")
                st.markdown(f"- 부채비율: `{debt}%`")
                st.markdown(f"- 유보율: `{reserve}%`")
                st.markdown(f"- 최대주주지분: `{holder}%`")
                st.markdown(f"- 52주 위치: `{pos52}`")
                st.markdown(f"- CB/BW 여부: `{cb_bw}`")
            with col2:
                st.markdown("**선정 이유**")
                st.info(reason if reason else "이유 없음")

            if warnings:
                st.markdown("**⚠️ 주의사항**")
                for w in warnings.split(";"):
                    if w.strip():
                        st.warning(w.strip())

            if manual:
                st.markdown("**🔍 수동 확인 필요 항목**")
                for m in manual.split(";"):
                    if m.strip():
                        st.markdown(f"- [ ] {m.strip()}")

            col_a, col_b = st.columns(2)
            with col_a:
                st.link_button("📊 네이버 금융", naver_url)
            with col_b:
                st.link_button("📄 DART 공시", dart_url)


# ────────────────────────────────────────────
# 탭별 렌더링
# ────────────────────────────────────────────
def render_tab_stocks(today_df: pd.DataFrame, track: str):
    today = datetime.now().strftime("%Y-%m-%d")
    if today_df.empty:
        st.info("데이터가 없습니다. 파이프라인을 실행해 주세요.")
        return

    data = today_df[today_df["날짜"] == today] if "날짜" in today_df.columns else today_df
    data = data[data["트랙"] == track]

    if data.empty:
        st.info(f"오늘 {track} 종목이 없습니다.")
        return

    st.markdown(f"**총 {len(data)}종목**")
    for _, row in data.iterrows():
        render_stock_card(row)


def render_tab_theme(theme_df: pd.DataFrame):
    today = datetime.now().strftime("%Y-%m-%d")
    if theme_df.empty:
        st.info("테마 데이터가 없습니다.")
        return

    data = theme_df[theme_df["날짜"] == today] if "날짜" in theme_df.columns else theme_df
    data = data.sort_values("점수", ascending=False)

    st.markdown("### 오늘의 테마 온도")
    for _, row in data.iterrows():
        grade = row.get("등급", "")
        color = GRADE_COLOR.get(grade, "#888888")
        score = row.get("점수", 0)
        bar_w = int(float(score) * 10)

        st.markdown(
            f"""
            <div style="display:flex; align-items:center; margin-bottom:8px;">
                <div style="width:120px; font-weight:bold;">{row.get('테마명','')}</div>
                <div style="width:{bar_w}%; background:{color}; height:20px;
                            border-radius:4px; margin:0 10px;"></div>
                <div style="color:{color}; font-weight:bold;">{score}점 {grade}</div>
                <div style="color:#888; margin-left:12px; font-size:12px;">
                    뉴스 {row.get('뉴스건수',0)}건
                </div>
            </div>
            """,
            unsafe_allow_html=True
        )


def render_tab_history(history_df: pd.DataFrame):
    if history_df.empty:
        st.info("히스토리 데이터가 없습니다.")
        return

    st.markdown("### 종목 팔로우업 히스토리")
    df_show = history_df[[
        "종목명", "종목코드", "최초진입일", "최초트랙",
        "현재트랙", "최초가격", "현재가격", "수익률(%)", "최근업데이트"
    ]].copy()

    # 수익률 색상
    def highlight_ret(val):
        try:
            v = float(val)
            return "color: #ff4b4b;" if v > 0 else ("color: #4fc3f7;" if v < 0 else "")
        except Exception:
            return ""

    st.dataframe(
        df_show.style.applymap(highlight_ret, subset=["수익률(%)"]),
        use_container_width=True,
        height=500
    )


def render_tab_alerts(alerts_df: pd.DataFrame):
    if alerts_df.empty:
        st.info("알림 내역이 없습니다.")
        return

    st.markdown("### 알림 이력")
    st.dataframe(alerts_df.sort_values("일시", ascending=False), use_container_width=True)


# ────────────────────────────────────────────
# 메인
# ────────────────────────────────────────────
def main():
    # 데이터 로드
    today_df   = load_tab("TODAY")
    history_df = load_tab("HISTORY")
    alerts_df  = load_tab("ALERTS")
    theme_df   = load_tab("THEME")

    # 사이드바
    render_sidebar(today_df, theme_df)

    # 타이틀
    st.markdown(
        "<h1 style='color:#ffd700;'>📈 Andy Jo 주식 AI</h1>"
        "<p style='color:#888;'>시간여행TV 기준 소형주 자동선정 시스템</p>",
        unsafe_allow_html=True
    )

    # 새로고침
    if st.button("🔄 데이터 새로고침"):
        st.cache_data.clear()
        st.rerun()

    st.markdown("---")

    # 메인 탭
    tab1, tab2, tab3, tab4, tab5, tab6 = st.tabs([
        "🔴 CORE", "🟠 BUY NOW", "🟡 READY", "🚀 LAUNCHED", "🌡️ 테마온도", "📊 팔로우업"
    ])

    with tab1:
        st.markdown("### 🔴 CORE — 핵심 매수 종목")
        st.caption("재무 + 테마 + 타이밍 모두 최상위 조건 충족")
        render_tab_stocks(today_df, "CORE")

    with tab2:
        st.markdown("### 🟠 BUY NOW — 즉시 매수 검토")
        st.caption("테마 뉴스 3건 이상 + 거래량 200% 이상")
        render_tab_stocks(today_df, "BUY_NOW")

    with tab3:
        st.markdown("### 🟡 READY — 관심 종목 (예비 대기)")
        st.caption("기본 조건 통과, 진입 타이밍 대기 중")
        render_tab_stocks(today_df, "READY")

    with tab4:
        st.markdown("### 🚀 LAUNCHED — 이미 급등 종목")
        st.caption("⚠️ 추격매수 위험. 참고용으로만 활용하세요.")
        render_tab_stocks(today_df, "LAUNCHED")

    with tab5:
        render_tab_theme(theme_df)

    with tab6:
        col_h, col_a = st.columns([3, 1])
        with col_h:
            render_tab_history(history_df)
        with col_a:
            render_tab_alerts(alerts_df)


if __name__ == "__main__":
    main()
