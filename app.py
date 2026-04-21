import streamlit as st
import pandas as pd
import plotly.graph_objects as go
import plotly.express as px
import gspread
from google.oauth2.service_account import Credentials
import json, os
from datetime import datetime

st.set_page_config(
    page_title="Time-Travel TV 주식 스크리너",
    page_icon="📡",
    layout="wide",
    initial_sidebar_state="expanded"
)

SHEETS_ID = "1OqRboKwx7X0-3W67_raZyV2ZjcQ_G7ylHilf7SgAU88"

# ───────────────────────────────────────────────
# Google Sheets에서 데이터 로드
# ───────────────────────────────────────────────
@st.cache_data(ttl=300)
def load_data():
    try:
        creds_raw = st.secrets.get("GOOGLE_CREDENTIALS") or os.environ.get("GOOGLE_CREDENTIALS")
        if not creds_raw:
            return pd.DataFrame(), "GOOGLE_CREDENTIALS 없음"
        if isinstance(creds_raw, str):
            creds_dict = json.loads(creds_raw)
        else:
            creds_dict = dict(creds_raw)
        scopes = ["https://spreadsheets.google.com/feeds",
                  "https://www.googleapis.com/auth/drive"]
        creds = Credentials.from_service_account_info(creds_dict, scopes=scopes)
        gc    = gspread.authorize(creds)
        sh    = gc.open_by_key(SHEETS_ID)
        ws    = sh.sheet1
        data  = ws.get_all_records()
        df    = pd.DataFrame(data)
        return df, "ok"
    except Exception as e:
        return pd.DataFrame(), str(e)

# ───────────────────────────────────────────────
# 사이드바
# ───────────────────────────────────────────────
with st.sidebar:
    st.title("⚙️ 필터 설정")
    grade_options = ["🌟핵심후보","⭐우선후보","관심후보","📊참고"]
    selected_grades = st.multiselect("등급 선택", grade_options,
                                     default=["🌟핵심후보","⭐우선후보"])
    min_score = st.slider("최소 종합점수", 0, 25, 6)
    only_timing = st.checkbox("L6 타이밍 종목만")
    st.divider()
    if st.button("🔄 데이터 새로고침", use_container_width=True):
        st.cache_data.clear()
        st.rerun()
    st.divider()
    st.caption("📅 분석: 매일 새벽 2시 자동 실행")
    st.caption("🔗 DART + FinanceDataReader 연동")
    st.divider()
    st.markdown("**점수 기준:**")
    st.markdown("- 🌟 핵심: 16점 이상")
    st.markdown("- ⭐ 우선: 11-15점")
    st.markdown("- 관심: 6-10점")
    st.markdown("- 📊 참고: 5점 이하")

# ───────────────────────────────────────────────
# 메인 헤더
# ───────────────────────────────────────────────
st.title("📡 Time-Travel TV 주식 스크리너")

df, status = load_data()

if status != "ok":
    st.warning(f"⚠️ 데이터 로드 오류: {status}")
    st.info("GitHub Actions가 아직 분석을 실행하지 않았거나 Google Sheets 연동을 확인해주세요.")
    st.stop()

if df.empty:
    st.info("📊 아직 분석 결과가 없습니다. GitHub Actions 첫 실행을 기다려주세요.")
    st.stop()

# 업데이트 시간
update_time = df["분석일"].iloc[0] if "분석일" in df.columns else "알 수 없음"
total_count = len(df)

col1, col2, col3, col4 = st.columns(4)
col1.metric("마지막 업데이트", update_time)
col2.metric("전체 후보", f"{total_count}개")
col3.metric("핵심후보", f"{len(df[df['등급']=='🌟핵심후보'])}개")
col4.metric("L6 타이밍", f"{len(df[df['L6타이밍']=='✅'])}개")

st.divider()

# 필터 적용
filtered = df.copy()
if selected_grades:
    filtered = filtered[filtered["등급"].isin(selected_grades)]
filtered = filtered[filtered["종합점수"] >= min_score]
if only_timing:
    filtered = filtered[filtered["L6타이밍"] == "✅"]

# ───────────────────────────────────────────────
# 탭 구성
# ───────────────────────────────────────────────
tab1, tab2, tab3, tab4 = st.tabs(["🏆 오늘의 후보","🔍 종목 상세","📡 매집 레이더","⏰ 시장 맥락"])

# ── 탭1: 오늘의 후보 ──
with tab1:
    st.subheader(f"필터 결과: {len(filtered)}개 종목")
    if filtered.empty:
        st.info("필터 조건에 맞는 종목이 없습니다.")
    else:
        show_cols = ["등급","종목명","종목코드","시장","시총(억)","종합점수",
                     "재무점수","매집점수","테마점수","테마","L6타이밍"]
        show_cols = [c for c in show_cols if c in filtered.columns]
        st.dataframe(
            filtered[show_cols].reset_index(drop=True),
            use_container_width=True,
            height=500
        )
        csv = filtered.to_csv(index=False, encoding="utf-8-sig")
        st.download_button("📥 CSV 다운로드", csv, "stock_result.csv", "text/csv")

# ── 탭2: 종목 상세 ──
with tab2:
    if filtered.empty:
        st.info("필터 조건에 맞는 종목이 없습니다.")
    else:
        names = filtered["종목명"].tolist()
        sel = st.selectbox("종목 선택", names)
        row = filtered[filtered["종목명"] == sel].iloc[0]

        c1, c2, c3 = st.columns(3)
        c1.metric("종합점수", row.get("종합점수","N/A"))
        c2.metric("등급", row.get("등급","N/A"))
        c3.metric("L6 타이밍", row.get("L6타이밍","N/A"))

        st.divider()
        col_a, col_b = st.columns(2)
        with col_a:
            st.markdown("**📊 재무 분석**")
            st.info(row.get("재무상세","정보없음"))
        with col_b:
            st.markdown("**📡 매집 패턴**")
            st.info(row.get("매집상세","정보없음"))

        st.markdown("**🏷️ 테마**")
        themes = str(row.get("테마","")).split(", ")
        for t in themes:
            if t:
                st.badge(t)

        st.markdown("**👥 주주구조**")
        st.write(row.get("주주구조","정보없음"))

        naver_url = f"https://finance.naver.com/item/main.naver?code={row['종목코드']}"
        st.link_button("🔗 네이버 증권에서 보기", naver_url)

# ── 탭3: 매집 레이더 ──
with tab3:
    st.subheader("📡 세력 매집 레이더")
    if filtered.empty:
        st.info("데이터 없음")
    else:
        plot_df = filtered[["종목명","매집점수","재무점수","테마점수","종합점수"]].head(20)
        fig = px.scatter(
            plot_df,
            x="재무점수", y="매집점수",
            size="종합점수", color="테마점수",
            hover_name="종목명",
            color_continuous_scale="Viridis",
            title="매집점수 vs 재무점수 (버블 크기=종합점수, 색상=테마점수)"
        )
        fig.update_layout(template="plotly_dark", height=500)
        st.plotly_chart(fig, use_container_width=True)

        st.subheader("🏆 매집 Top 10")
        top10 = filtered.nlargest(10,"매집점수")[["종목명","매집점수","종합점수","등급","테마"]]
        st.dataframe(top10.reset_index(drop=True), use_container_width=True)

# ── 탭4: 시장 맥락 ──
with tab4:
    st.subheader("⏰ 시장 맥락 & 매수 타이밍")

    try:
        import FinanceDataReader as fdr
        from datetime import timedelta
        today = datetime.today()

        col1, col2 = st.columns(2)
        with col1:
            kosdaq = fdr.DataReader("KQ11",
                (today - timedelta(days=30)).strftime('%Y-%m-%d'),
                today.strftime('%Y-%m-%d'))
            if not kosdaq.empty:
                fig_kq = go.Figure(go.Scatter(
                    x=kosdaq.index, y=kosdaq["Close"],
                    mode="lines", name="KOSDAQ",
                    line=dict(color="#00d4aa", width=2)
                ))
                fig_kq.update_layout(title="KOSDAQ 30일", template="plotly_dark", height=300)
                st.plotly_chart(fig_kq, use_container_width=True)

        with col2:
            kospi = fdr.DataReader("KS11",
                (today - timedelta(days=30)).strftime('%Y-%m-%d'),
                today.strftime('%Y-%m-%d'))
            if not kospi.empty:
                fig_ks = go.Figure(go.Scatter(
                    x=kospi.index, y=kospi["Close"],
                    mode="lines", name="KOSPI",
                    line=dict(color="#ff6b6b", width=2)
                ))
                fig_ks.update_layout(title="KOSPI 30일", template="plotly_dark", height=300)
                st.plotly_chart(fig_ks, use_container_width=True)
    except Exception as e:
        st.warning(f"시장 데이터 로드 실패: {e}")

    st.divider()
    st.subheader("🚦 L6 타이밍 신호")
    timing_ok = df[df["L6타이밍"] == "✅"] if not df.empty else pd.DataFrame()
    if not timing_ok.empty:
        st.success(f"✅ 현재 {len(timing_ok)}개 종목이 매수 타이밍 조건 충족")
        st.dataframe(timing_ok[["종목명","종합점수","등급","테마"]].reset_index(drop=True),
                     use_container_width=True)
    else:
        st.warning("⏳ 현재 매수 타이밍 조건 충족 종목 없음")
