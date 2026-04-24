"""
Andy Jo Stock AI — Streamlit 대시보드
5탭 구성: 오늘의 테마 / BUY NOW / READY / LAUNCHED / 팔로우업
"""

import streamlit as st
import pandas as pd
from datetime import datetime

# ────────────────────────────────────────
# 페이지 기본 설정
# ────────────────────────────────────────
st.set_page_config(
    page_title="Andy Jo Stock AI",
    page_icon="📈",
    layout="wide",
    initial_sidebar_state="collapsed"
)

# ────────────────────────────────────────
# 스타일 (다크 테마 강화)
# ────────────────────────────────────────
st.markdown("""
<style>
    .main-header {
        font-size: 2rem;
        font-weight: 800;
        color: #00D4AA;
        margin-bottom: 0.2rem;
    }
    .sub-header {
        font-size: 0.9rem;
        color: #888888;
        margin-bottom: 1.5rem;
    }
    .metric-card {
        background: #1E1E2E;
        border-radius: 12px;
        padding: 1rem 1.5rem;
        border-left: 4px solid;
        margin-bottom: 0.5rem;
    }
    .card-ready    { border-color: #FFD700; }
    .card-buynow   { border-color: #00FF88; }
    .card-launched { border-color: #FF6B6B; }
    .track-badge-READY    { background:#FFD700; color:#000; padding:2px 8px; border-radius:4px; font-size:0.75rem; font-weight:700; }
    .track-badge-BUY_NOW  { background:#00FF88; color:#000; padding:2px 8px; border-radius:4px; font-size:0.75rem; font-weight:700; }
    .track-badge-LAUNCHED { background:#FF6B6B; color:#fff; padding:2px 8px; border-radius:4px; font-size:0.75rem; font-weight:700; }
    .theme-bar {
        background: #2A2A3E;
        border-radius: 8px;
        padding: 0.7rem 1rem;
        margin-bottom: 0.4rem;
        display: flex;
        justify-content: space-between;
        align-items: center;
    }
    .score-fill {
        height: 8px;
        border-radius: 4px;
        background: linear-gradient(90deg, #00D4AA, #00FF88);
    }
    .stTabs [data-baseweb="tab"] {
        font-size: 1rem;
        font-weight: 600;
    }
</style>
""", unsafe_allow_html=True)


# ────────────────────────────────────────
# 데이터 로드 (캐시 10분)
# ────────────────────────────────────────
@st.cache_data(ttl=600)
def load_data():
    try:
        from data_store import (
            load_daily_snapshot,
            load_stock_history,
            load_theme_daily
        )
        snapshot = load_daily_snapshot()
        history  = load_stock_history()
        themes   = load_theme_daily()
        return snapshot, history, themes
    except Exception as e:
        st.error(f"데이터 로드 실패: {e}")
        return [], [], []


def get_today_data(snapshot: list) -> dict:
    """오늘 날짜 데이터만 필터링 → 3트랙 분리"""
    today = datetime.now().strftime("%Y-%m-%d")
    today_rows = [r for r in snapshot if str(r.get("날짜", "")) == today]

    result = {"READY": [], "BUY_NOW": [], "LAUNCHED": []}
    for row in today_rows:
        track = row.get("트랙", "")
        if track in result:
            result[track].append(row)

    # 점수 기준 정렬
    for track in result:
        result[track].sort(key=lambda x: -float(x.get("총점", 0) or 0))

    return result


def get_today_themes(themes: list) -> list:
    """오늘 테마 데이터 정렬"""
    today = datetime.now().strftime("%Y-%m-%d")
    today_themes = [t for t in themes if str(t.get("날짜", "")) == today]
    today_themes.sort(key=lambda x: -float(x.get("온도점수", 0) or 0))
    return today_themes


# ────────────────────────────────────────
# 헤더
# ────────────────────────────────────────
def render_header(today_data: dict):
    col1, col2 = st.columns([3, 1])
    with col1:
        st.markdown('<div class="main-header">📈 Andy Jo Stock AI</div>', unsafe_allow_html=True)
        st.markdown(
            f'<div class="sub-header">시간여행TV 기준 자동 스크리닝 | '
            f'업데이트: {datetime.now().strftime("%Y-%m-%d %H:%M")}</div>',
            unsafe_allow_html=True
        )
    with col2:
        r = len(today_data.get("READY", []))
        b = len(today_data.get("BUY_NOW", []))
        l = len(today_data.get("LAUNCHED", []))
        st.markdown(f"""
        <div style="text-align:right; padding-top:0.5rem;">
            <span style="color:#FFD700; font-weight:700;">🟡 READY {r}</span>&nbsp;&nbsp;
            <span style="color:#00FF88; font-weight:700;">🟢 BUY NOW {b}</span>&nbsp;&nbsp;
            <span style="color:#FF6B6B; font-weight:700;">🚀 LAUNCHED {l}</span>
        </div>
        """, unsafe_allow_html=True)


# ────────────────────────────────────────
# 탭 1: 오늘의 테마
# ────────────────────────────────────────
def render_theme_tab(today_themes: list):
    st.subheader("🌡️ 오늘의 테마 온도 TOP 10")

    if not today_themes:
        st.info("오늘 테마 데이터가 없습니다. 파이프라인 실행 후 확인해주세요.")
        return

    top10 = today_themes[:10]

    for i, theme in enumerate(top10, 1):
        name  = theme.get("테마명", "")
        score = float(theme.get("온도점수", 0) or 0)
        news  = float(theme.get("뉴스신호", 0) or 0)
        vol   = float(theme.get("거래량신호", 0) or 0)
        price = float(theme.get("주가신호", 0) or 0)
        comm  = float(theme.get("커뮤니티신호", 0) or 0)

        # 온도 색상
        if score >= 7:   color = "#00FF88"
        elif score >= 4: color = "#FFD700"
        else:            color = "#888888"

        with st.container():
            col1, col2, col3, col4, col5, col6 = st.columns([0.3, 1.5, 1, 1, 1, 1])
            col1.markdown(f"**{i}위**")
            col2.markdown(f"**{name}**")
            col3.metric("온도", f"{score:.1f}점", delta=None)
            col4.metric("뉴스", f"{news:.1f}")
            col5.metric("거래량", f"{vol:.1f}")
            col6.metric("커뮤니티", f"{comm:.1f}")

        # 온도 게이지 바
        bar_width = min(int(score * 10), 100)
        st.markdown(f"""
        <div style="background:#2A2A3E; border-radius:4px; height:6px; margin-bottom:0.8rem;">
            <div style="width:{bar_width}%; height:6px; border-radius:4px;
                        background:linear-gradient(90deg, {color}, {color}88);"></div>
        </div>
        """, unsafe_allow_html=True)

    st.divider()

    # 테마 전체 표
    if len(today_themes) > 10:
        with st.expander(f"전체 테마 보기 ({len(today_themes)}개)"):
            df = pd.DataFrame(today_themes)[["테마명", "온도점수", "뉴스신호", "거래량신호", "주가신호", "커뮤니티신호", "랭킹"]]
            st.dataframe(df, use_container_width=True)


# ────────────────────────────────────────
# 탭 2: BUY NOW
# ────────────────────────────────────────
def render_buynow_tab(buy_now: list):
    st.subheader("🟢 BUY NOW — 지금 바로 볼 종목")
    st.caption("L0-L6 전부 통과 + 테마 점수 높음 + 거래량 급등 + 가격 타이밍 최적")

    if not buy_now:
        st.info("오늘 BUY NOW 종목이 없습니다.")
        return

    for s in buy_now[:20]:
        code   = s.get("종목코드", "")
        name   = s.get("종목명", "")
        score  = s.get("총점", 0)
        themes = s.get("테마", "")
        mktcap = s.get("시가총액(억)", "")
        vol_r  = s.get("거래량비율", "")
        price  = s.get("현재가", "")
        low52  = s.get("52주저점", "")

        with st.expander(f"🟢 {name} ({code}) — 총점 {score}점 | {themes}"):
            col1, col2, col3, col4 = st.columns(4)
            col1.metric("시가총액", f"{mktcap}억")
            col2.metric("현재가", f"{price}원")
            col3.metric("52주 저점", f"{low52}원")
            col4.metric("거래량비율", f"{vol_r}배")

            st.markdown(f"""
            - **테마:** {themes}
            - **총점:** {score}점
            - **네이버 증권:** [바로가기](https://finance.naver.com/item/main.naver?code={code})
            """)


# ────────────────────────────────────────
# 탭 3: READY
# ────────────────────────────────────────
def render_ready_tab(ready: list):
    st.subheader("🟡 READY — 미리 담아두는 준비 종목")
    st.caption("재무·주주구조 우수, 아직 테마 미반영 — 테마 붙으면 BUY NOW로 자동 전환")

    if not ready:
        st.info("오늘 READY 종목이 없습니다.")
        return

    # 표 형식으로 표시
    df_data = []
    for s in ready[:50]:
        df_data.append({
            "종목코드": s.get("종목코드", ""),
            "종목명":   s.get("종목명", ""),
            "총점":     s.get("총점", 0),
            "시가총액(억)": s.get("시가총액(억)", ""),
            "테마":     s.get("테마", "없음"),
            "현재가":   s.get("현재가", ""),
            "52주저점": s.get("52주저점", ""),
            "거래량비율": s.get("거래량비율", ""),
        })

    df = pd.DataFrame(df_data)
    st.dataframe(
        df,
        use_container_width=True,
        column_config={
            "종목코드": st.column_config.TextColumn("코드", width=70),
            "종목명":   st.column_config.TextColumn("종목명", width=100),
            "총점":     st.column_config.NumberColumn("총점", format="%.1f"),
            "시가총액(억)": st.column_config.NumberColumn("시총(억)", format="%.0f"),
            "테마":     st.column_config.TextColumn("테마", width=150),
            "현재가":   st.column_config.NumberColumn("현재가", format="%.0f"),
            "52주저점": st.column_config.NumberColumn("52주저점", format="%.0f"),
            "거래량비율": st.column_config.NumberColumn("거래량비율", format="%.2f"),
        }
    )


# ────────────────────────────────────────
# 탭 4: LAUNCHED
# ────────────────────────────────────────
def render_launched_tab(launched: list):
    st.subheader("🚀 LAUNCHED — 이미 출발한 종목")
    st.caption("20일 수익률 30% 이상 — 놓친 종목 기록 / 다음 기회 참고용")

    if not launched:
        st.info("오늘 LAUNCHED 종목이 없습니다.")
        return

    df_data = []
    for s in launched[:50]:
        gain = s.get("20일수익률", 0)
        try:
            gain_f = float(gain)
        except Exception:
            gain_f = 0

        df_data.append({
            "종목코드":   s.get("종목코드", ""),
            "종목명":     s.get("종목명", ""),
            "20일수익률": gain_f,
            "테마":       s.get("테마", ""),
            "시가총액(억)": s.get("시가총액(억)", ""),
        })

    df = pd.DataFrame(df_data)
    df = df.sort_values("20일수익률", ascending=False)

    st.dataframe(
        df,
        use_container_width=True,
        column_config={
            "20일수익률": st.column_config.NumberColumn(
                "20일 수익률(%)",
                format="%.1f%%",
            ),
        }
    )


# ────────────────────────────────────────
# 탭 5: 팔로우업
# ────────────────────────────────────────
def render_followup_tab(history: list):
    st.subheader("📋 팔로우업 — 종목 이력 카드")
    st.caption("최초 포착일부터 현재까지 트랙 변경 이력 전체 기록")

    if not history:
        st.info("이력 데이터가 없습니다.")
        return

    # 검색 필터
    search = st.text_input("종목명 또는 코드 검색", placeholder="예: 한화에어로, 012450")

    filtered = history
    if search:
        filtered = [
            h for h in history
            if search in str(h.get("종목명", "")) or search in str(h.get("종목코드", ""))
        ]

    st.caption(f"총 {len(filtered)}종목 이력")

    for h in filtered[:30]:
        code    = h.get("종목코드", "")
        name    = h.get("종목명", "")
        first   = h.get("최초진입일", "")
        track   = h.get("현재트랙", "")
        history_log = h.get("트랙변경이력", "")
        best_score  = h.get("최고점수", "")
        buy_date    = h.get("BUY_NOW전환일", "")
        launch_date = h.get("LAUNCHED전환일", "")

        badge_color = {
            "READY": "#FFD700",
            "BUY_NOW": "#00FF88",
            "LAUNCHED": "#FF6B6B"
        }.get(track, "#888888")

        with st.expander(f"{name} ({code}) — 최초 {first} 진입 | 현재: {track}"):
            col1, col2, col3 = st.columns(3)
            col1.metric("최초 진입일", first)
            col2.metric("최고 점수", best_score)
            col3.metric("현재 트랙", track)

            st.markdown(f"**트랙 변경 이력:** {history_log}")

            timeline = []
            if first:        timeline.append(f"📌 {first} 최초 진입")
            if buy_date:     timeline.append(f"🟢 {buy_date} BUY NOW 전환")
            if launch_date:  timeline.append(f"🚀 {launch_date} LAUNCHED 전환")

            for t in timeline:
                st.markdown(f"- {t}")

            st.markdown(
                f"[네이버 증권 바로가기](https://finance.naver.com/item/main.naver?code={code})"
            )


# ────────────────────────────────────────
# 메인 실행
# ────────────────────────────────────────
def main():
    snapshot, history, themes = load_data()
    today_data   = get_today_data(snapshot)
    today_themes = get_today_themes(themes)

    render_header(today_data)

    tab1, tab2, tab3, tab4, tab5 = st.tabs([
        "🌡️ 오늘의 테마",
        "🟢 BUY NOW",
        "🟡 READY",
        "🚀 LAUNCHED",
        "📋 팔로우업"
    ])

    with tab1:
        render_theme_tab(today_themes)
    with tab2:
        render_buynow_tab(today_data.get("BUY_NOW", []))
    with tab3:
        render_ready_tab(today_data.get("READY", []))
    with tab4:
        render_launched_tab(today_data.get("LAUNCHED", []))
    with tab5:
        render_followup_tab(history)

    # 새로고침 버튼
    st.divider()
    col1, col2, col3 = st.columns([1, 1, 1])
    with col2:
        if st.button("🔄 데이터 새로고침", use_container_width=True):
            st.cache_data.clear()
            st.rerun()


if __name__ == "__main__":
    main()
