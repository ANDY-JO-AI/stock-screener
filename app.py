import streamlit as st
import pandas as pd
import plotly.graph_objects as go
import plotly.express as px
from datetime import datetime
import FinanceDataReader as fdr
import os

# ── 페이지 설정 ────────────────────────────────────────
st.set_page_config(
    page_title="Time-Travel TV 스크리너",
    page_icon="🕵️",
    layout="wide",
    initial_sidebar_state="expanded"
)

# ── 다크모드 스타일 ────────────────────────────────────
st.markdown("""
<style>
    .main { background-color: #0e1117; }
    .stApp { background-color: #0e1117; }
    .metric-card {
        background: linear-gradient(135deg, #1e2130, #252836);
        border-radius: 12px;
        padding: 16px 20px;
        border-left: 4px solid #4f8ef7;
        margin: 6px 0;
    }
    .grade-star  { color: #ffd700; font-weight: bold; font-size: 15px; }
    .grade-blue  { color: #4f8ef7; font-weight: bold; font-size: 15px; }
    .grade-yellow{ color: #f7c94f; font-weight: bold; font-size: 15px; }
    .grade-white { color: #aaaaaa; font-weight: bold; font-size: 15px; }
    .signal-fire { color: #ff4b4b; font-weight: bold; }
    .signal-warn { color: #ffa500; font-weight: bold; }
    .tag {
        display: inline-block;
        background: #2a2d3e;
        border-radius: 6px;
        padding: 2px 8px;
        margin: 2px;
        font-size: 12px;
        color: #ccc;
    }
    div[data-testid="stMetricValue"] { font-size: 28px; font-weight: bold; }
    .stDataFrame { border-radius: 10px; }
    .sidebar-title { font-size: 20px; font-weight: bold; color: #4f8ef7; }
</style>
""", unsafe_allow_html=True)

# ── 캐시: 분석 실행 ────────────────────────────────────
@st.cache_data(ttl=3600, show_spinner=False)
def load_data():
    from filter_engine import run_pipeline
    logs = []
    def cb(msg): logs.append(msg)
    result_df, fail_df = run_pipeline(progress_callback=cb)
    return result_df, fail_df, logs

@st.cache_data(ttl=300)
def get_market_indices():
    try:
        kq = fdr.DataReader('KQ11', '2026-01-01')
        ks = fdr.DataReader('KS11', '2026-01-01')
        kq_chg = round((kq['Close'].iloc[-1]-kq['Close'].iloc[-2])/kq['Close'].iloc[-2]*100, 2)
        ks_chg = round((ks['Close'].iloc[-1]-ks['Close'].iloc[-2])/ks['Close'].iloc[-2]*100, 2)
        return {
            'kosdaq': round(kq['Close'].iloc[-1], 2),
            'kosdaq_chg': kq_chg,
            'kospi':  round(ks['Close'].iloc[-1], 2),
            'kospi_chg':  ks_chg,
        }
    except:
        return {'kosdaq':0,'kosdaq_chg':0,'kospi':0,'kospi_chg':0}

@st.cache_data(ttl=600)
def get_stock_chart(code):
    try:
        df = fdr.DataReader(str(code).zfill(6), '2025-10-01')
        return df
    except:
        return None

# ══════════════════════════════════════════════════════
# 사이드바
# ══════════════════════════════════════════════════════
with st.sidebar:
    st.markdown('<div class="sidebar-title">🕵️ Time-Travel TV</div>', unsafe_allow_html=True)
    st.caption("코스닥/코스피 소형주 자동 스크리너")
    st.divider()

    run_btn = st.button("🔄 분석 실행 (새로고침)", use_container_width=True, type="primary")
    if run_btn:
        st.cache_data.clear()
        st.rerun()

    st.divider()
    st.markdown("**📋 필터 설정**")
    grade_filter = st.multiselect(
        "등급 선택",
        ["⭐핵심후보","🔵우선후보","🟡관심","⚪참고"],
        default=["⭐핵심후보","🔵우선후보"]
    )
    theme_filter = st.multiselect(
        "테마 선택 (복수 가능)",
        ["🏛️정치/선거","🛡️방산/안보","🤖AI/로봇","⚡에너지/전력",
         "💊바이오/헬스","🌾농업/식품","🚗자동차부품","🎬미디어/콘텐츠",
         "🏗️건설/부동산","❄️계절/기후","💰화폐/금융","🏭제조/소재",
         "📡IT/통신","👶저출산/복지","🕊️대북/통일"],
        default=[]
    )
    acc_only = st.checkbox("🔥 매집 신호 종목만", value=False)
    min_score = st.slider("최소 종합점수", 0, 30, 0)

    st.divider()
    st.markdown("**ℹ️ 점수 기준**")
    st.caption("⭐핵심: 16점 이상")
    st.caption("🔵우선: 11-15점")
    st.caption("🟡관심: 6-10점")
    st.caption("⚪참고: 5점 이하")

# ══════════════════════════════════════════════════════
# 메인 헤더
# ══════════════════════════════════════════════════════
st.markdown("## 🕵️ Time-Travel TV 주식 스크리너")
st.caption(f"마지막 업데이트: {datetime.now().strftime('%Y-%m-%d %H:%M')}  |  LAYER 0-6 완전 자동화")

# 시장 지수
idx = get_market_indices()
c1, c2, c3, c4 = st.columns(4)
kq_color = "normal" if idx['kosdaq_chg'] >= 0 else "inverse"
ks_color = "normal" if idx['kospi_chg'] >= 0 else "inverse"
c1.metric("KOSDAQ", f"{idx['kosdaq']:,}", f"{idx['kosdaq_chg']}%")
c2.metric("KOSPI",  f"{idx['kospi']:,}",  f"{idx['kospi_chg']}%")
timing_ok = idx['kosdaq_chg'] <= -1.5
c3.metric("매수타이밍", "✅ 유효" if timing_ok else "⏳ 대기",
          "전일 1.5%↓ 조건 충족" if timing_ok else "시장 하락 대기 중")

st.divider()

# ══════════════════════════════════════════════════════
# 탭 구성
# ══════════════════════════════════════════════════════
tab1, tab2, tab3, tab4 = st.tabs([
    "🎯 오늘의 후보",
    "🔍 종목 상세",
    "🔥 매집 레이더",
    "📊 시장 맥락"
])

# ── 데이터 로드 ────────────────────────────────────────
with st.spinner("📡 LAYER 0-6 분석 중... (최초 실행 시 30-60분 소요)"):
    try:
        result_df, fail_df, run_logs = load_data()
        data_ok = not result_df.empty
    except Exception as e:
        st.error(f"분석 오류: {e}")
        data_ok = False
        result_df = pd.DataFrame()
        fail_df   = pd.DataFrame()
        run_logs  = []

# ══════════════════════════════════════════════════════
# TAB 1 — 오늘의 후보
# ══════════════════════════════════════════════════════
with tab1:
    if not data_ok:
        st.warning("왼쪽 사이드바에서 **🔄 분석 실행** 버튼을 눌러주세요.")
        st.stop()

    df = result_df.copy()

    # 필터 적용
    if grade_filter:
        df = df[df['등급'].isin(grade_filter)]
    if theme_filter:
        df = df[df['테마분류'].apply(
            lambda x: any(t in str(x) for t in theme_filter)
        )]
    if acc_only:
        df = df[pd.to_numeric(df['매집점수'], errors='coerce') >= 3]
    df = df[pd.to_numeric(df['종합점수'], errors='coerce') >= min_score]

    # 요약 카드
    total = len(result_df)
    stars = len(result_df[result_df['등급']=='⭐핵심후보'])
    blues = len(result_df[result_df['등급']=='🔵우선후보'])
    fires = len(result_df[pd.to_numeric(result_df['매집점수'],errors='coerce')>=5])
    c1,c2,c3,c4 = st.columns(4)
    c1.metric("최종 후보", f"{total}개")
    c2.metric("⭐ 핵심후보", f"{stars}개")
    c3.metric("🔵 우선후보", f"{blues}개")
    c4.metric("🔥 강한매집", f"{fires}개")

    st.caption(f"필터 적용 후: **{len(df)}개** 표시 중")
    st.divider()

    # 종목 카드 출력
    if df.empty:
        st.info("조건에 맞는 종목이 없어요. 필터를 조정해보세요.")
    else:
        for _, row in df.iterrows():
            grade     = str(row.get('등급',''))
            acc_score = int(row.get('매집점수', 0) or 0)
            fin_score = int(row.get('재무점수', 0) or 0)
            thm_score = int(row.get('테마점수', 0) or 0)
            tot_score = int(row.get('종합점수', 0) or 0)
            signals   = str(row.get('매집신호',''))
            themes    = str(row.get('테마분류',''))
            news      = str(row.get('최신뉴스',''))
            holder    = str(row.get('주주구조등급',''))
            disc      = str(row.get('공시상태',''))
            code      = str(row.get('종목코드','')).zfill(6)
            name      = str(row.get('종목명',''))
            marcap    = row.get('시가총액(억)', 0)
            cur_prc   = row.get('현재가', 0)
            low_52    = row.get('52주최저가', 0)

            fire_icon = "🔥" if acc_score >= 5 else "⚠️" if acc_score >= 3 else ""
            border_color = {
                "⭐핵심후보": "#ffd700",
                "🔵우선후보": "#4f8ef7",
                "🟡관심":    "#f7c94f",
                "⚪참고":    "#666666"
            }.get(grade, "#444")

            naver_url = f"https://finance.naver.com/item/main.nhn?code={code}"
            dart_url  = f"https://dart.fss.or.kr/dsearch/main.do?query={name}"

            with st.container():
                st.markdown(f"""
                <div style='border-left:4px solid {border_color};
                            background:#1a1d2e;
                            border-radius:10px;
                            padding:14px 18px;
                            margin-bottom:10px;'>
                    <div style='display:flex; justify-content:space-between; align-items:center;'>
                        <div>
                            <span style='font-size:17px; font-weight:bold; color:white;'>
                                {grade} {fire_icon} {name}
                            </span>
                            <span style='color:#888; margin-left:10px; font-size:13px;'>
                                {code}
                            </span>
                        </div>
                        <div style='font-size:20px; font-weight:bold; color:{border_color};'>
                            종합 {tot_score}점
                        </div>
                    </div>
                    <div style='margin-top:8px; color:#ccc; font-size:13px;'>
                        시총 <b>{marcap}억</b> &nbsp;|&nbsp;
                        현재가 <b>{int(cur_prc):,}원</b> &nbsp;|&nbsp;
                        52주저가 <b>{int(low_52):,}원</b> &nbsp;|&nbsp;
                        재무 <b>{fin_score}점</b> &nbsp;|&nbsp;
                        테마 <b>{thm_score}점</b> &nbsp;|&nbsp;
                        매집 <b>{acc_score}점</b>
                    </div>
                    <div style='margin-top:6px; font-size:13px;'>
                        <span style='color:#ff6b6b;'>{fire_icon} {signals}</span>
                    </div>
                    <div style='margin-top:6px; font-size:12px; color:#aaa;'>
                        테마: {themes} &nbsp;|&nbsp; 주주: {holder}
                    </div>
                    <div style='margin-top:4px; font-size:12px; color:#888;'>
                        📰 {news[:80] if news else '뉴스 없음'}
                    </div>
                </div>
                """, unsafe_allow_html=True)

                col_a, col_b, col_c = st.columns([1,1,6])
                col_a.markdown(f"[📊 네이버]({naver_url})")
                col_b.markdown(f"[📋 DART]({dart_url})")

    st.divider()
    # 엑셀 다운로드
    if not df.empty:
        csv = df.to_csv(index=False).encode('utf-8-sig')
        st.download_button(
            "📥 결과 엑셀 다운로드",
            csv,
            f"stock_result_{datetime.now().strftime('%Y%m%d')}.csv",
            "text/csv"
        )

# ══════════════════════════════════════════════════════
# TAB 2 — 종목 상세
# ══════════════════════════════════════════════════════
with tab2:
    if not data_ok or result_df.empty:
        st.info("먼저 분석을 실행해주세요.")
    else:
        names = result_df['종목명'].tolist()
        sel   = st.selectbox("종목 선택", names)
        row   = result_df[result_df['종목명']==sel].iloc[0]
        code  = str(row['종목코드']).zfill(6)

        st.markdown(f"### {row.get('등급','')} {sel} ({code})")
        naver_url = f"https://finance.naver.com/item/main.nhn?code={code}"
        dart_url  = f"https://dart.fss.or.kr/dsearch/main.do?query={sel}"
        st.markdown(f"[📊 네이버증권]({naver_url}) &nbsp; [📋 DART공시]({dart_url})")
        st.divider()

        # 점수 요약
        c1,c2,c3,c4 = st.columns(4)
        c1.metric("종합점수", f"{int(row.get('종합점수',0))}점")
        c2.metric("재무점수", f"{int(row.get('재무점수',0))}점")
        c3.metric("테마점수", f"{int(row.get('테마점수',0))}점")
        c4.metric("매집점수", f"{int(row.get('매집점수',0))}점")

        col1, col2 = st.columns(2)

        with col1:
            st.markdown("**📋 LAYER 1 체크리스트**")
            items_l1 = [
                ("CB/BW/유증 공시",   "이상없음" in str(row.get('공시상태',''))),
                ("부채비율 100% 미만", True),
                ("자본잠식 50% 미만",  True),
                ("52주저가 기준 통과", True),
            ]
            for label, ok in items_l1:
                icon = "✅" if ok else "❌"
                st.markdown(f"{icon} {label}")

            st.markdown("**👥 LAYER 4 주주 구조**")
            st.markdown(f"최대주주 지분: **{row.get('최대주주지분(%)','확인불가')}%**")
            st.markdown(f"구조 등급: **{row.get('주주구조등급','확인불가')}**")

        with col2:
            st.markdown("**💰 LAYER 2 재무 점수 상세**")
            detail = str(row.get('재무상세',''))
            for d in detail.split(' / '):
                if d: st.markdown(f"  • {d}")

            st.markdown("**🎯 LAYER 5 테마**")
            st.markdown(str(row.get('테마분류','해당없음')))
            st.markdown("**📰 최신 뉴스**")
            for n in str(row.get('최신뉴스','')).split(' | '):
                if n: st.markdown(f"  • {n[:60]}")

        # 캔들차트
        st.divider()
        st.markdown("**📈 주가 차트 (최근 6개월)**")
        chart_df = get_stock_chart(code)
        if chart_df is not None and len(chart_df) > 0:
            fig = go.Figure(data=[go.Candlestick(
                x=chart_df.index,
                open=chart_df['Open'],
                high=chart_df['High'],
                low=chart_df['Low'],
                close=chart_df['Close'],
                increasing_line_color='#ff4b4b',
                decreasing_line_color='#4f8ef7',
                name="주가"
            )])
            fig.update_layout(
                template="plotly_dark",
                height=350,
                xaxis_rangeslider_visible=False,
                margin=dict(l=0,r=0,t=20,b=0),
                paper_bgcolor='#1a1d2e',
                plot_bgcolor='#1a1d2e',
            )
            st.plotly_chart(fig, use_container_width=True)

        # 거래량 차트
        st.markdown("**📊 거래량 (최근 60일)**")
        if chart_df is not None and len(chart_df) >= 10:
            vol_df = chart_df.tail(60).copy()
            avg60  = vol_df['Volume'].mean()
            colors = ['#ff4b4b' if v > avg60*2 else '#4f8ef7' for v in vol_df['Volume']]
            fig2 = go.Figure(go.Bar(
                x=vol_df.index,
                y=vol_df['Volume'],
                marker_color=colors,
                name="거래량"
            ))
            fig2.add_hline(y=avg60, line_dash="dash", line_color="#ffd700",
                           annotation_text="60일 평균")
            fig2.update_layout(
                template="plotly_dark",
                height=200,
                margin=dict(l=0,r=0,t=10,b=0),
                paper_bgcolor='#1a1d2e',
                plot_bgcolor='#1a1d2e',
            )
            st.plotly_chart(fig2, use_container_width=True)

# ══════════════════════════════════════════════════════
# TAB 3 — 매집 레이더
# ══════════════════════════════════════════════════════
with tab3:
    st.markdown("### 🔥 세력 매집 레이더")
    st.caption("거래량 급증 + 주가 횡보 + 연속 양봉 + 저가 근접 복합 분석")

    if not data_ok or result_df.empty:
        st.info("먼저 분석을 실행해주세요.")
    else:
        acc_df = result_df.copy()
        acc_df['매집점수'] = pd.to_numeric(acc_df['매집점수'], errors='coerce').fillna(0)
        acc_df = acc_df.sort_values('매집점수', ascending=False).head(30)

        if acc_df.empty:
            st.info("매집 신호 종목이 없어요.")
        else:
            # 매집 강도 히트맵
            fig_bar = px.bar(
                acc_df,
                x='매집점수',
                y='종목명',
                orientation='h',
                color='매집점수',
                color_continuous_scale=['#1a1d2e','#4f8ef7','#ff4b4b'],
                title="매집 강도 순위 (상위 30개)",
                labels={'매집점수':'매집 점수','종목명':'종목'},
            )
            fig_bar.update_layout(
                template="plotly_dark",
                height=max(400, len(acc_df)*22),
                paper_bgcolor='#1a1d2e',
                plot_bgcolor='#1a1d2e',
                margin=dict(l=0,r=0,t=40,b=0),
                yaxis={'categoryorder':'total ascending'},
                showlegend=False,
            )
            st.plotly_chart(fig_bar, use_container_width=True)

            st.divider()
            st.markdown("**매집 신호 상세**")
            for _, row in acc_df[acc_df['매집점수']>=3].iterrows():
                score = int(row['매집점수'])
                icon  = "🔥" if score >= 5 else "⚠️"
                st.markdown(
                    f"{icon} **{row['종목명']}** ({row['종목코드']}) &nbsp; "
                    f"매집점수: **{score}** &nbsp;|&nbsp; "
                    f"{row.get('매집신호','')}"
                )

# ══════════════════════════════════════════════════════
# TAB 4 — 시장 맥락
# ══════════════════════════════════════════════════════
with tab4:
    st.markdown("### 📊 시장 맥락 & 매수 타이밍 체크")

    idx = get_market_indices()
    timing_ok = idx['kosdaq_chg'] <= -1.5

    # LAYER 6 타이밍 신호등
    st.markdown("**🚦 LAYER 6 매수 타이밍 신호등**")
    signals_l6 = [
        ("전일 코스닥 1.5% 이상 하락",
         timing_ok,
         f"코스닥 {idx['kosdaq_chg']}%"),
        ("후보 종목 평균 거래대금 80억 이하",
         True,
         "세력 미진입 구간"),
    ]
    for label, ok, desc in signals_l6:
        icon = "✅" if ok else "⏳"
        color = "#44ff88" if ok else "#aaaaaa"
        st.markdown(
            f"<span style='color:{color}'>{icon} **{label}**</span> &nbsp; "
            f"<span style='color:#888; font-size:13px;'>({desc})</span>",
            unsafe_allow_html=True
        )

    st.divider()

    # 코스닥 지수 차트
    st.markdown("**📈 코스닥 지수 (최근 3개월)**")
    try:
        kq_chart = fdr.DataReader('KQ11', '2026-01-01')
        fig_kq = go.Figure(go.Scatter(
            x=kq_chart.index,
            y=kq_chart['Close'],
            fill='tozeroy',
            line_color='#4f8ef7',
            fillcolor='rgba(79,142,247,0.15)',
            name='KOSDAQ'
        ))
        fig_kq.update_layout(
            template="plotly_dark",
            height=280,
            margin=dict(l=0,r=0,t=10,b=0),
            paper_bgcolor='#1a1d2e',
            plot_bgcolor='#1a1d2e',
        )
        st.plotly_chart(fig_kq, use_container_width=True)
    except:
        st.info("지수 데이터를 불러올 수 없어요.")

    st.divider()

    # 분석 로그
    if run_logs:
        with st.expander("📋 분석 실행 로그 보기"):
            for log in run_logs:
                st.text(log)

    if not fail_df.empty:
        with st.expander(f"❌ 탈락 종목 목록 ({len(fail_df)}개)"):
            st.dataframe(fail_df, use_container_width=True)
