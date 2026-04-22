import streamlit as st
import pandas as pd
import plotly.graph_objects as go
import plotly.express as px
import json
from datetime import datetime, timedelta

from data_store import load_candidates, load_market_report

st.set_page_config(
    page_title="ANDY JO's STOCK AI",
    page_icon="🤖",
    layout="wide",
    initial_sidebar_state="expanded"
)

# ── 커스텀 CSS ──
st.markdown("""
<style>
.main { background-color: #0e1117; }
.metric-card {
    background: #1e2130;
    border-radius: 10px;
    padding: 15px;
    border-left: 4px solid #00d4aa;
}
.theme-badge {
    display: inline-block;
    padding: 3px 10px;
    border-radius: 12px;
    background: #2d3748;
    color: #e2e8f0;
    margin: 2px;
    font-size: 0.85em;
}
.alert-red { border-left: 4px solid #ff4444; padding: 10px; background: #2d1b1b; border-radius: 5px; }
.alert-green { border-left: 4px solid #00d4aa; padding: 10px; background: #1b2d2a; border-radius: 5px; }
</style>
""", unsafe_allow_html=True)

# ── 데이터 로드 ──
@st.cache_data(ttl=300)
def get_data():
    candidates = load_candidates()
    report     = load_market_report()
    return candidates, report

@st.cache_data(ttl=600)
def get_market_chart(code, days=30):
    try:
        import FinanceDataReader as fdr
        df = fdr.DataReader(code,
            (datetime.today() - timedelta(days=days)).strftime('%Y-%m-%d'),
            datetime.today().strftime('%Y-%m-%d'))
        return df
    except:
        return None

# ── 사이드바 ──
with st.sidebar:
    st.image("https://img.shields.io/badge/ANDY%20JO's-STOCK%20AI-00d4aa?style=for-the-badge", width=200)
    st.caption("매일 오후 7시 자동 업데이트")
    st.divider()

    st.markdown("**⚙️ 후보 필터**")
    grade_opts = ["🌟핵심후보", "⭐우선후보", "관심후보", "📊참고"]
    sel_grades = st.multiselect("등급", grade_opts, default=["🌟핵심후보", "⭐우선후보"])
    min_score  = st.slider("최소 종합점수", 0, 25, 6)
    only_l6    = st.checkbox("L6 타이밍 종목만")
    only_accum = st.checkbox("매집신호 종목만")
    st.divider()

    if st.button("🔄 새로고침", use_container_width=True):
        st.cache_data.clear()
        st.rerun()

    st.divider()
    st.markdown("**📊 점수 기준**")
    st.markdown("🌟 핵심: 16점 이상")
    st.markdown("⭐ 우선: 11-15점")
    st.markdown("관심: 6-10점")
    st.markdown("📊 참고: 5점 이하")
    st.divider()
    st.markdown("**LAYER 기준**")
    st.markdown("L0 시총 150억-700억")
    st.markdown("L1 공시/재무 하드필터")
    st.markdown("L2 재무점수 7점 이상")
    st.markdown("L3 거래대금 1000억 달성")
    st.markdown("L4 주주구조 확인")
    st.markdown("L5 테마 점수")
    st.markdown("L6 52주저가×1.7 이내")

# ── 헤더 ──
st.markdown("# 🤖 ANDY JO's STOCK AI")
candidates, report = get_data()

update_date = report.get("분석일", "—")
st.caption(f"마지막 업데이트: {update_date} 19:00 KST  |  LAYER 0-6 완전 자동화")

# 지수 요약
try:
    kospi_data  = json.loads(report.get("KOSPI지수", "{}"))
    kosdaq_data = json.loads(report.get("KOSDAQ지수", "{}"))
    c1, c2, c3, c4 = st.columns(4)
    c1.metric("KOSPI",  f"{kospi_data.get('지수','—')}",  f"{kospi_data.get('등락률','—')}%")
    c2.metric("KOSDAQ", f"{kosdaq_data.get('지수','—')}", f"{kosdaq_data.get('등락률','—')}%")
    total_cand = len(candidates) if not candidates.empty else 0
    core_cand  = len(candidates[candidates["등급"] == "🌟핵심후보"]) if not candidates.empty else 0
    c3.metric("전체 후보", f"{total_cand}개")
    c4.metric("핵심 후보", f"{core_cand}개")
except:
    st.info("데이터를 불러오는 중입니다.")

st.divider()

# ── 10탭 구성 ──
tabs = st.tabs([
    "📊 코스피 시황",
    "📊 코스닥 시황",
    "🔥 특징 테마",
    "📋 특징종목(코스피)",
    "📋 특징종목(코스닥)",
    "🚀 상한가&급등",
    "🌙 시간외 특징주",
    "🏆 ANDY JO 후보",
    "🔍 종목 상세",
    "📡 매집레이더&맥락"
])

# ════════════════════════════════
# 탭 1 — 코스피 마감시황
# ════════════════════════════════
with tabs[0]:
    st.subheader("📊 코스피 마감시황")
    try:
        kd = json.loads(report.get("KOSPI지수", "{}"))
        col1, col2, col3 = st.columns(3)
        col1.metric("코스피 지수", kd.get("지수", "—"), f"{kd.get('등락률','—')}%")
        col2.metric("전일 종가", kd.get("전일", "—"))
        col3.metric("업데이트", update_date)
    except:
        pass

    news_list = []
    try:
        news_list = json.loads(report.get("시황뉴스", "[]"))
    except:
        pass

    if news_list:
        st.markdown("**📰 주요 시황 뉴스**")
        for n in news_list[:5]:
            st.markdown(f"- {n}")
    else:
        st.info("시황 데이터 수집 중입니다. 오늘 오후 7시 이후 확인해주세요.")

    st.markdown("**📈 코스피 30일 차트**")
    df_ks = get_market_chart("KS11", 30)
    if df_ks is not None and not df_ks.empty:
        fig = go.Figure(go.Scatter(
            x=df_ks.index, y=df_ks["Close"],
            mode="lines", line=dict(color="#ff6b6b", width=2),
            fill="tozeroy", fillcolor="rgba(255,107,107,0.1)"
        ))
        fig.update_layout(template="plotly_dark", height=300,
                          margin=dict(l=0, r=0, t=20, b=0))
        st.plotly_chart(fig, use_container_width=True)

# ════════════════════════════════
# 탭 2 — 코스닥 마감시황
# ════════════════════════════════
with tabs[1]:
    st.subheader("📊 코스닥 마감시황")
    try:
        kqd = json.loads(report.get("KOSDAQ지수", "{}"))
        col1, col2, col3 = st.columns(3)
        col1.metric("코스닥 지수", kqd.get("지수", "—"), f"{kqd.get('등락률','—')}%")
        col2.metric("전일 종가", kqd.get("전일", "—"))
        col3.metric("업데이트", update_date)
    except:
        pass

    if news_list:
        st.markdown("**📰 주요 시황 뉴스**")
        for n in news_list[5:10]:
            st.markdown(f"- {n}")
    else:
        st.info("시황 데이터 수집 중입니다.")

    st.markdown("**📈 코스닥 30일 차트**")
    df_kq = get_market_chart("KQ11", 30)
    if df_kq is not None and not df_kq.empty:
        fig = go.Figure(go.Scatter(
            x=df_kq.index, y=df_kq["Close"],
            mode="lines", line=dict(color="#00d4aa", width=2),
            fill="tozeroy", fillcolor="rgba(0,212,170,0.1)"
        ))
        fig.update_layout(template="plotly_dark", height=300,
                          margin=dict(l=0, r=0, t=20, b=0))
        st.plotly_chart(fig, use_container_width=True)

# ════════════════════════════════
# 탭 3 — 특징 테마
# ════════════════════════════════
with tabs[2]:
    st.subheader("🔥 오늘의 특징 테마")
    try:
        themes_today = json.loads(report.get("오늘테마", "[]"))
        if themes_today:
            st.markdown("**🔺 오늘 포착된 테마**")
            cols = st.columns(4)
            for i, t in enumerate(themes_today):
                cols[i % 4].markdown(f"<span class='theme-badge'>{t}</span>", unsafe_allow_html=True)
        else:
            st.info("테마 데이터 수집 중입니다.")
    except:
        st.info("테마 데이터 수집 중입니다.")

    st.divider()

    # 호재 공시
    try:
        good_disc = json.loads(report.get("호재공시", "[]"))
        if good_disc:
            st.markdown("**✅ 오늘 호재 공시**")
            for d in good_disc[:8]:
                st.markdown(f"- **{d.get('종목명','')}** — {d.get('공시','')}")
    except:
        pass

    st.divider()

    # 악재 공시
    try:
        bad_disc = json.loads(report.get("악재공시", "[]"))
        if bad_disc:
            st.markdown("**⚠️ 오늘 악재 공시 (CB/유증 주의)**")
            for d in bad_disc[:8]:
                st.markdown(f"<div class='alert-red'>⚠️ <b>{d.get('종목명','')}</b> — {d.get('공시','')}</div>",
                            unsafe_allow_html=True)
    except:
        pass

# ════════════════════════════════
# 탭 4 — 특징 종목 (코스피)
# ════════════════════════════════
with tabs[3]:
    st.subheader("📋 특징 종목 — 코스피")
    try:
        jumpers = json.loads(report.get("급등주", "[]"))
        kospi_jumpers = [j for j in jumpers if j.get("시장") == "KOSPI"]
        if kospi_jumpers:
            for stock in kospi_jumpers[:15]:
                col1, col2 = st.columns([1, 4])
                chg = stock.get("등락률", 0)
                color = "🔴" if chg >= 20 else "🟠" if chg >= 10 else "🟡"
                col1.markdown(f"**{color} +{chg:.1f}%**")
                news = stock.get("뉴스", [])
                themes = stock.get("테마", [])
                theme_str = " ".join(themes) if themes else ""
                news_str = news[0] if news else "뉴스 수집 중"
                col2.markdown(f"**{stock.get('종목명','')}** ({stock.get('코드','')})")
                col2.caption(f"{theme_str}  |  {news_str}")
                st.divider()
        else:
            st.info("코스피 특징 종목 데이터 수집 중입니다.")
    except Exception as e:
        st.info(f"데이터 로드 중: {e}")

# ════════════════════════════════
# 탭 5 — 특징 종목 (코스닥)
# ════════════════════════════════
with tabs[4]:
    st.subheader("📋 특징 종목 — 코스닥")
    try:
        jumpers = json.loads(report.get("급등주", "[]"))
        kosdaq_jumpers = [j for j in jumpers if j.get("시장") == "KOSDAQ"]
        if kosdaq_jumpers:
            for stock in kosdaq_jumpers[:15]:
                col1, col2 = st.columns([1, 4])
                chg = stock.get("등락률", 0)
                color = "🔴" if chg >= 20 else "🟠" if chg >= 10 else "🟡"
                col1.markdown(f"**{color} +{chg:.1f}%**")
                news = stock.get("뉴스", [])
                themes = stock.get("테마", [])
                theme_str = " ".join(themes) if themes else ""
                news_str = news[0] if news else "뉴스 수집 중"
                col2.markdown(f"**{stock.get('종목명','')}** ({stock.get('코드','')})")
                col2.caption(f"{theme_str}  |  {news_str}")
                st.divider()
        else:
            st.info("코스닥 특징 종목 데이터 수집 중입니다.")
    except Exception as e:
        st.info(f"데이터 로드 중: {e}")

# ════════════════════════════════
# 탭 6 — 상한가 & 정규장 급등
# ════════════════════════════════
with tabs[5]:
    st.subheader("🚀 상한가 & 정규장 급등 종목")

    try:
        upper = json.loads(report.get("상한가", "[]"))
        if upper:
            st.markdown("**🏆 상한가 (29% 이상)**")
            df_upper = pd.DataFrame(upper)
            if not df_upper.empty:
                st.dataframe(
                    df_upper[["종목명", "코드", "등락률", "시장"]].sort_values("등락률", ascending=False),
                    use_container_width=True, height=200
                )
        else:
            st.info("상한가 종목 없음")
    except:
        pass

    st.divider()

    try:
        jumpers = json.loads(report.get("급등주", "[]"))
        if jumpers:
            st.markdown("**🔺 테마별 급등 종목 (5% 이상)**")
            df_j = pd.DataFrame(jumpers)
            if not df_j.empty and "테마" in df_j.columns:
                # 테마별 그룹
                all_themes = set()
                for t_list in df_j["테마"]:
                    if isinstance(t_list, list):
                        all_themes.update(t_list)
                for theme in list(all_themes)[:8]:
                    theme_stocks = [j for j in jumpers
                                    if isinstance(j.get("테마"), list) and theme in j["테마"]]
                    if theme_stocks:
                        names = " / ".join([f"{s['종목명']} +{s['등락률']:.1f}%" for s in theme_stocks[:4]])
                        st.markdown(f"**{theme}**")
                        st.markdown(f"&nbsp;&nbsp;{names}")
            else:
                st.dataframe(df_j[["종목명", "코드", "등락률", "시장"]].sort_values("등락률", ascending=False),
                             use_container_width=True)
    except Exception as e:
        st.info(f"급등주 데이터 로드 중: {e}")

# ════════════════════════════════
# 탭 7 — 시간외 특징주
# ════════════════════════════════
with tabs[6]:
    st.subheader("🌙 시간외 단일가 특징주")

    col1, col2 = st.columns(2)
    with col1:
        st.markdown("**🔺 시간외 상승**")
        try:
            af_up = json.loads(report.get("시간외상승", "[]"))
            if af_up:
                for s in af_up[:10]:
                    chg = s.get("등락률", 0)
                    st.markdown(f"🟢 **{s.get('종목명','')}** +{chg:.2f}%")
            else:
                st.info("시간외 상승 데이터 없음")
        except:
            st.info("데이터 수집 중")

    with col2:
        st.markdown("**🔻 시간외 하락**")
        try:
            af_dn = json.loads(report.get("시간외하락", "[]"))
            if af_dn:
                for s in af_dn[:10]:
                    chg = s.get("등락률", 0)
                    st.markdown(f"🔴 **{s.get('종목명','')}** {chg:.2f}%")
            else:
                st.info("시간외 하락 데이터 없음")
        except:
            st.info("데이터 수집 중")

    st.divider()
    st.markdown("**📌 장 마감 후 주요 공시**")
    try:
        bad_disc = json.loads(report.get("악재공시", "[]"))
        good_disc = json.loads(report.get("호재공시", "[]"))
        for d in bad_disc[:5]:
            st.markdown(f"<div class='alert-red'>⚠️ <b>{d.get('종목명','')}</b> {d.get('공시','')}</div>",
                        unsafe_allow_html=True)
        for d in good_disc[:5]:
            st.markdown(f"<div class='alert-green'>✅ <b>{d.get('종목명','')}</b> {d.get('공시','')}</div>",
                        unsafe_allow_html=True)
    except:
        pass

# ════════════════════════════════
# 탭 8 — ANDY JO 후보 종목
# ════════════════════════════════
with tabs[7]:
    st.subheader("🏆 ANDY JO's STOCK AI — 후보 종목")

    if candidates.empty:
        st.info("분석 데이터 없음. GitHub Actions 실행 후 확인해주세요.")
    else:
        # 테마 싱크율 계산
        try:
            themes_today = json.loads(report.get("오늘테마", "[]"))
            if themes_today and "테마" in candidates.columns:
                sync_count = candidates["테마"].apply(
                    lambda x: any(t in str(x) for t in themes_today)
                ).sum()
                sync_rate = int(sync_count / len(candidates) * 100)
                st.markdown(f"### 🎯 오늘 시황 싱크율: **{sync_rate}%**")
                st.caption(f"후보 {len(candidates)}개 중 {sync_count}개가 오늘 급등 테마와 일치")
                st.divider()
        except:
            pass

        # 필터 적용
        filtered = candidates.copy()
        if "종합점수" in filtered.columns:
            filtered["종합점수"] = pd.to_numeric(filtered["종합점수"], errors="coerce").fillna(0)
        if sel_grades and "등급" in filtered.columns:
            filtered = filtered[filtered["등급"].isin(sel_grades)]
        if "종합점수" in filtered.columns:
            filtered = filtered[filtered["종합점수"] >= min_score]
        if only_l6 and "L6타이밍" in filtered.columns:
            filtered = filtered[filtered["L6타이밍"] == "✅"]
        if only_accum and "매집점수" in filtered.columns:
            filtered["매집점수"] = pd.to_numeric(filtered["매집점수"], errors="coerce").fillna(0)
            filtered = filtered[filtered["매집점수"] >= 3]

        st.markdown(f"**필터 결과: {len(filtered)}개 종목**")

        # 오늘 시황 연결 컬럼 추가
        try:
            themes_today = json.loads(report.get("오늘테마", "[]"))
            if themes_today and "테마" in filtered.columns:
                filtered["시황연결"] = filtered["테마"].apply(
                    lambda x: "🔥오늘급등" if any(t in str(x) for t in themes_today) else "─"
                )
        except:
            filtered["시황연결"] = "─"

        show_cols = ["등급", "종목명", "종목코드", "시장", "시총(억)",
                     "종합점수", "재무점수", "매집점수", "테마점수",
                     "테마", "시황연결", "L6타이밍"]
        show_cols = [c for c in show_cols if c in filtered.columns]

        st.dataframe(
            filtered[show_cols].reset_index(drop=True),
            use_container_width=True,
            height=500
        )

        csv = filtered.to_csv(index=False, encoding="utf-8-sig")
        st.download_button("📥 CSV 다운로드", csv, "andy_jo_candidates.csv", "text/csv")

# ════════════════════════════════
# 탭 9 — 종목 상세 카드
# ════════════════════════════════
with tabs[8]:
    st.subheader("🔍 종목 상세 분석")

    if candidates.empty:
        st.info("후보 데이터 없음")
    else:
        names = candidates["종목명"].tolist() if "종목명" in candidates.columns else []
        if names:
            sel = st.selectbox("종목 선택", names)
            row = candidates[candidates["종목명"] == sel].iloc[0]

            col1, col2, col3, col4 = st.columns(4)
            col1.metric("종합점수", row.get("종합점수", "—"))
            col2.metric("등급",     row.get("등급", "—"))
            col3.metric("L6 타이밍", row.get("L6타이밍", "—"))
            col4.metric("시총(억)", row.get("시총(억)", "—"))

            st.divider()
            c1, c2 = st.columns(2)

            with c1:
                st.markdown("**📊 LAYER 체크리스트**")
                fin_detail = str(row.get("재무상세", ""))
                accum_detail = str(row.get("매집상세", ""))

                checks = {
                    "L0 유니버스": "✅",
                    "L1 공시/재무": "✅" if "탈락" not in fin_detail else "❌",
                    "L2 재무점수": f"✅ {row.get('재무점수','0')}점",
                    "L3 매집신호": f"✅ {row.get('매집점수','0')}점",
                    "L4 주주구조": str(row.get("주주구조", "—")),
                    "L5 테마":     str(row.get("테마", "—")),
                    "L6 타이밍":   str(row.get("L6타이밍", "—")),
                }
                for k, v in checks.items():
                    st.markdown(f"- **{k}**: {v}")

            with c2:
                st.markdown("**📈 재무 레이더 차트**")
                try:
                    fin_s  = float(row.get("재무점수", 0))
                    acc_s  = float(row.get("매집점수", 0))
                    thm_s  = float(row.get("테마점수", 0))
                    tot_s  = float(row.get("종합점수", 0))
                    fig_r  = go.Figure(go.Scatterpolar(
                        r=[fin_s, acc_s, thm_s, min(tot_s/2, 10), fin_s],
                        theta=["재무", "매집", "테마", "종합", "재무"],
                        fill="toself",
                        line_color="#00d4aa"
                    ))
                    fig_r.update_layout(
                        polar=dict(radialaxis=dict(visible=True, range=[0, 15])),
                        template="plotly_dark", height=300,
                        margin=dict(l=20, r=20, t=20, b=20)
                    )
                    st.plotly_chart(fig_r, use_container_width=True)
                except Exception as e:
                    st.caption(f"차트 오류: {e}")

            st.divider()
            st.markdown("**📋 재무 상세**")
            st.info(str(row.get("재무상세", "정보 없음")))

            st.markdown("**📡 매집 패턴**")
            st.info(str(row.get("매집상세", "정보 없음")))

            # 네이버 증권 / DART 링크
            code = str(row.get("종목코드", ""))
            if code:
                c1, c2 = st.columns(2)
                c1.link_button("🔗 네이버 증권",
                               f"https://finance.naver.com/item/main.naver?code={code}")
                c2.link_button("🔗 DART 공시",
                               f"https://dart.fss.or.kr/dsab001/search.ax?textCrpNm={sel}")

# ════════════════════════════════
# 탭 10 — 매집 레이더 & 시장 맥락
# ════════════════════════════════
with tabs[9]:
    st.subheader("📡 매집 레이더 & 시장 맥락")

    if not candidates.empty:
        try:
            plot_df = candidates.copy()
            for col in ["재무점수", "매집점수", "테마점수", "종합점수"]:
                if col in plot_df.columns:
                    plot_df[col] = pd.to_numeric(plot_df[col], errors="coerce").fillna(0)

            c1, c2 = st.columns(2)
            with c1:
                st.markdown("**매집점수 vs 재무점수 버블차트**")
                fig_b = px.scatter(
                    plot_df.head(30),
                    x="재무점수", y="매집점수",
                    size="종합점수", color="테마점수",
                    hover_name="종목명" if "종목명" in plot_df.columns else None,
                    color_continuous_scale="Viridis",
                    template="plotly_dark"
                )
                fig_b.update_layout(height=350, margin=dict(l=0, r=0, t=20, b=0))
                st.plotly_chart(fig_b, use_container_width=True)

            with c2:
                st.markdown("**매집 Top 10**")
                top10 = plot_df.nlargest(10, "매집점수")
                show = ["종목명", "매집점수", "종합점수", "등급", "테마"]
                show = [c for c in show if c in top10.columns]
                st.dataframe(top10[show].reset_index(drop=True), use_container_width=True)
        except Exception as e:
            st.warning(f"차트 오류: {e}")

    st.divider()
    st.markdown("**🚦 L6 타이밍 신호**")
    if not candidates.empty and "L6타이밍" in candidates.columns:
        timing_ok = candidates[candidates["L6타이밍"] == "✅"]
        if not timing_ok.empty:
            st.success(f"✅ 현재 {len(timing_ok)}개 종목이 매수 타이밍 조건 충족")
            show = ["종목명", "종합점수", "등급", "테마"]
            show = [c for c in show if c in timing_ok.columns]
            st.dataframe(timing_ok[show].reset_index(drop=True), use_container_width=True)
        else:
            st.warning("⏳ 현재 매수 타이밍 조건 충족 종목 없음")

    st.divider()
    st.markdown("**📈 시장 30일 흐름**")
    col1, col2 = st.columns(2)
    with col1:
        df_ks = get_market_chart("KS11")
        if df_ks is not None and not df_ks.empty:
            fig = go.Figure(go.Scatter(
                x=df_ks.index, y=df_ks["Close"],
                mode="lines", line=dict(color="#ff6b6b", width=2)
            ))
            fig.update_layout(title="KOSPI", template="plotly_dark",
                              height=250, margin=dict(l=0, r=0, t=30, b=0))
            st.plotly_chart(fig, use_container_width=True)
    with col2:
        df_kq = get_market_chart("KQ11")
        if df_kq is not None and not df_kq.empty:
            fig = go.Figure(go.Scatter(
                x=df_kq.index, y=df_kq["Close"],
                mode="lines", line=dict(color="#00d4aa", width=2)
            ))
            fig.update_layout(title="KOSDAQ", template="plotly_dark",
                              height=250, margin=dict(l=0, r=0, t=30, b=0))
            st.plotly_chart(fig, use_container_width=True)
