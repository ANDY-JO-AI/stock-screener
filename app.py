# app.py
import os, json, time, ast
import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from data_store import (
    load_candidates, load_market_report,
    load_theme_data, load_after_hours,
    load_price_data, load_news_data,
)

st.set_page_config(
    page_title="ANDY JO's STOCK AI",
    page_icon="📈",
    layout="wide",
    initial_sidebar_state="expanded",
)

# ── 공통 유틸 ─────────────────────────────────────────────────
def safe_json(val):
    if isinstance(val, (dict, list)):
        return val
    try:
        return json.loads(str(val))
    except:
        return {}


def fmt_num(v, unit="억"):
    try:
        return f"{float(v):,.1f}{unit}"
    except:
        return str(v)


# ── 데이터 로딩 ───────────────────────────────────────────────
@st.cache_data(ttl=1800, show_spinner=False)
def load_all():
    candidates  = load_candidates()
    report      = load_market_report()
    themes      = load_theme_data()
    after_hours = load_after_hours()
    prices      = load_price_data()
    news        = load_news_data()
    return candidates, report, themes, after_hours, prices, news


# ── 사이드바 ─────────────────────────────────────────────────
with st.sidebar:
    st.image("https://img.icons8.com/color/96/stock-market.png", width=70)
    st.title("ANDY JO's\nSTOCK AI")
    st.caption(
        "📅 " + pd.Timestamp.now(tz="Asia/Seoul").strftime("%Y-%m-%d %H:%M KST")
    )
    st.divider()
    if st.button("🔄 데이터 새로고침", use_container_width=True):
        st.cache_data.clear()
        st.rerun()
    st.divider()
    st.caption("평일 자동 업데이트:")
    st.caption("08:50 장전 뉴스·테마")
    st.caption("15:40 장마감 급등")
    st.caption("19:00 종목 스크리닝")
    st.caption("19:30 시간외 특징주")
    st.divider()
    st.caption("© ANDY JO's STOCK AI v3.0")

# ── 데이터 로드 ───────────────────────────────────────────────
with st.spinner("📡 데이터 로딩 중..."):
    candidates, report, themes, after_hours, prices, all_news = load_all()

# 현재가 딕셔너리 (code → price info)
price_map = {p["code"]: p for p in prices} if prices else {}

# ── 헤더 ─────────────────────────────────────────────────────
st.title("📈 ANDY JO's STOCK AI")
st.caption(
    "코스닥·코스피 소형주 자동 스크리닝 | 시간여행TV 기준 L0-L6 | "
    "테마랩 | 인포스탁급 시황"
)

idx = report.get("index_summary", {})
kospi_val  = idx.get("kospi",  {}).get("close", 0)
kosdaq_val = idx.get("kosdaq", {}).get("close", 0)
usdkrw     = idx.get("usd_krw", 0)

c1, c2, c3, c4, c5 = st.columns(5)
c1.metric("KOSPI",    f"{kospi_val:,.2f}"  if kospi_val  else "—",
          f"{idx.get('kospi',{}).get('change',0):+.2f}%" if kospi_val else "")
c2.metric("KOSDAQ",   f"{kosdaq_val:,.2f}" if kosdaq_val else "—",
          f"{idx.get('kosdaq',{}).get('change',0):+.2f}%" if kosdaq_val else "")
c3.metric("USD/KRW",  f"{usdkrw:,.1f}" if usdkrw else "—")
c4.metric("후보 종목", f"{len(candidates)}개" if not candidates.empty else "0개")
c5.metric("업데이트", report.get("generated_at", "—"))
st.divider()

# ── 탭 ───────────────────────────────────────────────────────
tabs = st.tabs([
    "🏠 증시 요약", "🔥 테마 레이더", "📰 실시간 뉴스",
    "📈 급등·상한가", "🌙 시간외 특징주",
    "⭐ 종목 발굴", "🔍 종목 상세",
    "🎯 매집 레이더", "🌍 시장 맥락",
])

# ────────────────────────────────────────────────────────────
# TAB 0: 증시 요약
# ────────────────────────────────────────────────────────────
with tabs[0]:
    st.subheader("🏠 오늘의 증시 요약")

    col_a, col_b = st.columns(2)
    with col_a:
        st.markdown("#### 📊 KOSPI")
        if kospi_val:
            chg = idx.get("kospi", {}).get("change", 0)
            color = "🟢" if chg >= 0 else "🔴"
            st.metric("KOSPI 종가", f"{kospi_val:,.2f}",
                      delta=f"{chg:+.2f}%")
        else:
            st.info("장 마감 후 업데이트")

    with col_b:
        st.markdown("#### 📊 KOSDAQ")
        if kosdaq_val:
            chg = idx.get("kosdaq", {}).get("change", 0)
            st.metric("KOSDAQ 종가", f"{kosdaq_val:,.2f}",
                      delta=f"{chg:+.2f}%")
        else:
            st.info("장 마감 후 업데이트")

    st.divider()
    st.markdown("#### 🔑 오늘의 핵심 이슈")
    top_themes = themes[:3] if themes else []
    if top_themes:
        for i, t in enumerate(top_themes, 1):
            synergy_badge = "🔗 복수매체" if t.get("synergy") else ""
            st.markdown(
                f"**{i}. [{t['theme']}]** {synergy_badge} — "
                f"뉴스 {t['count']}건 | 대장주: **{t['leader']}**"
            )
            top_n = t.get("top_news", [])[:2]
            for n in top_n:
                st.markdown(
                    f"&nbsp;&nbsp;&nbsp;`{n.get('source','')}` "
                    f"[{n.get('title','')}]({n.get('url','')})"
                )
    else:
        st.warning("테마 데이터 없음 — 08:50 이후 업데이트됩니다")

    st.divider()
    st.markdown("#### 💱 환율")
    if usdkrw:
        st.metric("USD/KRW", f"{usdkrw:,.1f}원")
    else:
        st.info("데이터 없음")


# ────────────────────────────────────────────────────────────
# TAB 1: 테마 레이더
# ────────────────────────────────────────────────────────────
with tabs[1]:
    st.subheader("🔥 오늘의 테마 레이더")

    if themes:
        # 테마 히트맵
        theme_df = pd.DataFrame([
            {"테마": t["theme"], "뉴스수": t["count"],
             "대장주": t["leader"], "시너지": "✅" if t.get("synergy") else ""}
            for t in themes
        ])
        fig = px.bar(
            theme_df.head(10), x="테마", y="뉴스수",
            color="뉴스수", color_continuous_scale="Reds",
            title="테마별 뉴스 언급 수 (상위 10개)",
            text="대장주",
        )
        fig.update_traces(textposition="outside")
        st.plotly_chart(fig, use_container_width=True)

        st.divider()
        # 테마 상세
        for t in themes:
            synergy = "🔗 복수매체 시너지" if t.get("synergy") else ""
            with st.expander(
                f"📌 **{t['theme']}** — {t['count']}건 | "
                f"대장주: {t['leader']} {synergy}"
            ):
                col1, col2 = st.columns([1, 2])
                with col1:
                    st.markdown(f"**대장주:** {t['leader']}")
                    st.markdown(f"**뉴스 수:** {t['count']}건")
                    st.markdown(f"**소스:** {', '.join(t.get('sources', []))}")
                    st.markdown(f"**키워드:** {', '.join(t.get('keywords', []))}")
                with col2:
                    st.markdown("**관련 뉴스:**")
                    for n in t.get("top_news", []):
                        st.markdown(
                            f"- `{n.get('source','')}` "
                            f"[{n.get('title','')}]({n.get('url','')})"
                        )
    else:
        st.warning("테마 데이터 없음 — 08:50 이후 업데이트됩니다")


# ────────────────────────────────────────────────────────────
# TAB 2: 실시간 뉴스
# ────────────────────────────────────────────────────────────
with tabs[2]:
    st.subheader("📰 실시간 뉴스")

    if all_news:
        sources = list(set(n.get("source", "") for n in all_news))
        col_f1, col_f2 = st.columns([2, 1])
        with col_f1:
            sel_src = st.multiselect(
                "소스 필터", sources, default=sources
            )
        with col_f2:
            kw = st.text_input("키워드 검색", placeholder="예: 반도체, 수주")

        filtered = [
            n for n in all_news
            if n.get("source", "") in sel_src
            and (not kw or kw in n.get("title", ""))
        ]
        st.caption(f"총 {len(filtered)}건")

        for n in filtered[:100]:
            tagged = n.get("tagged_stocks", [])
            tag_str = " ".join([f"`{s}`" for s in tagged]) if tagged else ""
            st.markdown(
                f"- `{n.get('source','')}` "
                f"[{n.get('title','')}]({n.get('url','')}) {tag_str}"
            )
    else:
        st.warning("뉴스 없음 — 08:50 이후 업데이트됩니다")


# ────────────────────────────────────────────────────────────
# TAB 3: 급등·상한가
# ────────────────────────────────────────────────────────────
with tabs[3]:
    st.subheader("📈 오늘의 급등·상한가")

    upper = report.get("upper_limit_stocks", [])
    if upper:
        df_u = pd.DataFrame(upper)
        # 테마 연결
        if themes:
            theme_kw_map = {}
            for t in themes:
                for kw in t.get("keywords", []):
                    theme_kw_map[kw] = t["theme"]

            def match_theme(name):
                for kw, theme in theme_kw_map.items():
                    if kw in str(name):
                        return theme
                return "—"

            if "name" in df_u.columns:
                df_u["테마"] = df_u["name"].apply(match_theme)

        st.dataframe(
            df_u,
            use_container_width=True,
            column_config={
                "name":        "종목명",
                "code":        "코드",
                "change_rate": st.column_config.NumberColumn(
                    "등락률(%)", format="%.1f%%"
                ),
                "price":       "현재가",
                "테마":        "연관테마",
            },
        )
    else:
        st.warning("급등주 데이터 없음 — 15:40 이후 업데이트됩니다")


# ────────────────────────────────────────────────────────────
# TAB 4: 시간외 특징주
# ────────────────────────────────────────────────────────────
with tabs[4]:
    st.subheader("🌙 시간외 특징주 (DART + 등락률)")

    if after_hours:
        for item in after_hours:
            flag   = item.get("flag", "📋")
            itype  = item.get("type", "")
            name   = item.get("name", "")
            detail = item.get("detail", "")
            link   = item.get("link", "")

            if flag == "⚠️":
                st.error(f"{flag} **[{itype}]** {name} — {detail}")
            elif flag == "✅":
                st.success(f"{flag} **[{itype}]** {name} — {detail}")
            else:
                if link:
                    st.markdown(f"{flag} **[{itype}]** {name} — [{detail}]({link})")
                else:
                    st.markdown(f"{flag} **[{itype}]** {name} — {detail}")
    else:
        st.warning("시간외 데이터 없음 — 19:30 이후 업데이트됩니다")


# ────────────────────────────────────────────────────────────
# TAB 5: 종목 발굴 (핵심 탭)
# ────────────────────────────────────────────────────────────
with tabs[5]:
    st.subheader("⭐ 종목 발굴 — 시간여행TV L0-L6 기준")

    if not candidates.empty:

        # 점수 기준 설명
        with st.expander("📌 점수 산정 기준 및 정렬 방식 (클릭하여 펼치기)"):
            st.markdown("""
**총점 = L2재무 + L4주주구조 + L5테마 + 타이밍보너스**

| 레이어 | 항목 | 배점 | 세부 기준 |
|--------|------|------|-----------|
| L2 재무 | 영업이익 연속흑자 | 최대 3점 | 3년연속+3 / 2년+1 |
| L2 재무 | 매출/시총 비율 | 최대 2점 | ≥1.0배+2 / ≥0.5배+1 |
| L2 재무 | 부채비율 | 최대 2점 | <50%+2 / <100%+1 |
| L2 재무 | 부채비율 감소추세 | 1점 | 전년대비 하락 |
| L2 재무 | 유보율 | 최대 2점 | ≥500%+2 / ≥300%+1 |
| L2 재무 | ROE | 1점 | ≥10% |
| L2 재무 | 순자산>시총 | 2점 | 자산주 |
| L4 주주 | 지분율 | 최대 2점 | <30% 또는 >70% |
| L5 테마 | 테마 연관성 | 최대 7점 | 종목명+뉴스 매칭 |
| L6 타이밍 | Timing True | 보너스 5점 | 3조건 동시 충족 |

**🟢 Timing True 조건 (3개 모두 충족):**
- 현재가 ≤ 52주 최저가 × 1.5
- 최근 10일 평균 거래대금 ≤ 80억 (소외 상태)
- 장대양봉 후 장대음봉 패턴 없음

**정렬:** Timing True 종목 우선 → 총점 내림차순
            """)

        # 필터
        col_f1, col_f2, col_f3, col_f4 = st.columns(4)
        min_score    = col_f1.slider("최소 총점", 0, 30, 7)
        only_timing  = col_f2.checkbox("🟢 Timing True만", False)
        group_theme  = col_f3.checkbox("🗂️ 테마별 그룹핑", False)
        theme_filter = col_f4.selectbox(
            "테마 필터",
            ["전체"] + sorted(set(
                t for ts in candidates["l5_themes"].dropna()
                for t in str(ts).split(",") if t.strip()
            )) if "l5_themes" in candidates.columns else ["전체"]
        )

        df_show = candidates.copy()
        for col in ["total_score", "l2_score", "l4_score", "l5_score", "marcap_억"]:
            if col in df_show.columns:
                df_show[col] = pd.to_numeric(df_show[col], errors="coerce").fillna(0)

        df_show = df_show[df_show["total_score"] >= min_score]
        if only_timing and "timing_true" in df_show.columns:
            df_show = df_show[df_show["timing_true"].astype(str).isin(["True", "true", "1"])]
        if theme_filter != "전체" and "l5_themes" in df_show.columns:
            df_show = df_show[df_show["l5_themes"].str.contains(theme_filter, na=False)]

        # timing_true 정렬
        df_show["_t"] = df_show.get("timing_true", False).apply(
            lambda x: 1 if str(x) in ["True", "true", "1"] else 0
        )
        df_show = df_show.sort_values(["_t", "total_score"], ascending=[False, False])
        df_show = df_show.drop(columns=["_t"])

        st.metric("필터 후 종목 수", f"{len(df_show)}개")

        if group_theme and "l5_themes" in df_show.columns:
            # ── 테마별 그룹핑 뷰
            all_t = sorted(set(
                t.strip() for ts in df_show["l5_themes"].dropna()
                for t in str(ts).split(",") if t.strip()
            ))
            for theme in all_t:
                t_df = df_show[df_show["l5_themes"].str.contains(theme, na=False)].copy()
                if t_df.empty:
                    continue

                # 대장주 = 총점 최고
                leader_name = t_df.sort_values(
                    "total_score", ascending=False
                ).iloc[0]["name"]

                # 해당 테마 뉴스 시너지
                t_info = next(
                    (t for t in themes if t["theme"] == theme), {}
                )
                synergy = "🔗 복수매체 시너지" if t_info.get("synergy") else ""

                st.markdown(f"### 🏷️ {theme} ({len(t_df)}종목) {synergy}")

                for _, row in t_df.iterrows():
                    is_leader = row["name"] == leader_name
                    is_timing = str(row.get("timing_true", "")).lower() in ["true", "1"]
                    badges = ""
                    if is_leader:
                        badges += " 👑 대장주"
                    if is_timing:
                        badges += " 🟢"

                    # KIS 현재가 연결
                    p_info = price_map.get(str(row.get("code", "")), {})
                    price_str = (
                        f"{p_info['current']:,}원 ({p_info['change_rate']:+.1f}%)"
                        if p_info.get("current") else "—"
                    )

                    with st.expander(
                        f"{badges} **{row['name']}** | "
                        f"총점 {row['total_score']:.0f} | "
                        f"시총 {row['marcap_억']:.0f}억 | {price_str}"
                    ):
                        _render_score_detail(row)
                st.divider()

        else:
            # ── 기본 리스트 뷰
            display_cols = [
                c for c in [
                    "rank", "name", "marcap_억", "total_score",
                    "l2_score", "l4_score", "l5_score",
                    "l5_themes", "timing_true",
                ]
                if c in df_show.columns
            ]
            st.dataframe(
                df_show[display_cols],
                use_container_width=True,
                column_config={
                    "rank":        "순위",
                    "name":        "종목명",
                    "marcap_억":   st.column_config.NumberColumn("시총(억)", format="%d억"),
                    "total_score": st.column_config.NumberColumn("총점", format="%d"),
                    "l2_score":    "재무",
                    "l4_score":    "주주",
                    "l5_score":    "테마",
                    "l5_themes":   "테마명",
                    "timing_true": "타이밍🟢",
                },
            )
    else:
        st.warning("후보 종목 없음 — 19:00 이후 업데이트됩니다")


def _render_score_detail(row):
    """점수 상세 렌더링 (종목 발굴 + 종목 상세 공용)"""
    score_cols = st.columns(4)
    for idx_c, (label, key, max_pt) in enumerate([
        ("L2 재무", "l2_score", 15),
        ("L4 주주", "l4_score", 4),
        ("L5 테마", "l5_score", 7),
        ("총점",    "total_score", 30),
    ]):
        try:
            val = int(float(str(row.get(key, 0))))
        except:
            val = 0
        score_cols[idx_c].metric(label, f"{val}/{max_pt}")

    is_timing = str(row.get("timing_true", "")).lower() in ["true", "1"]
    if is_timing:
        st.success("🟢 **Timing True** — 매수 타이밍 조건 3개 충족")
    else:
        st.warning("⚪ Timing False — 매수 타이밍 미충족")

    # 점수 근거 상세
    bd = safe_json(row.get("score_breakdown", {}))
    if bd:
        st.markdown("**📋 점수 산정 근거:**")
        for layer, details in bd.items():
            with st.expander(f"{layer}"):
                if isinstance(details, dict):
                    for k, v in details.items():
                        st.write(f"· {k}: **{v}**")
                else:
                    st.write(details)


# ────────────────────────────────────────────────────────────
# TAB 6: 종목 상세
# ────────────────────────────────────────────────────────────
with tabs[6]:
    st.subheader("🔍 종목 상세")

    if not candidates.empty and "name" in candidates.columns:
        sel = st.selectbox("종목 선택", candidates["name"].tolist())
        row = candidates[candidates["name"] == sel].iloc[0]
        code = str(row.get("code", "")).lstrip("'").zfill(6)

        # KIS 현재가
        p_info = price_map.get(code, {})
        col_h = st.columns(5)
        col_h[0].metric("종목코드", code)
        col_h[1].metric("시가총액", fmt_num(row.get("marcap_억", 0)))
        col_h[2].metric("총점", row.get("total_score", "—"))
        if p_info.get("current"):
            col_h[3].metric(
                "현재가",
                f"{p_info['current']:,}원",
                delta=f"{p_info['change_rate']:+.1f}%",
            )
            col_h[4].metric("거래량", f"{p_info.get('volume',0):,}")
        else:
            col_h[3].metric("현재가", "—")

        st.divider()
        _render_score_detail(row)

        # 관련 뉴스
        st.divider()
        st.markdown("#### 📰 관련 뉴스")
        related = [
            n for n in all_news
            if sel in n.get("title", "")
            or (len(sel) >= 3 and sel[:3] in n.get("title", ""))
        ]
        if related:
            for n in related[:10]:
                st.markdown(
                    f"- `{n.get('source','')}` "
                    f"[{n.get('title','')}]({n.get('url','')})"
                )
        else:
            st.info("관련 뉴스 없음")

        # 매도 트리거
        st.divider()
        st.markdown("#### 🚨 매도 트리거 (시간여행TV 기준)")
        st.info(
            "트리거 A: 당일 거래량이 60일 평균 대비 **5배 이상** 폭증 시 즉시 매도 검토\n\n"
            "트리거 B: 테마 재료 소멸 예상 시점 전 매도 (선거 완료, 이슈 종료)\n\n"
            "트리거 C: 저점 대비 **+100% 달성** 시 부분 또는 전량 매도\n\n"
            "트리거 D: 더 우수한 조건의 종목 발견 시 종목 스위칭"
        )

        # 바로가기
        st.markdown(
            f"🔗 [네이버 증권](https://finance.naver.com/item/main.naver?code={code}) "
            f"| [DART 공시](https://dart.fss.or.kr/search/crtfc.do?query={sel}) "
            f"| [KRX](https://www.krx.co.kr)"
        )
    else:
        st.warning("후보 종목 없음")


# ────────────────────────────────────────────────────────────
# TAB 7: 매집 레이더
# ────────────────────────────────────────────────────────────
with tabs[7]:
    st.subheader("🎯 매집 레이더")

    if not candidates.empty:
        for col in ["l2_score", "l4_score", "l5_score", "total_score", "marcap_억"]:
            if col in candidates.columns:
                candidates[col] = pd.to_numeric(
                    candidates[col], errors="coerce"
                ).fillna(0)

        col_r1, col_r2 = st.columns(2)

        with col_r1:
            fig1 = px.scatter(
                candidates,
                x="l2_score", y="l5_score",
                size="marcap_억",
                color="total_score",
                hover_name="name" if "name" in candidates.columns else None,
                labels={
                    "l2_score":    "재무점수(L2)",
                    "l5_score":    "테마점수(L5)",
                    "total_score": "총점",
                },
                title="재무 vs 테마 (버블=시총, 색=총점)",
                color_continuous_scale="RdYlGn",
            )
            st.plotly_chart(fig1, use_container_width=True)

        with col_r2:
            # 기관·외인 순매수 (KIS)
            if prices:
                df_p = pd.DataFrame(prices)
                if "inst_net_buy" in df_p.columns and "name" not in df_p.columns:
                    # code로 이름 매핑
                    code_name = candidates.set_index("code")["name"].to_dict() \
                        if "code" in candidates.columns else {}
                    df_p["name"] = df_p["code"].map(code_name).fillna(df_p["code"])

                if "inst_net_buy" in df_p.columns:
                    df_p["inst_net_buy"] = pd.to_numeric(
                        df_p["inst_net_buy"], errors="coerce"
                    ).fillna(0)
                    top_inst = df_p.nlargest(15, "inst_net_buy")
                    fig2 = px.bar(
                        top_inst,
                        x="name" if "name" in top_inst.columns else "code",
                        y="inst_net_buy",
                        title="기관 순매수 TOP 15",
                        color="inst_net_buy",
                        color_continuous_scale="Blues",
                    )
                    st.plotly_chart(fig2, use_container_width=True)
                else:
                    st.info("기관/외인 데이터 없음")
            else:
                st.info("KIS 현재가 데이터 없음 — 장중 업데이트됩니다")

        st.divider()
        st.markdown("#### Top 20 종목")
        top_cols = [
            c for c in [
                "rank", "name", "marcap_억", "total_score",
                "l2_score", "l5_score", "l5_themes", "timing_true",
            ]
            if c in candidates.columns
        ]
        st.dataframe(candidates[top_cols].head(20), use_container_width=True)

    else:
        st.warning("데이터 없음")


# ────────────────────────────────────────────────────────────
# TAB 8: 시장 맥락
# ────────────────────────────────────────────────────────────
with tabs[8]:
    st.subheader("🌍 시장 맥락 & 타이밍 신호등")

    # KOSPI / KOSDAQ 분리 분석
    col_m1, col_m2 = st.columns(2)
    with col_m1:
        st.markdown("#### KOSPI")
        kp_chg = idx.get("kospi", {}).get("change", 0)
        if kp_chg >= 1:
            st.success(f"🟢 KOSPI {kp_chg:+.2f}% — 강세")
        elif kp_chg >= -1:
            st.warning(f"🟡 KOSPI {kp_chg:+.2f}% — 혼조")
        else:
            st.error(f"🔴 KOSPI {kp_chg:+.2f}% — 약세")

    with col_m2:
        st.markdown("#### KOSDAQ")
        kq_chg = idx.get("kosdaq", {}).get("change", 0)
        if kq_chg >= 1:
            st.success(f"🟢 KOSDAQ {kq_chg:+.2f}% — 강세")
        elif kq_chg >= -1:
            st.warning(f"🟡 KOSDAQ {kq_chg:+.2f}% — 혼조")
        else:
            st.error(f"🔴 KOSDAQ {kq_chg:+.2f}% — 약세")

    st.divider()
    # 매수 타이밍 신호등
    st.markdown("#### 🚦 매수 타이밍 신호등")
    if not candidates.empty and "timing_true" in candidates.columns:
        timing_cnt = candidates["timing_true"].apply(
            lambda x: str(x).lower() in ["true", "1"]
        ).sum()
        total_cnt = len(candidates)
        pct = timing_cnt / total_cnt * 100 if total_cnt > 0 else 0

        st.metric("Timing True 비율", f"{timing_cnt}/{total_cnt} ({pct:.0f}%)")
        if pct >= 40:
            st.success("🟢 매수 타이밍 우호적 — 공격적 접근 가능")
        elif pct >= 20:
            st.warning("🟡 혼조 — 선별적 접근 권장")
        else:
            st.error("🔴 타이밍 불리 — 관망 권장")

        # 진행 바
        st.progress(int(pct), text=f"Timing True 비율: {pct:.0f}%")

    st.divider()
    # 테마 분포 파이차트
    if themes:
        st.markdown("#### 📊 오늘의 테마 분포")
        pie_df = pd.DataFrame([
            {"테마": t["theme"], "뉴스수": t["count"]}
            for t in themes[:10]
        ])
        fig3 = px.pie(
            pie_df, values="뉴스수", names="테마",
            title="테마별 뉴스 비중",
        )
        st.plotly_chart(fig3, use_container_width=True)

    # 포트폴리오 구성 가이드
    st.divider()
    st.markdown("#### 💼 시간여행TV 포트폴리오 구성 가이드")
    top3_themes = [t["theme"] for t in themes[:3]] if themes else []
    st.info(
        f"**현재 강세 테마:** {', '.join(top3_themes) if top3_themes else '—'}\n\n"
        "**배분 원칙:** 소형주 7 : 대형주 3\n\n"
        "**소형주 내 구성:** 정치테마 3종 + 계절테마 1-2종 + 상황테마 1-2종\n\n"
        "**분할매수:** 1회당 총자산의 10% 이하, 지정가 주문"
    )
