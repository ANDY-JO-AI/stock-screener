import os, json, time
import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import gspread
from google.oauth2.service_account import Credentials

st.set_page_config(
    page_title="ANDY JO's STOCK AI",
    page_icon="📈",
    layout="wide",
    initial_sidebar_state="expanded"
)

SHEETS_ID = "1OqRboKwx7X0-3W67_raZyV2ZjcQ_G7ylHilf7SgAU88"
SCOPES = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]


def connect_sheets(retries=3, delay=4):
    for attempt in range(retries):
        try:
            gcp = st.secrets["gcp"]
            cred_info = {
                "type":                        gcp["type"],
                "project_id":                  gcp["project_id"],
                "private_key_id":              gcp["private_key_id"],
                "private_key":                 gcp["private_key"],
                "client_email":                gcp["client_email"],
                "client_id":                   gcp["client_id"],
                "auth_uri":                    gcp["auth_uri"],
                "token_uri":                   gcp["token_uri"],
                "auth_provider_x509_cert_url": gcp["auth_provider_x509_cert_url"],
                "client_x509_cert_url":        gcp["client_x509_cert_url"],
                "universe_domain":             gcp.get("universe_domain", "googleapis.com"),
            }
            creds = Credentials.from_service_account_info(cred_info, scopes=SCOPES)
            gc = gspread.authorize(creds)
            return gc.open_by_key(SHEETS_ID)
        except Exception as e:
            if attempt < retries - 1:
                time.sleep(delay)
            else:
                st.error(f"Sheets 연결 실패: {e}")
                return None


@st.cache_data(ttl=1800, show_spinner=False)
def load_all_data():
    sh = connect_sheets()
    candidates = pd.DataFrame()
    report = {}
    if sh is None:
        return candidates, report
    try:
        ws = sh.worksheet("후보종목")
        data = ws.get_all_records()
        if data:
            candidates = pd.DataFrame(data)
            # code 앞자리 ' 제거 후 zfill
            if "code" in candidates.columns:
                candidates["code"] = candidates["code"].astype(str).str.lstrip("'").str.zfill(6)
    except Exception as e:
        st.warning(f"후보종목 오류: {e}")
    time.sleep(1)
    try:
        ws2 = sh.worksheet("시황리포트")
        cell = ws2.acell("B2").value
        report = json.loads(cell) if cell else {}
    except Exception as e:
        st.warning(f"시황리포트 오류: {e}")
    return candidates, report


# ── 사이드바 ─────────────────────────────────────────────────
with st.sidebar:
    st.image("https://img.icons8.com/color/96/stock-market.png", width=70)
    st.title("ANDY JO's\nSTOCK AI")
    st.caption("📅 " + pd.Timestamp.now(tz="Asia/Seoul").strftime("%Y-%m-%d %H:%M KST"))
    st.divider()
    if st.button("🔄 데이터 새로고침", use_container_width=True):
        st.cache_data.clear()
        st.rerun()
    st.divider()
    st.caption("매일 평일 오후 7시 자동 업데이트")
    st.caption("© ANDY JO's STOCK AI v2.0")

# ── 데이터 로딩 ───────────────────────────────────────────────
with st.spinner("📡 데이터 로딩 중..."):
    candidates, report = load_all_data()

# ── 헤더 ─────────────────────────────────────────────────────
st.title("📈 ANDY JO's STOCK AI")
st.caption("코스닥·코스피 소형주 자동 스크리닝 + 시황 리포트 | 매일 평일 오후 7시 업데이트")

c1, c2, c3, c4 = st.columns(4)
c1.metric("후보 종목", f"{len(candidates)}개" if not candidates.empty else "0개")
c2.metric("L6 타이밍 통과",
          f"{(candidates['l6_timing'].astype(str)=='True').sum()}개"
          if not candidates.empty and 'l6_timing' in candidates.columns else "—")
c3.metric("활성 테마", f"{len(report.get('themes',{}))}개" if report else "—")
c4.metric("업데이트", report.get("generated_at", "—"))
st.divider()

# ── 탭 ───────────────────────────────────────────────────────
tabs = st.tabs([
    "🏠 시황 리포트", "🔥 오늘의 테마", "📈 급등·상한가",
    "🌙 시간외 특징주", "⭐ 후보 종목", "🔍 종목 상세",
    "🎯 매집 레이더", "🕐 시장 맥락", "💡 독보적 기능", "📰 전체 뉴스"
])

# ── TAB 0: 시황 리포트 ────────────────────────────────────────
with tabs[0]:
    st.subheader("🏠 오늘의 시황 리포트")
    if report:
        st.success(f"✅ 생성 시각: {report.get('generated_at','—')}")
        news = report.get("news", [])
        if news:
            st.subheader("📰 주요 뉴스 헤드라인")
            for n in news[:10]:
                src = n.get("source","")
                st.markdown(f"- `{src}` [{n.get('title','')}]({n.get('url','')})")
        else:
            st.warning("뉴스 데이터 없음")
    else:
        st.warning("시황 리포트 없음 — 평일 오후 7시 이후 업데이트됩니다")

# ── TAB 1: 오늘의 테마 ────────────────────────────────────────
with tabs[1]:
    st.subheader("🔥 오늘의 급등 테마")
    themes = report.get("themes", {})
    if themes:
        cols_t = st.columns(min(len(themes), 4))
        for i, (theme, info) in enumerate(list(themes.items())[:4]):
            cols_t[i].metric(f"📌 {theme}", f"{info.get('count',0)}건")
        st.divider()
        for theme, info in themes.items():
            with st.expander(f"📌 {theme} — {info.get('count',0)}건 뉴스"):
                for n in info.get("news", []):
                    if isinstance(n, dict):
                        st.markdown(f"- `{n.get('source','')}` [{n.get('title','')}]({n.get('url','')})")
                    else:
                        st.write(f"• {n}")
    else:
        st.warning("테마 데이터 없음 — 평일 오후 7시 이후 업데이트됩니다")

# ── TAB 2: 급등·상한가 ───────────────────────────────────────
with tabs[2]:
    st.subheader("📈 오늘의 급등·상한가 종목")
    upper = report.get("upper_limit_stocks", [])
    if upper:
        df_u = pd.DataFrame(upper)
        if "reason" in df_u.columns:
            st.dataframe(
                df_u[["name","code","change_rate","price","reason"]],
                use_container_width=True,
                column_config={
                    "name":        "종목명",
                    "code":        "코드",
                    "change_rate": st.column_config.NumberColumn("등락률(%)", format="%.2f%%"),
                    "price":       "현재가",
                    "reason":      "급등 이유",
                }
            )
        else:
            st.dataframe(df_u, use_container_width=True)
    else:
        st.warning("급등주 데이터 없음 — 장 마감(15:30) 이후 업데이트됩니다")

# ── TAB 3: 시간외 특징주 ─────────────────────────────────────
with tabs[3]:
    st.subheader("🌙 시간외 특징주 (DART 공시)")
    dart = report.get("dart_disclosures", [])
    bad_kw  = ["유상증자","전환사채","신주인수권","횡령","배임","감사의견"]
    good_kw = ["자사주취득","계약체결","수주","특허","임상","허가"]
    if dart:
        for d in dart[:20]:
            title = d.get("title", "")
            corp  = d.get("corp", "")
            url   = d.get("url", "")
            if any(kw in title for kw in bad_kw):
                st.error(f"⚠️ **{corp}** — [{title}]({url})")
            elif any(kw in title for kw in good_kw):
                st.success(f"✅ **{corp}** — [{title}]({url})")
            else:
                st.markdown(f"📋 **{corp}** — [{title}]({url})")
    else:
        st.warning("공시 데이터 없음")

# ── TAB 4: 후보 종목 ─────────────────────────────────────────
with tabs[4]:
    st.subheader("⭐ LAYER 0-6 후보 종목")
    if not candidates.empty:
        col_f1, col_f2, col_f3 = st.columns(3)
        min_score = col_f1.slider("최소 총점", 0, 25, 5)
        show_l6   = col_f2.checkbox("L6 타이밍 통과만", False)
        sort_by   = col_f3.selectbox("정렬 기준",
                                     ["total_score","marcap_억","l4_score","l5_score"])

        df_show = candidates.copy()
        if "total_score" in df_show.columns:
            df_show["total_score"] = pd.to_numeric(df_show["total_score"], errors="coerce").fillna(0)
            df_show = df_show[df_show["total_score"] >= min_score]
        if show_l6 and "l6_timing" in df_show.columns:
            df_show = df_show[df_show["l6_timing"].astype(str) == "True"]
        if sort_by in df_show.columns:
            df_show = df_show.sort_values(sort_by, ascending=False)

        st.metric("필터 후 종목 수", f"{len(df_show)}개")
        display_cols = [c for c in
                        ["code","name","marcap_억","market","total_score",
                         "l2_score","l4_score","l5_score","l6_timing","l5_themes"]
                        if c in df_show.columns]
        st.dataframe(
            df_show[display_cols],
            use_container_width=True,
            column_config={
                "code":        "종목코드",
                "name":        "종목명",
                "marcap_억":   st.column_config.NumberColumn("시총(억)", format="%d억"),
                "market":      "시장",
                "total_score": st.column_config.NumberColumn("총점", format="%d"),
                "l2_score":    "재무",
                "l4_score":    "매집",
                "l5_score":    "테마",
                "l6_timing":   "타이밍",
                "l5_themes":   "테마명",
            }
        )
    else:
        st.warning("후보 종목 없음")

# ── TAB 5: 종목 상세 ─────────────────────────────────────────
with tabs[5]:
    st.subheader("🔍 종목 상세 카드")
    if not candidates.empty and "name" in candidates.columns:
        sel = st.selectbox("종목 선택", candidates["name"].tolist())
        row = candidates[candidates["name"] == sel].iloc[0]
        code = str(row.get("code","")).lstrip("'").zfill(6)

        ca, cb, cc, cd = st.columns(4)
        ca.metric("종목코드", code)
        cb.metric("시가총액", f"{row.get('marcap_억','—')}억")
        cc.metric("총점", row.get("total_score","—"))
        cd.metric("시장", row.get("market","—"))
        st.divider()

        p1, p2, p3 = st.columns(3)
        for col_w, label, key in [(p1,"L2 재무","l2_score"),(p2,"L4 매집","l4_score"),(p3,"L5 테마","l5_score")]:
            try:
                val = int(float(str(row.get(key,0))))
            except:
                val = 0
            col_w.progress(val/10, text=f"{label}: {val}/10")

        if str(row.get("l6_timing","")) == "True":
            st.success(f"✅ L6 매수 타이밍 통과 — {row.get('l6_detail','')}")
        else:
            st.error(f"❌ L6 미통과 — {row.get('l6_detail','')}")

        if row.get("l5_themes"):
            st.info(f"📌 테마: {row.get('l5_themes','')}")

        st.divider()
        st.markdown(
            f"🔗 [네이버 증권](https://finance.naver.com/item/main.naver?code={code}) &nbsp;|&nbsp; "
            f"[DART 공시](https://dart.fss.or.kr/search/crtfc.do?query={row.get('name','')}) &nbsp;|&nbsp; "
            f"[KRX](https://www.krx.co.kr)"
        )
    else:
        st.warning("후보 종목 없음")

# ── TAB 6: 매집 레이더 ───────────────────────────────────────
with tabs[6]:
    st.subheader("🎯 매집 레이더")
    if not candidates.empty and "l4_score" in candidates.columns:
        for col in ["l2_score","l4_score","l5_score","total_score","marcap_억"]:
            if col in candidates.columns:
                candidates[col] = pd.to_numeric(candidates[col], errors="coerce").fillna(0)

        fig = px.scatter(
            candidates,
            x="l2_score", y="l4_score",
            size="marcap_억",
            color="total_score",
            hover_name="name" if "name" in candidates.columns else None,
            hover_data=["code","l5_themes"] if "l5_themes" in candidates.columns else ["code"],
            labels={"l2_score":"재무점수","l4_score":"매집점수","total_score":"총점"},
            title="매집점수 vs 재무점수 (버블=시총, 색상=총점)",
            color_continuous_scale="RdYlGn"
        )
        st.plotly_chart(fig, use_container_width=True)

        st.subheader("Top 15 종목")
        top_cols = [c for c in ["name","code","market","marcap_억","total_score","l4_score","l5_themes","l6_timing"]
                    if c in candidates.columns]
        st.dataframe(candidates[top_cols].head(15), use_container_width=True)
    else:
        st.warning("데이터 없음")

# ── TAB 7: 시장 맥락 ─────────────────────────────────────────
with tabs[7]:
    st.subheader("🕐 시장 맥락 & 타이밍")
    themes = report.get("themes", {})
    if themes:
        theme_df = pd.DataFrame([
            {"테마": t, "뉴스수": v.get("count", 0)}
            for t, v in list(themes.items())[:8]
        ])
        fig2 = px.bar(theme_df, x="테마", y="뉴스수",
                      title="오늘 급등 테마별 뉴스 언급량", color="뉴스수",
                      color_continuous_scale="Oranges")
        st.plotly_chart(fig2, use_container_width=True)

    if not candidates.empty and "l6_timing" in candidates.columns:
        l6_t = (candidates["l6_timing"].astype(str) == "True").sum()
        total = len(candidates)
        pct   = l6_t / total * 100 if total > 0 else 0
        st.metric("L6 타이밍 통과율", f"{l6_t}/{total} ({pct:.0f}%)")
        if pct >= 40:
            st.success("🟢 매수 타이밍 우호적 — 공격적 접근 가능")
        elif pct >= 20:
            st.warning("🟡 혼조 — 선별적 접근 권장")
        else:
            st.error("🔴 타이밍 불리 — 관망 권장")

# ── TAB 8: 독보적 기능 ───────────────────────────────────────
with tabs[8]:
    st.subheader("💡 독보적 기능 3가지")
    d1, d2, d3 = st.columns(3)

    with d1:
        st.markdown("### 🎯 테마 싱크율")
        if not candidates.empty and "l5_themes" in candidates.columns:
            all_t = []
            for t in candidates["l5_themes"].dropna():
                all_t.extend([x.strip() for x in str(t).split(",") if x.strip()])
            if all_t:
                tc = pd.Series(all_t).value_counts()
                fig3 = px.pie(values=tc.values, names=tc.index,
                              title="후보 종목 테마 분포")
                st.plotly_chart(fig3, use_container_width=True)
            else:
                st.info("테마 데이터 없음")
        else:
            st.info("후보 종목 없음")

    with d2:
        st.markdown("### 🚨 공시 위험 알람")
        dart = report.get("dart_disclosures", [])
        bad_kw2 = ["유상증자","전환사채","신주인수권","횡령","배임","감사의견"]
        alerts = [d for d in dart if any(kw in d.get("title","") for kw in bad_kw2)]
        if alerts:
            for a in alerts[:6]:
                st.error(f"⚠️ {a.get('corp','')} — {a.get('title','')[:30]}")
        else:
            st.success("✅ 위험 공시 없음")

        good_kw2 = ["자사주취득","계약체결","수주","특허","임상","허가"]
        goods = [d for d in dart if any(kw in d.get("title","") for kw in good_kw2)]
        if goods:
            st.divider()
            st.markdown("**🟢 호재 공시**")
            for g in goods[:4]:
                st.success(f"✅ {g.get('corp','')} — {g.get('title','')[:30]}")

    with d3:
        st.markdown("### 🌡️ 매집 히트맵 TOP 10")
        if not candidates.empty and "l4_score" in candidates.columns:
            top10 = candidates.nlargest(10, "l4_score")
            fig4 = go.Figure(go.Bar(
                x=top10["name"] if "name" in top10.columns else top10.index,
                y=top10["l4_score"],
                marker=dict(color=top10["l4_score"], colorscale="RdYlGn"),
                text=top10["l4_score"],
                textposition="auto"
            ))
            fig4.update_layout(title="매집 강도 TOP 10", showlegend=False)
            st.plotly_chart(fig4, use_container_width=True)
        else:
            st.info("데이터 없음")

# ── TAB 9: 전체 뉴스 ─────────────────────────────────────────
with tabs[9]:
    st.subheader("📰 오늘의 전체 뉴스")
    news_all = report.get("news", [])
    if news_all:
        src_filter = st.multiselect(
            "뉴스 소스 필터",
            options=list(set(n.get("source","") for n in news_all)),
            default=list(set(n.get("source","") for n in news_all))
        )
        for n in news_all:
            if n.get("source","") in src_filter:
                st.markdown(f"- `{n.get('source','')}` [{n.get('title','')}]({n.get('url','')})")
    else:
        st.warning("뉴스 데이터 없음")
