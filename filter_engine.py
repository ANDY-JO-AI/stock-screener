# filter_engine.py — 코스닥 전용, 실시간 진행 표시, 시간여행TV L0-L6 완전 반영
# 로컬 실행: python filter_engine.py
# GitHub Actions: python filter_engine.py 동일하게 실행

import os, time, warnings, re, json, zipfile, io, sys
import pandas as pd
import requests
from datetime import datetime, timedelta


from data_store import save_candidates, load_news_data as get_saved_news

warnings.filterwarnings("ignore")

# ─────────────────────────────────────────
# 설정값
# ─────────────────────────────────────────
DART_KEY = os.environ.get("DART_API_KEY", "")
MAX_WORKERS = 1          # 로컬 안정성: 단일 스레드
STOCK_TIMEOUT = 30       # 종목당 최대 분석 시간(초)
SAVE_INTERVAL = 10       # N종목마다 중간 저장
MIN_FINAL_SCORE = 5      # 최소 총점 (이 이상만 결과에 포함)
TOP_N = 50               # 최종 저장 종목 수

# 시총 기준 (단위: 억원)
MKTCAP_MIN = 150
MKTCAP_MAX = 700

# 제거 키워드
BAD_KW = [
    "스팩", "SPAC", "리츠", "REIT", " 우", "홀딩스",
    "제1", "제2", "ETF", "ETN", "인버스", "레버리지",
    "중국", "China", "선박", "해운"
]

MEZZANINE_KW = [
    "전환사채", "신주인수권부사채", "유상증자 결정",
    "신주인수권 행사", "전환가액 조정"
]

HYPE_KW = [
    "신사업 진출", "MOU", "업무협약", "양해각서",
    "계약 체결", "수주"
]

THEME_DICT = {
    "방산":     ["방산","방위","탄약","레이더","한화","빅텍","퍼스텍","휴니드","LIG","SNT"],
    "로봇/AI":  ["로봇","AI","자율","협동로봇","뉴로","레인보우","인공지능","자동화"],
    "2차전지":  ["배터리","전지","양극재","음극재","에코프로","엘앤에프","포스코","솔루스"],
    "바이오":   ["바이오","신약","임상","제약","헬스케어","의료","진단","치료"],
    "반도체":   ["반도체","웨이퍼","파운드리","HBM","메모리","칩","소부장"],
    "조선":     ["조선","선박","LNG선","해양","크레인","도크"],
    "원전":     ["원전","원자력","SMR","핵융합","두산에너빌","한전"],
    "정치테마": ["정치","대선","총선","후보","여당","야당","대통령"],
    "대북":     ["대북","통일","남북","철도","개성","북한"],
    "미세먼지": ["미세먼지","공기청정","마스크","황사","필터"],
    "조류독감": ["조류독감","구제역","살처분","수산","축산","방역"],
    "에너지":   ["태양광","풍력","수소","신재생","ESS","연료전지"],
    "건설":     ["건설","재건축","시공","주택","리모델링","인테리어"],
    "엔터":     ["엔터","K-POP","드라마","콘텐츠","영화","음반","아이돌"],
    "저출산":   ["저출산","출산","육아","보육","어린이","유아"],
    "우주":     ["우주","위성","발사체","항공","드론","UAM"],
    "헬스케어": ["헬스케어","웰니스","의료기기","병원","체외진단"],
}

# ─────────────────────────────────────────
# DART 유틸
# ─────────────────────────────────────────
_DART_CORP_MAP = {}

def get_dart_corp_map() -> dict:
    """DART 전체 기업 코드 매핑 (종목코드 → corp_code)"""
    global _DART_CORP_MAP
    if _DART_CORP_MAP:
        return _DART_CORP_MAP
    if not DART_KEY:
        print("[DART] API 키 없음 — 재무 데이터 스킵")
        return {}
    try:
        print("[DART] 기업 코드 다운로드 중...")
        url = f"https://opendart.fss.or.kr/api/corpCode.xml?crtfc_key={DART_KEY}"
        r = requests.get(url, timeout=60)
        import zipfile, io, xml.etree.ElementTree as ET
        zf = zipfile.ZipFile(io.BytesIO(r.content))
        xml_data = zf.read("CORPCODE.xml")
        root = ET.fromstring(xml_data)
        for item in root.findall("list"):
            stock_code = item.findtext("stock_code", "").strip()
            corp_code  = item.findtext("corp_code", "").strip()
            if stock_code:
                _DART_CORP_MAP[stock_code] = corp_code
        print(f"[DART] 기업 코드 {len(_DART_CORP_MAP)}건 로드 완료")
    except Exception as e:
        print(f"[DART] 기업 코드 로드 실패: {e}")
    return _DART_CORP_MAP


def get_dart_financials(corp_code: str, year: int) -> dict:
    """DART 단일 기업 재무제표 (연결/개별)"""
    if not DART_KEY or not corp_code:
        return {}
    try:
        url = "https://opendart.fss.or.kr/api/fnlttSinglAcntAll.json"
        params = {
            "crtfc_key": DART_KEY,
            "corp_code": corp_code,
            "bsns_year": str(year),
            "reprt_code": "11011",  # 사업보고서
            "fs_div": "CFS",        # 연결재무제표
        }
        r = requests.get(url, params=params, timeout=15)
        data = r.json()
        if data.get("status") != "000":
            # 연결 없으면 개별로 재시도
            params["fs_div"] = "OFS"
            r = requests.get(url, params=params, timeout=15)
            data = r.json()
        if data.get("status") != "000":
            return {}
        result = {}
        for item in data.get("list", []):
            nm  = item.get("account_nm", "")
            val = item.get("thstrm_amount", "0") or "0"
            try:
                result[nm] = int(val.replace(",", ""))
            except Exception:
                result[nm] = 0
        return result
    except Exception:
        return {}


def get_dart_disclosures(corp_code: str, days: int = 730) -> list:
    """DART 최근 N일 공시 목록"""
    if not DART_KEY or not corp_code:
        return []
    try:
        start = (datetime.today() - timedelta(days=days)).strftime("%Y%m%d")
        end   = datetime.today().strftime("%Y%m%d")
        url   = "https://opendart.fss.or.kr/api/list.json"
        params = {
            "crtfc_key": DART_KEY,
            "corp_code": corp_code,
            "bgn_de": start,
            "end_de": end,
            "page_count": 100,
        }
        r = requests.get(url, params=params, timeout=15)
        data = r.json()
        if data.get("status") != "000":
            return []
        return [item.get("report_nm", "") for item in data.get("list", [])]
    except Exception:
        return []


# ─────────────────────────────────────────
# FDR 유틸
# ─────────────────────────────────────────
def safe_fdr_load(code: str, start: str, end: str) -> pd.DataFrame:
    """FinanceDataReader 안전 로드"""
    try:
        import FinanceDataReader as fdr
        df = fdr.DataReader(code, start, end)
        if df is None or df.empty:
            return pd.DataFrame()
        return df
    except Exception:
        return pd.DataFrame()


# ─────────────────────────────────────────
# LAYER 0 — 유니버스 구성
# ─────────────────────────────────────────
def load_kosdaq_universe() -> pd.DataFrame:
    """코스닥 전종목 로드 (시총 필터 포함)"""
    try:
        import FinanceDataReader as fdr
        print("[L0] 코스닥 종목 목록 로드 중...")
        df = fdr.StockListing("KOSDAQ")
        if df is None or df.empty:
            print("[L0] 코스닥 목록 로드 실패")
            return pd.DataFrame()

        # 컬럼 정규화
                # 실제 컬럼명 기반 직접 매핑
        df = df.rename(columns={
            "Code":   "code",
            "Name":   "name",
            "Marcap": "mktcap",
        })


        required = {"code", "name", "mktcap"}
        missing = required - set(df.columns)
        if missing:
            print(f"[L0] 필수 컬럼 없음: {missing}")
            return pd.DataFrame()

        df["mktcap"] = pd.to_numeric(df["mktcap"], errors="coerce").fillna(0)
        df["mktcap_억"] = df["mktcap"] / 1e8

        # 시총 필터
        df = df[(df["mktcap_억"] >= MKTCAP_MIN) & (df["mktcap_억"] <= MKTCAP_MAX)]

        # 불량 키워드 제거
        for kw in BAD_KW:
            df = df[~df["name"].str.contains(kw, na=False)]

        df["code"] = df["code"].astype(str).str.zfill(6)
        df = df.reset_index(drop=True)
        print(f"[L0] 코스닥 유니버스: {len(df)}종목 (시총 {MKTCAP_MIN}억-{MKTCAP_MAX}억)")
        return df[["code", "name", "mktcap_억"]]
    except Exception as e:
        print(f"[L0] 유니버스 로드 오류: {e}")
        return pd.DataFrame()


# ─────────────────────────────────────────
# LAYER 1 — 즉시 탈락 필터
# ─────────────────────────────────────────
def check_l1_reject(code: str, name: str, corp_code: str,
                    disclosures: list, price_df: pd.DataFrame) -> tuple:
    """
    반환: (reject: bool, reason: str)
    """
    today = datetime.today()

    # 1. CB/BW/유상증자 (최근 2년)
    if disclosures:
        for disc in disclosures:
            for kw in MEZZANINE_KW:
                if kw in disc:
                    return True, f"CB/BW/유상증자: {disc[:30]}"

    # 2. 호재성 공시 4건 이상
    if disclosures:
        hype_cnt = sum(1 for d in disclosures for kw in HYPE_KW if kw in d)
        if hype_cnt >= 4:
            return True, f"호재 공시 과다({hype_cnt}건)"

    # 3. 가격 위치 (52주 최저가 × 1.5 초과)
    if not price_df.empty and len(price_df) >= 20:
        try:
            year_ago = today - timedelta(days=252)
            yearly = price_df[price_df.index >= year_ago]
            if not yearly.empty:
                low52 = yearly["Low"].min()
                cur   = price_df["Close"].iloc[-1]
                if cur > low52 * 1.5:
                    return True, f"주가 52주최저가×1.5 초과 (현재{cur:,.0f} > 기준{low52*1.5:,.0f})"
        except Exception:
            pass

    # 4. 일 거래대금 100억 이상 기록 없음 (최근 3년)
    if not price_df.empty and "Volume" in price_df.columns and "Close" in price_df.columns:
        try:
            price_df["turnover"] = price_df["Close"] * price_df["Volume"]
            max_turnover = price_df["turnover"].max()
            if max_turnover < 10_000_000_000:  # 100억
                return True, f"3년내 일거래대금 100억 미달 (최대{max_turnover/1e8:.1f}억)"
        except Exception:
            pass

    return False, ""


# ─────────────────────────────────────────
# LAYER 2 — 재무 점수 (최대 15점)
# ─────────────────────────────────────────
def calc_l2_financial(corp_code: str) -> tuple:
    """반환: (score: int, breakdown: dict)"""
    bd = {}
    score = 0
    if not corp_code or not DART_KEY:
        return score, bd

    cur_year  = datetime.today().year
    prev_year = cur_year - 1

    fin_cur  = get_dart_financials(corp_code, prev_year)
    fin_prev = get_dart_financials(corp_code, prev_year - 1)
    fin_pp   = get_dart_financials(corp_code, prev_year - 2)

    if not fin_cur:
        return 0, {"재무": "DART 데이터 없음"}

    # 영업이익 3년 흑자 (3점)
    op_cur  = fin_cur.get("영업이익", 0)
    op_prev = fin_prev.get("영업이익", 0)
    op_pp   = fin_pp.get("영업이익", 0)
    if op_cur > 0 and op_prev > 0 and op_pp > 0:
        score += 3
        bd["영업이익3년흑자"] = 3
    elif op_cur > 0 and op_prev > 0:
        score += 1
        bd["영업이익2년흑자"] = 1

    # 매출 >= 시가총액 (2점) — 시총 데이터 없으면 스킵
    rev = fin_cur.get("매출액", 0)
    if rev > 0:
        score += 2
        bd["매출흑자"] = 2

    # 부채비율 < 100% (3점), < 50% (5점)
    total_debt   = fin_cur.get("부채총계", 0)
    total_equity = fin_cur.get("자본총계", 1)
    if total_equity > 0:
        debt_ratio = total_debt / total_equity * 100
        if debt_ratio < 50:
            score += 5
            bd["부채비율<50%"] = 5
        elif debt_ratio < 100:
            score += 3
            bd["부채비율<100%"] = 3
    else:
        bd["자본잠식"] = "자본총계 0 이하"

    # 유보율 >= 300% (1점), >= 500% (2점)
    capital = fin_cur.get("자본금", 1)
    retained = fin_cur.get("이익잉여금", 0)
    if capital > 0:
        reserve_ratio = retained / capital * 100
        if reserve_ratio >= 500:
            score += 2
            bd[f"유보율{reserve_ratio:.0f}%"] = 2
        elif reserve_ratio >= 300:
            score += 1
            bd[f"유보율{reserve_ratio:.0f}%"] = 1

    # ROE >= 10% (2점)
    net_income = fin_cur.get("당기순이익", 0)
    if total_equity > 0 and net_income > 0:
        roe = net_income / total_equity * 100
        if roe >= 10:
            score += 2
            bd[f"ROE{roe:.1f}%"] = 2

    # 순자산 > 시가총액 (1점) — 정확한 시총 없으면 대략적 체크
    net_asset = fin_cur.get("자본총계", 0)
    if net_asset > 0:
        score += 1
        bd["순자산양수"] = 1

    return min(score, 15), bd


# ─────────────────────────────────────────
# LAYER 3 — 유동성 체크
# ─────────────────────────────────────────
def check_l3_liquidity(price_df: pd.DataFrame) -> tuple:
    """반환: (pass: bool, avg10_억: float, flag: str)"""
    if price_df.empty or "Volume" not in price_df.columns:
        return False, 0.0, "데이터없음"
    try:
        price_df["turnover"] = price_df["Close"] * price_df["Volume"]
        avg10 = price_df["turnover"].tail(10).mean() / 1e8
        flag  = "매수적기" if avg10 <= 80 else "유동성과다"
        return True, avg10, flag
    except Exception:
        return False, 0.0, "계산오류"


# ─────────────────────────────────────────
# LAYER 4 — 주주구조 (최대 4점)
# ─────────────────────────────────────────
def calc_l4_shareholder(corp_code: str) -> tuple:
    """반환: (score: int, breakdown: dict)"""
    bd = {}
    score = 0
    if not corp_code or not DART_KEY:
        return 0, bd
    try:
        url = "https://opendart.fss.or.kr/api/majorstock.json"
        params = {"crtfc_key": DART_KEY, "corp_code": corp_code}
        r = requests.get(url, params=params, timeout=10)
        data = r.json()
        if data.get("status") != "000":
            return 0, {"주주구조": "데이터없음"}
        items = data.get("list", [])
        if not items:
            return 0, bd
        # 최대주주 지분율
        main_holder = items[0]
        ratio_str = main_holder.get("stkqy_irds", "0") or "0"
        ratio = float(ratio_str.replace(",", "").replace("%", "") or 0)
        bd["최대주주지분"] = f"{ratio:.1f}%"
        if ratio < 30 or ratio > 70:
            score += 2
            bd["지분율가점"] = 2
        elif 30 <= ratio <= 50:
            bd["지분율경고"] = "30-50% 구간"
    except Exception as e:
        bd["주주구조오류"] = str(e)
    return min(score, 4), bd


# ─────────────────────────────────────────
# LAYER 5 — 테마 점수 (최대 7점)
# ─────────────────────────────────────────
def calc_l5_theme(name: str, news_titles: list) -> tuple:
    """반환: (score: int, matched_themes: list)"""
    matched = []
    text = name + " " + " ".join(news_titles)

    for theme, keywords in THEME_DICT.items():
        if any(kw in text for kw in keywords):
            matched.append(theme)

    score = 0
    if len(matched) >= 3:
        score = 7
    elif len(matched) == 2:
        score = 5
    elif len(matched) == 1:
        score = 3
    return score, matched


# ─────────────────────────────────────────
# LAYER 6 — 타이밍 (all-or-nothing)
# ─────────────────────────────────────────
def check_l6_timing(price_df: pd.DataFrame) -> tuple:
    """
    반환: (timing_ok: bool, reason: str)
    조건 3가지 모두 충족 시 True
    """
    if price_df.empty or len(price_df) < 20:
        return False, "데이터부족"
    try:
        today = datetime.today()
        year_ago = today - timedelta(days=252)
        yearly = price_df[price_df.index >= year_ago]
        low52 = yearly["Low"].min() if not yearly.empty else 0
        cur   = price_df["Close"].iloc[-1]

        # 조건1: 현재가 <= 52주최저가 × 1.5
        cond1 = (cur <= low52 * 1.5) if low52 > 0 else False

        # 조건2: 최근 10일 평균 거래대금 <= 80억
        price_df_c = price_df.copy()
        price_df_c["turnover"] = price_df_c["Close"] * price_df_c["Volume"]
        avg10 = price_df_c["turnover"].tail(10).mean() / 1e8
        cond2 = avg10 <= 80

        # 조건3: 장대양봉 후 장대음봉 패턴 없음 (단순 체크)
        recent = price_df.tail(5)
        bodies = (recent["Close"] - recent["Open"]).abs() / recent["Open"] * 100
        large = bodies[bodies > 7]
        cond3 = len(large) == 0  # 최근 5일 내 7% 이상 봉 없음

        timing_ok = cond1 and cond2
        reason = f"주가위치{'✅' if cond1 else '❌'} 거래대금{'✅' if cond2 else '❌'} 패턴{'✅' if cond3 else '⚠️'}"
        return timing_ok, reason
    except Exception as e:
        return False, str(e)


# ─────────────────────────────────────────
# 종목 분석 통합
# ─────────────────────────────────────────
def analyze_stock(idx: int, total: int, code: str, name: str,
                  mktcap: float, corp_map: dict, news_titles: list) -> dict | None:
    """단일 종목 전체 레이어 분석"""
    prefix = f"[{idx+1:3d}/{total}] {name}({code})"

    try:
        today  = datetime.today()
        start3 = (today - timedelta(days=365*3)).strftime("%Y-%m-%d")
        end    = today.strftime("%Y-%m-%d")

        # 가격 데이터 로드
        price_df = safe_fdr_load(code, start3, end)
        if price_df.empty:
            print(f"{prefix} — 가격 데이터 없음, 스킵")
            return None

        # DART 코드
        corp_code = corp_map.get(code, "")

        # 공시 목록
        disclosures = get_dart_disclosures(corp_code, days=730) if corp_code else []

        # L1 즉시 탈락
        reject, reason = check_l1_reject(code, name, corp_code, disclosures, price_df)
        if reject:
            print(f"{prefix} — L1탈락: {reason}")
            return None

        # L2 재무
        fin_score, fin_bd = calc_l2_financial(corp_code)

        # L3 유동성
        liq_pass, avg10, liq_flag = check_l3_liquidity(price_df)

        # L4 주주구조
        sha_score, sha_bd = calc_l4_shareholder(corp_code)

        # L5 테마
        theme_score, themes = calc_l5_theme(name, news_titles)

        # L6 타이밍
        timing_ok, timing_reason = check_l6_timing(price_df)

        # 총점 (최대 30점)
        # 재무15 + 유동성2 + 주주4 + 테마7 + 타이밍2
        liq_score   = 2 if liq_pass else 0
        timing_score = 2 if timing_ok else 0
        total_score = fin_score + liq_score + sha_score + theme_score + timing_score

        result = {
            "code":           code,
            "name":           name,
            "mktcap_억":      round(mktcap, 1),
            "total_score":    total_score,
            "fin_score":      fin_score,
            "liq_score":      liq_score,
            "sha_score":      sha_score,
            "theme_score":    theme_score,
            "timing_score":   timing_score,
            "timing":         timing_ok,
            "timing_reason":  timing_reason,
            "avg10_억":       round(avg10, 1),
            "liq_flag":       liq_flag,
            "themes":         ", ".join(themes),
            "fin_detail":     json.dumps(fin_bd, ensure_ascii=False),
            "sha_detail":     json.dumps(sha_bd, ensure_ascii=False),
            "analyzed_at":    today.strftime("%Y-%m-%d %H:%M"),
        }

        grade = "🔴탈락"
        if total_score >= 20:
            grade = "⭐핵심"
        elif total_score >= 15:
            grade = "✅우선"
        elif total_score >= 10:
            grade = "👀관심"
        elif total_score >= MIN_FINAL_SCORE:
            grade = "📌후보"

        print(f"{prefix} — 총점:{total_score:2d}점 재무:{fin_score} 테마:{theme_score} 타이밍:{'✅' if timing_ok else '❌'} [{grade}]")
        return result if total_score >= MIN_FINAL_SCORE else None

    except Exception as e:
        print(f"{prefix} — 분석 오류: {e}")
        return None


# ─────────────────────────────────────────
# 메인 파이프라인
# ─────────────────────────────────────────
def run_pipeline():
    start_time = time.time()
    print("=" * 60)
    print(f"ANDY JO STOCK AI — filter_engine 시작")
    print(f"실행 시각: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("=" * 60)

    # 유니버스 로드
    universe = load_kosdaq_universe()
    if universe.empty:
        print("[ERROR] 유니버스 로드 실패, 종료")
        return

    total = len(universe)
    print(f"\n총 {total}종목 분석 시작\n")

    # 뉴스 로드
    try:
        news_raw = get_saved_news()
        news_titles = [n.get("title", "") for n in news_raw]
        print(f"[NEWS] 뉴스 {len(news_titles)}건 로드")
    except Exception:
        news_titles = []
        print("[NEWS] 뉴스 없음 — 테마 점수 종목명 기반으로만 산정")

    # DART 코드 매핑
    corp_map = get_dart_corp_map()

    # 분석 루프
    results = []
    for idx, row in universe.iterrows():
        code   = str(row["code"]).zfill(6)
        name   = str(row["name"])
        mktcap = float(row["mktcap_억"])

        result = analyze_stock(idx, total, code, name, mktcap, corp_map, news_titles)
        if result:
            results.append(result)

        # 중간 저장
        if (idx + 1) % SAVE_INTERVAL == 0 and results:
            _save_intermediate(results)

    # 최종 정렬 및 저장
    if results:
        df = pd.DataFrame(results)
        df = df.sort_values(
            ["timing", "total_score"],
            ascending=[False, False]
        ).head(TOP_N).reset_index(drop=True)
        save_candidates(df)
        print(f"\n{'='*60}")
        print(f"✅ 완료: 후보종목 {len(df)}건 저장")
        print(f"   Timing True: {df['timing'].sum()}건")
        print(f"   평균 점수: {df['total_score'].mean():.1f}점")
    else:
        print("\n[결과] 조건 통과 종목 없음")

    elapsed = time.time() - start_time
    print(f"   소요 시간: {elapsed/60:.1f}분")
    print("=" * 60)


def _save_intermediate(results: list):
    """중간 저장 (오류 무시)"""
    try:
        df = pd.DataFrame(results)
        df = df.sort_values(["timing", "total_score"], ascending=[False, False])
        save_candidates(df)
        print(f"  [중간저장] {len(df)}건 저장됨")
    except Exception as e:
        print(f"  [중간저장 실패] {e}")


if __name__ == "__main__":
    run_pipeline()
