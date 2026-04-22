import requests, pandas as pd, time, zipfile, io, xml.etree.ElementTree as ET, os, json
from datetime import datetime, timedelta
from concurrent.futures import ThreadPoolExecutor, as_completed

try:
    import FinanceDataReader as fdr
except:
    fdr = None

from data_store import save_candidates

DART_API_KEY = os.environ.get("DART_API_KEY", "7d2191837b9373fc6f049fd6fa30d7678f2f96f6")
TODAY        = datetime.today()
TODAY_STR    = TODAY.strftime('%Y%m%d')
THREE_YRS    = (TODAY - timedelta(days=365*3)).strftime('%Y%m%d')
TWO_YRS      = (TODAY - timedelta(days=365*2)).strftime('%Y%m%d')
ONE_YR       = (TODAY - timedelta(days=365)).strftime('%Y%m%d')
MIN_MARCAP   = 150e8
MAX_MARCAP   = 700e8

THEME_DICT = {
    "🏛️정치/선거": ["선거","정치","대선","총선","정권","여당","야당","국회","후보"],
    "🛡️방산/안보": ["방산","방위","무기","군","안보","국방","미사일","전투기","드론","나토"],
    "🤖AI/로봇": ["인공지능","AI","로봇","자동화","머신러닝","딥러닝","옵티머스","휴머노이드"],
    "⚡에너지/전력": ["에너지","전력","태양광","풍력","배터리","ESS","전기차","수소","원전","유가"],
    "💊바이오/헬스": ["바이오","신약","임상","의료","헬스케어","제약","치료제","백신"],
    "🌾농업/식품": ["농업","식품","먹거리","농산물","식량"],
    "🚗자동차부품": ["자동차","부품","완성차","전장","모빌리티","2차전지"],
    "🎬미디어/콘텐츠": ["미디어","콘텐츠","엔터","드라마","영화","OTT","게임"],
    "🏗️건설/부동산": ["건설","부동산","재건축","개발","토목"],
    "💰금융/핀테크": ["금융","은행","증권","핀테크","코인","블록체인"],
    "🏭제조/소재": ["제조","소재","철강","화학","반도체","강관","비철금속"],
    "📡IT/통신": ["IT","통신","5G","6G","반도체","클라우드","데이터센터"],
    "🕊️대북/통일": ["대북","통일","남북","북한","비핵화","평화"],
}

BAD_KW  = ["전환사채","신주인수권부사채","유상증자","전환가액 조정","제3자배정","사모사채"]
GOOD_KW = ["계약","수주","MOU","협약","공급","납품","신사업","허가","승인"]

# ──────────────────────────────────────────
# DART 유틸
# ──────────────────────────────────────────
def load_corp_code_map():
    url = f"https://opendart.fss.or.kr/api/corpCode.xml?crtfc_key={DART_API_KEY}"
    try:
        r = requests.get(url, timeout=30)
        z = zipfile.ZipFile(io.BytesIO(r.content))
        xml_data = z.read("CORPCODE.xml")
        root = ET.fromstring(xml_data)
        mp = {}
        for item in root.findall("list"):
            code  = item.findtext("corp_code", "").strip()
            stock = item.findtext("stock_code", "").strip()
            name  = item.findtext("corp_name", "").strip()
            if stock:
                mp[stock] = {"corp_code": code, "corp_name": name}
        return mp
    except Exception as e:
        print(f"[DART corp_code 로드 실패] {e}")
        return {}

def dart_disclosures(corp_code, bgn_de, end_de):
    url = "https://opendart.fss.or.kr/api/list.json"
    params = {"crtfc_key": DART_API_KEY, "corp_code": corp_code,
              "bgn_de": bgn_de, "end_de": end_de, "page_count": 100}
    try:
        r = requests.get(url, params=params, timeout=15)
        data = r.json()
        if data.get("status") == "000":
            return data.get("list", [])
    except:
        pass
    return []

def dart_financials(corp_code, year):
    url = "https://opendart.fss.or.kr/api/fnlttSinglAcntAll.json"
    params = {"crtfc_key": DART_API_KEY, "corp_code": corp_code,
              "bsns_year": str(year), "reprt_code": "11011", "fs_div": "CFS"}
    try:
        r = requests.get(url, params=params, timeout=15)
        data = r.json()
        if data.get("status") == "000":
            return data.get("list", [])
    except:
        pass
    return []

def get_account(items, names):
    for item in items:
        for n in names:
            if n in item.get("account_nm", ""):
                try:
                    return float(item.get("thstrm_amount", "0").replace(",", ""))
                except:
                    pass
    return None

# ──────────────────────────────────────────
# LAYER 0 — 유니버스
# ──────────────────────────────────────────
def layer0_universe():
    stocks = []
    for mkt in ["KOSDAQ", "KOSPI"]:
        try:
            df = fdr.StockListing(mkt)
            if df is None or df.empty:
                continue
            df.columns = [c.strip() for c in df.columns]
            cap_col  = next((c for c in df.columns if "시가총액" in c or "Marcap" in c or "marcap" in c), None)
            name_col = next((c for c in df.columns if "종목명" in c or "Name" in c), None)
            code_col = next((c for c in df.columns if "종목코드" in c or "Code" in c or "Symbol" in c), None)
            if not all([cap_col, name_col, code_col]):
                print(f"[L0 {mkt}] 컬럼 없음")
                continue
            df = df[[code_col, name_col, cap_col]].copy()
            df.columns = ["code", "name", "marcap"]
            df["code"]   = df["code"].astype(str).str.zfill(6)
            df["name"]   = df["name"].astype(str)
            df["market"] = mkt
            df["marcap"] = pd.to_numeric(df["marcap"], errors="coerce").fillna(0)
            df = df[df["marcap"] >= MIN_MARCAP]
            df = df[df["marcap"] <= MAX_MARCAP]
            # 시총 상단 기준: 600억 미만→500억, 600억 이상→700억
            df = df[~((df["marcap"] >= 500e8) & (df["marcap"] < 600e8))]
            exclude = ["스팩", "SPAC", "우", "중국", "홀딩스", "리츠", "기업인수"]
            for kw in exclude:
                df = df[~df["name"].str.contains(kw, na=False)]
            stocks.append(df)
            print(f"[L0 {mkt}] {len(df)}개")
        except Exception as e:
            print(f"[L0 {mkt} 오류] {e}")
    if not stocks:
        return pd.DataFrame()
    return pd.concat(stocks, ignore_index=True)

# ──────────────────────────────────────────
# LAYER 1 — 하드 필터
# ──────────────────────────────────────────
def layer1_hard(row, corp_map):
    code = str(row["code"]).zfill(6)
    info = corp_map.get(code)
    if not info:
        return False, "DART corp_code 없음"
    corp_code = info["corp_code"]

    # CB/BW/유증 공시 (2년 이내)
    discs = dart_disclosures(corp_code, TWO_YRS, TODAY_STR)
    for d in discs:
        title = d.get("report_nm", "")
        for kw in BAD_KW:
            if kw in title:
                return False, f"공시탈락:{kw}"

    # 3개년 재무
    years = [TODAY.year - 1, TODAY.year - 2, TODAY.year - 3]
    op_list, eq_list, debt_list = [], [], []
    for yr in years:
        items = dart_financials(corp_code, yr)
        op_list.append(get_account(items, ["영업이익"]))
        eq_list.append(get_account(items, ["자본총계", "자본합계"]))
        debt_list.append(get_account(items, ["부채총계", "부채합계"]))

    # 3년 연속 영업손실
    if all(v is not None and v < 0 for v in op_list):
        return False, "3년연속영업손실"

    # 자본잠식
    if eq_list[0] is not None and eq_list[0] <= 0:
        return False, "자본잠식"

    # 부채비율 100% 이상
    if eq_list[0] and debt_list[0] and eq_list[0] > 0:
        if (debt_list[0] / eq_list[0]) >= 1.0:
            return False, f"부채비율{int(debt_list[0]/eq_list[0]*100)}%"

    # 호재 뉴스 4건 이상
    good_count = sum(1 for d in discs if any(kw in d.get("report_nm", "") for kw in GOOD_KW))
    if good_count >= 4:
        return False, f"호재공시{good_count}건초과"

    return True, corp_code

# ──────────────────────────────────────────
# LAYER 2 — 재무 점수
# ──────────────────────────────────────────
def layer2_financial(corp_code, marcap):
    score, detail = 0, []
    years = [TODAY.year - 1, TODAY.year - 2, TODAY.year - 3]
    fin = []
    for yr in years:
        items = dart_financials(corp_code, yr)
        fin.append({
            "op":   get_account(items, ["영업이익"]),
            "rev":  get_account(items, ["매출액", "수익(매출액)"]),
            "eq":   get_account(items, ["자본총계"]),
            "net":  get_account(items, ["당기순이익"]),
            "debt": get_account(items, ["부채총계"]),
            "res":  get_account(items, ["이익잉여금"]),
            "div":  get_account(items, ["배당금", "현금배당"]),
        })
    f0, f1, f2 = fin[0], fin[1], fin[2]

    # 연속 흑자
    ops = [f["op"] for f in fin]
    if all(v is not None and v > 0 for v in ops):
        score += 3; detail.append("3년연속흑자+3")
    elif ops[0] and ops[1] and ops[0] > 0 and ops[1] > 0:
        score += 1; detail.append("2년흑자+1")

    # 매출/시총
    if f0["rev"] and marcap > 0:
        r = f0["rev"] / marcap
        if r >= 1.0:   score += 2; detail.append("매출/시총≥1+2")
        elif r >= 0.5: score += 1; detail.append("매출/시총≥0.5+1")

    # 부채비율
    if f0["debt"] and f0["eq"] and f0["eq"] > 0:
        dr = f0["debt"] / f0["eq"] * 100
        if dr < 50:   score += 2; detail.append("부채<50%+2")
        elif dr < 100: score += 1; detail.append("부채50-99%+1")
        if f1["debt"] and f1["eq"] and f1["eq"] > 0:
            dr1 = f1["debt"] / f1["eq"] * 100
            if dr < dr1: score += 1; detail.append("부채감소+1")

    # 적립금비율
    if f0["res"] and f0["eq"] and f0["eq"] > 0:
        rr = f0["res"] / f0["eq"] * 100
        if rr >= 500:   score += 2; detail.append("적립금≥500%+2")
        elif rr >= 300: score += 1; detail.append("적립금≥300%+1")

    # ROE
    if f0["net"] and f0["eq"] and f0["eq"] > 0:
        roe = f0["net"] / f0["eq"] * 100
        if roe >= 10: score += 1; detail.append(f"ROE{roe:.1f}%+1")

    # 배당
    if (f0["div"] and f0["div"] > 0) or (f1["div"] and f1["div"] > 0):
        score += 1; detail.append("배당지급+1")

    # 순자산>시총
    if f0["eq"] and marcap > 0 and f0["eq"] > marcap:
        score += 2; detail.append("순자산>시총+2")

    return score, detail

# ──────────────────────────────────────────
# LAYER 3 — 매집/유동성
# ──────────────────────────────────────────
def layer3_liquidity(code):
    result = {"pass": False, "accum_score": 0, "detail": []}
    try:
        df = fdr.DataReader(code,
            (TODAY - timedelta(days=365*3)).strftime('%Y-%m-%d'),
            TODAY.strftime('%Y-%m-%d'))
        if df is None or len(df) < 20:
            return result
        df["turnover"] = df["Close"] * df["Volume"]
        if df["turnover"].max() >= 100e9:
            result["pass"] = True
            result["detail"].append("3년내1000억달성")
        else:
            return result

        avg10 = df["turnover"].tail(10).mean()
        if avg10 <= 80e9:
            result["accum_score"] += 2
            result["detail"].append("10일평균≤800억+2")

        vol_avg = df["Volume"].tail(60).mean()
        recent_max = df["Volume"].tail(5).max()
        if vol_avg > 0:
            if recent_max >= vol_avg * 5:
                result["accum_score"] += 3; result["detail"].append("거래량5배+3")
            elif recent_max >= vol_avg * 3:
                result["accum_score"] += 1; result["detail"].append("거래량3배+1")

        # 연속 양봉
        if "Open" in df.columns:
            closes = df["Close"].tail(5).values
            opens  = df["Open"].tail(5).values
            consec = sum(1 for i in range(len(closes)) if closes[i] > opens[i])
            if consec >= 3:
                result["accum_score"] += 1
                result["detail"].append(f"연속양봉{consec}일+1")
    except Exception as e:
        print(f"[L3 {code} 오류] {e}")
    return result

# ──────────────────────────────────────────
# LAYER 4 — 주주구조
# ──────────────────────────────────────────
def layer4_shareholder(corp_code):
    url = "https://opendart.fss.or.kr/api/majorstock.json"
    params = {"crtfc_key": DART_API_KEY, "corp_code": corp_code}
    try:
        r = requests.get(url, params=params, timeout=15)
        data = r.json()
        if data.get("status") == "000":
            items = data.get("list", [])
            if items:
                ratio = float(items[0].get("stock_ratio", "0").replace(",", "").replace("%", ""))
                if ratio <= 30 or ratio >= 70:
                    return f"최대주주{ratio:.1f}%우량"
                elif ratio <= 50:
                    return f"최대주주{ratio:.1f}%경고"
                else:
                    return f"최대주주{ratio:.1f}%"
    except:
        pass
    return "주주정보없음"

# ──────────────────────────────────────────
# LAYER 5 — 테마 점수
# ──────────────────────────────────────────
def layer5_theme(name, corp_code):
    score, themes = 0, []
    try:
        discs = dart_disclosures(corp_code, ONE_YR, TODAY_STR)
        text = " ".join([d.get("report_nm", "") for d in discs]) + " " + name
        for theme, kws in THEME_DICT.items():
            if any(kw in text for kw in kws):
                score += 2
                themes.append(theme)
    except:
        pass
    return score, themes

# ──────────────────────────────────────────
# LAYER 6 — 매수 타이밍
# ──────────────────────────────────────────
def layer6_timing(code, market):
    result = {"pass": False, "detail": []}
    try:
        df = fdr.DataReader(code,
            (TODAY - timedelta(days=365)).strftime('%Y-%m-%d'),
            TODAY.strftime('%Y-%m-%d'))
        if df is None or len(df) < 20:
            return result

        current = df["Close"].iloc[-1]
        low52   = df["Low"].min() if "Low" in df.columns else df["Close"].min()
        ratio   = 1.5 if market == "KOSPI" else 1.7

        if current > low52 * ratio:
            result["detail"].append(f"현재가>52주저가×{ratio}❌")
            return result
        result["detail"].append(f"현재가≤52주저가×{ratio}✅")

        # 전일 시장 하락 1.5% 이상
        idx_code = "KS11" if market == "KOSPI" else "KQ11"
        try:
            idx = fdr.DataReader(idx_code,
                (TODAY - timedelta(days=5)).strftime('%Y-%m-%d'),
                TODAY.strftime('%Y-%m-%d'))
            if len(idx) >= 2:
                prev_ret = (idx["Close"].iloc[-1] - idx["Close"].iloc[-2]) / idx["Close"].iloc[-2] * 100
                if prev_ret <= -1.5:
                    result["detail"].append(f"전일시장하락{prev_ret:.1f}%✅")
        except:
            pass

        result["pass"] = True
    except Exception as e:
        print(f"[L6 {code} 오류] {e}")
    return result

# ──────────────────────────────────────────
# 등급 계산
# ──────────────────────────────────────────
def calc_grade(total_score):
    if total_score >= 16: return "🌟핵심후보"
    elif total_score >= 11: return "⭐우선후보"
    elif total_score >= 6:  return "관심후보"
    else: return "📊참고"

# ──────────────────────────────────────────
# 단일 종목 분석 (병렬처리용)
# ──────────────────────────────────────────
def analyze_one(row, corp_map):
    code   = str(row["code"]).zfill(6)
    name   = row["name"]
    marcap = row["marcap"]
    market = row["market"]
    try:
        # L1
        passed, result = layer1_hard(row, corp_map)
        if not passed:
            return None, f"L1탈락:{result}"
        corp_code = result

        # L2
        fin_score, fin_detail = layer2_financial(corp_code, marcap)
        if fin_score < 7:
            return None, f"L2탈락:재무{fin_score}점"

        # L3
        liq = layer3_liquidity(code)
        if not liq["pass"]:
            return None, "L3탈락:거래대금미달"

        # L4
        sh_detail = layer4_shareholder(corp_code)

        # L5
        theme_score, themes = layer5_theme(name, corp_code)

        # L6
        timing = layer6_timing(code, market)

        total = fin_score + liq["accum_score"] + theme_score
        grade = calc_grade(total)

        return {
            "종목코드":  code,
            "종목명":   name,
            "시장":     market,
            "시총(억)": int(marcap / 1e8),
            "재무점수": fin_score,
            "매집점수": liq["accum_score"],
            "테마점수": theme_score,
            "종합점수": total,
            "등급":    grade,
            "테마":    ", ".join(themes),
            "주주구조": sh_detail,
            "L6타이밍": "✅" if timing["pass"] else "⏳",
            "재무상세": " | ".join(fin_detail),
            "매집상세": " | ".join(liq["detail"]),
            "분석일":   TODAY.strftime('%Y-%m-%d'),
        }, "통과"
    except Exception as e:
        return None, f"오류:{e}"

# ──────────────────────────────────────────
# 메인 파이프라인
# ──────────────────────────────────────────
def run_pipeline():
    print("=== ANDY JO's STOCK AI — 스크리너 시작 ===")
    universe = layer0_universe()
    if universe.empty:
        print("[L0] 종목 없음")
        return pd.DataFrame()
    print(f"[L0] 총 {len(universe)}개 종목")

    corp_map = load_corp_code_map()
    print(f"[DART] corp_code {len(corp_map)}개 로드")

    results = []
    rows = [row for _, row in universe.iterrows()]

    # 병렬처리 — 10개 스레드
    with ThreadPoolExecutor(max_workers=10) as executor:
        future_to_row = {executor.submit(analyze_one, row, corp_map): row for row in rows}
        done = 0
        for future in as_completed(future_to_row):
            done += 1
            stock, reason = future.result()
            row = future_to_row[future]
            if stock:
                results.append(stock)
                print(f"[{done}/{len(rows)}] ✅ {row['name']} → {reason}")
            else:
                print(f"[{done}/{len(rows)}] ❌ {row['name']} → {reason}")
            time.sleep(0.05)

    df = pd.DataFrame(results)
    if not df.empty:
        df = df.sort_values("종합점수", ascending=False).reset_index(drop=True)
        save_candidates(df)
        print(f"=== 완료: 최종 {len(df)}개 후보 ===")
    else:
        print("=== 후보 없음 ===")
    return df

if __name__ == "__main__":
    run_pipeline()
