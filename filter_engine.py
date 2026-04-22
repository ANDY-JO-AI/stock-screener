# filter_engine.py — 점수 산정 투명화 버전 (전체 교체)
import os, time, warnings
import pandas as pd
import FinanceDataReader as fdr
import requests
from concurrent.futures import ThreadPoolExecutor, as_completed
from data_store import save_candidates

warnings.filterwarnings("ignore")

DART_KEY = os.environ.get("DART_API_KEY", "")
THEMES = {
    "방산":       ["한화","LIG","빅텍","퍼스텍","휴니드","SNT","컨텍"],
    "로봇/AI":    ["레인보우","로보티즈","뉴로메카","에스비비","두산로보틱스","HD현대","SK텔레콤"],
    "2차전지":    ["에코프로","포스코","엘앤에프","코스모","천보","나노신소재","솔루스"],
    "바이오":     ["삼성바이오","셀트리온","한미약품","유한양행","보령","동아에스티"],
    "반도체":     ["삼성전자","SK하이닉스","한미반도체","리노공업","ISC","오킨스"],
    "조선":       ["HD현대중공업","한화오션","삼성중공업","HJ중공업","케이조선"],
    "원전":       ["두산에너빌","한전기술","보성파워","비에이치아이","우진"],
    "미디어/엔터":["하이브","SM","JYP","와이지","에스엠"],
}
BAD_KW = ["스팩","SPAC","리츠","우","홀딩스","지주","제1","제2","ETF","ETN","인버스","레버리지"]

# ─── 점수 가중치 (투명 공개) ───────────────────────────────────────
WEIGHTS = {
    "financial_score":   30,   # 재무 점수 (부채비율, 영업이익, 순이익) 최대 30점
    "liquidity_score":   15,   # 거래량·시가총액 유동성 최대 15점
    "accumulation_score":25,   # 기관/외인 매집 신호 최대 25점
    "theme_score":       20,   # 테마 연관성 최대 20점
    "timing_score":      10,   # 타이밍 (52주 위치, 이평선 배열) 최대 10점
}
# timing_true 기준: timing_score >= 7 (10점 만점 중 70% 이상)
TIMING_TRUE_THRESHOLD = 7
# ────────────────────────────────────────────────────────────────────

def get_dart_corp_list():
    url = "https://opendart.fss.or.kr/api/company.json"
    params = {"crtfc_key": DART_KEY, "pblntf_ty": "A", "page_no": 1, "page_count": 100}
    corp_map = {}
    try:
        r = requests.get(
            "https://opendart.fss.or.kr/api/corpCode.xml",
            params={"crtfc_key": DART_KEY}, timeout=10
        )
        import zipfile, io, xml.etree.ElementTree as ET
        z = zipfile.ZipFile(io.BytesIO(r.content))
        xml_data = z.read(z.namelist()[0])
        root = ET.fromstring(xml_data)
        for c in root.findall("list"):
            stock_code = c.findtext("stock_code","").strip()
            corp_code  = c.findtext("corp_code","").strip()
            if stock_code:
                corp_map[stock_code] = corp_code
    except Exception as e:
        print(f"[DART] corp list error: {e}")
    return corp_map

def layer0_universe():
    kosdaq = fdr.StockListing("KOSDAQ")[["Code","Name","Marcap"]].copy()
    kospi  = fdr.StockListing("KOSPI")[["Code","Name","Marcap"]].copy()
    df = pd.concat([kosdaq, kospi], ignore_index=True)
    df["Marcap"] = pd.to_numeric(df["Marcap"], errors="coerce").fillna(0)
    # 시가총액 150억-3000억 (빅데이터 확대 버전)
    df = df[(df["Marcap"] >= 15_000_000_000) & (df["Marcap"] <= 300_000_000_000)]
    for kw in BAD_KW:
        df = df[~df["Name"].str.contains(kw, na=False)]
    df["Code"] = df["Code"].astype(str).str.zfill(6)
    return df.reset_index(drop=True)

def get_dart_financials(corp_code):
    """DART에서 최근 3개년 재무 데이터 조회"""
    results = {}
    for year in [2024, 2023, 2022]:
        url = "https://opendart.fss.or.kr/api/fnlttSinglAcntAll.json"
        params = {
            "crtfc_key": DART_KEY,
            "corp_code": corp_code,
            "bsns_year": str(year),
            "reprt_code": "11011",
            "fs_div": "CFS",
        }
        try:
            r = requests.get(url, params=params, timeout=8)
            data = r.json()
            if data.get("status") == "000":
                results[year] = data.get("list", [])
        except:
            pass
        time.sleep(0.05)
    return results

def parse_financials(fin_data):
    """재무 데이터에서 핵심 지표 추출 + 점수 산정"""
    score = 0
    breakdown = {}
    
    def get_val(year_data, account):
        for item in year_data:
            if item.get("account_nm","") == account:
                try:
                    return float(item.get("thstrm_amount","0").replace(",",""))
                except:
                    return 0.0
        return 0.0
    
    # 3개년 영업이익 확인
    op_profits = {}
    for year, data in fin_data.items():
        op_profits[year] = get_val(data, "영업이익")
    
    consecutive_loss = all(v < 0 for v in op_profits.values() if v != 0)
    if consecutive_loss:
        breakdown["영업이익_3년연속적자"] = True
        return -1, breakdown
    
    # 최근년도 부채비율
    latest_year = max(fin_data.keys()) if fin_data else None
    debt_ratio = 0
    if latest_year and fin_data[latest_year]:
        total_liab  = get_val(fin_data[latest_year], "부채총계")
        total_equity= get_val(fin_data[latest_year], "자본총계")
        if total_equity > 0:
            debt_ratio = total_liab / total_equity * 100
        breakdown["부채비율"] = round(debt_ratio, 1)
    
    # 부채비율 점수 (최대 10점)
    if debt_ratio <= 100:
        d_score = 10
    elif debt_ratio <= 200:
        d_score = 7
    elif debt_ratio <= 300:
        d_score = 4
    elif debt_ratio <= 500:
        d_score = 1
    else:
        d_score = 0
    breakdown["부채비율_점수"] = d_score
    score += d_score
    
    # 영업이익 증가 점수 (최대 10점)
    if len(op_profits) >= 2:
        years = sorted(op_profits.keys(), reverse=True)
        recent = op_profits[years[0]]
        prev   = op_profits[years[1]]
        if prev > 0 and recent > prev:
            growth = (recent - prev) / abs(prev) * 100
            if growth >= 50:
                op_score = 10
            elif growth >= 20:
                op_score = 7
            elif growth >= 0:
                op_score = 5
            else:
                op_score = 2
            breakdown["영업이익증가율"] = f"{growth:.1f}%"
        elif recent > 0 and prev <= 0:
            op_score = 8  # 흑자전환
            breakdown["영업이익증가율"] = "흑자전환"
        else:
            op_score = 2
            breakdown["영업이익증가율"] = "감소"
    else:
        op_score = 3
        breakdown["영업이익증가율"] = "데이터부족"
    breakdown["영업이익_점수"] = op_score
    score += op_score
    
    # 순이익 점수 (최대 10점)
    if latest_year and fin_data[latest_year]:
        net_income = get_val(fin_data[latest_year], "당기순이익")
        if net_income > 0:
            ni_score = 10
            breakdown["순이익"] = "흑자"
        else:
            ni_score = 0
            breakdown["순이익"] = "적자"
        breakdown["순이익_점수"] = ni_score
        score += ni_score
    
    return min(score, 30), breakdown

def layer3_liquidity(code):
    """거래량·유동성 점수 (최대 15점)"""
    try:
        df = fdr.DataReader(code, pd.Timestamp.now() - pd.Timedelta(days=30))
        if df is None or len(df) < 10:
            return 0, {"유동성": "데이터부족"}
        avg_vol = df["Volume"].tail(20).mean()
        avg_price = df["Close"].tail(5).mean()
        avg_value = avg_vol * avg_price  # 일평균 거래대금
        
        if avg_value >= 5_000_000_000:    # 50억 이상
            liq_score = 15
        elif avg_value >= 2_000_000_000:  # 20억 이상
            liq_score = 10
        elif avg_value >= 500_000_000:    # 5억 이상
            liq_score = 5
        else:
            liq_score = 0
        return liq_score, {"일평균거래대금": f"{avg_value/1e8:.1f}억", "유동성_점수": liq_score}
    except:
        return 0, {"유동성": "조회실패"}

def layer4_accumulation(code):
    """기관/외인 매집 신호 (최대 25점) — 주가 패턴 기반 근사치"""
    try:
        df = fdr.DataReader(code, pd.Timestamp.now() - pd.Timedelta(days=60))
        if df is None or len(df) < 20:
            return 0, {"매집": "데이터부족"}
        
        score = 0
        breakdown = {}
        
        # 1) 거래량 급증 (최근 5일 vs 20일 평균)
        vol_recent = df["Volume"].tail(5).mean()
        vol_avg    = df["Volume"].tail(20).mean()
        vol_ratio  = vol_recent / vol_avg if vol_avg > 0 else 0
        if vol_ratio >= 2.0:
            score += 10
            breakdown["거래량급증"] = f"{vol_ratio:.1f}배"
        elif vol_ratio >= 1.5:
            score += 6
            breakdown["거래량급증"] = f"{vol_ratio:.1f}배"
        else:
            score += 0
            breakdown["거래량급증"] = f"{vol_ratio:.1f}배(미미)"
        
        # 2) 이동평균 정배열 (5 > 20 > 60일선)
        ma5  = df["Close"].tail(5).mean()
        ma20 = df["Close"].tail(20).mean()
        ma60 = df["Close"].tail(60).mean() if len(df) >= 60 else ma20
        if ma5 > ma20 > ma60:
            score += 10
            breakdown["이평선배열"] = "정배열✅"
        elif ma5 > ma20:
            score += 5
            breakdown["이평선배열"] = "단기정배열"
        else:
            breakdown["이평선배열"] = "역배열❌"
        
        # 3) 최근 5일 양봉 비율
        last5 = df.tail(5)
        bull_days = (last5["Close"] >= last5["Open"]).sum()
        if bull_days >= 4:
            score += 5
            breakdown["최근5일양봉"] = f"{bull_days}/5일"
        elif bull_days >= 3:
            score += 2
            breakdown["최근5일양봉"] = f"{bull_days}/5일"
        
        return min(score, 25), breakdown
    except:
        return 0, {"매집": "조회실패"}

def layer5_theme_score(name):
    """테마 연관성 점수 (최대 20점)"""
    matched_themes = []
    for theme, keywords in THEMES.items():
        for kw in keywords:
            if kw in name:
                matched_themes.append(theme)
                break
    if len(matched_themes) >= 2:
        score = 20
    elif len(matched_themes) == 1:
        score = 12
    else:
        score = 0
    return score, {"연관테마": matched_themes if matched_themes else ["없음"], "테마_점수": score}

def layer6_timing(code):
    """타이밍 점수 (최대 10점) — 52주 위치 + 이평선 돌파"""
    try:
        df = fdr.DataReader(code, pd.Timestamp.now() - pd.Timedelta(days=260))
        if df is None or len(df) < 20:
            return 0, False, {"타이밍": "데이터부족"}
        
        score = 0
        close = df["Close"].iloc[-1]
        high52 = df["High"].max()
        low52  = df["Low"].min()
        
        # 52주 위치 (최대 5점)
        pos52 = (close - low52) / (high52 - low52) if high52 != low52 else 0.5
        if pos52 >= 0.7:   # 52주 고점 70% 이상
            score += 5
        elif pos52 >= 0.4:
            score += 3
        else:
            score += 1
        
        # 20일선 돌파 여부 (최대 5점)
        ma20 = df["Close"].tail(20).mean()
        if close > ma20 * 1.03:  # 20일선 +3% 이상
            score += 5
        elif close > ma20:
            score += 3
        
        timing_true = score >= TIMING_TRUE_THRESHOLD
        pos_pct = f"{pos52*100:.0f}%"
        return score, timing_true, {
            "52주위치": pos_pct, 
            "20일선대비": f"{(close/ma20-1)*100:.1f}%",
            "타이밍_점수": score,
            "timing_true_기준": f"{TIMING_TRUE_THRESHOLD}점이상"
        }
    except:
        return 0, False, {"타이밍": "조회실패"}

def analyze_stock(row, corp_map):
    code = str(row["Code"]).zfill(6)
    name = row["Name"]
    marcap = row["Marcap"]
    
    result = {
        "code": code, "name": name,
        "marcap_억": round(marcap / 1e8, 1),
        "total_score": 0,
        "timing_true": False,
        "theme": [],
        "score_breakdown": {},   # ← 점수 근거 상세
        "weight_info": WEIGHTS,  # ← 가중치 공개
    }
    
    # L1: 재무
    corp_code = corp_map.get(code)
    if not corp_code:
        return None
    fin_data = get_dart_financials(corp_code)
    fin_score, fin_bd = parse_financials(fin_data)
    if fin_score < 0:
        return None
    result["score_breakdown"]["재무(30점만점)"] = fin_bd
    
    # L2: 유동성
    liq_score, liq_bd = layer3_liquidity(code)
    result["score_breakdown"]["유동성(15점만점)"] = liq_bd
    
    # L3: 매집
    acc_score, acc_bd = layer4_accumulation(code)
    result["score_breakdown"]["매집(25점만점)"] = acc_bd
    
    # L4: 테마
    theme_score, theme_bd = layer5_theme_score(name)
    result["score_breakdown"]["테마(20점만점)"] = theme_bd
    result["theme"] = theme_bd["연관테마"]
    
    # L5: 타이밍
    tim_score, tim_true, tim_bd = layer6_timing(code)
    result["score_breakdown"]["타이밍(10점만점)"] = tim_bd
    result["timing_true"] = tim_true
    
    # 총점
    total = fin_score + liq_score + acc_score + theme_score + tim_score
    result["total_score"] = total
    result["score_detail"] = {
        "재무": fin_score,
        "유동성": liq_score,
        "매집": acc_score,
        "테마": theme_score,
        "타이밍": tim_score,
    }
    
    return result

def run_pipeline():
    print("[PIPELINE] 유니버스 로드 중...")
    universe = layer0_universe()
    print(f"[PIPELINE] {len(universe)}종목 확보")
    
    print("[PIPELINE] DART 코드 매핑 중...")
    corp_map = get_dart_corp_list()
    print(f"[PIPELINE] 매핑 {len(corp_map)}건")
    
    candidates = []
    with ThreadPoolExecutor(max_workers=8) as ex:
        futures = {ex.submit(analyze_stock, row, corp_map): row for _, row in universe.iterrows()}
        for i, f in enumerate(as_completed(futures), 1):
            try:
                res = f.result()
                if res:
                    candidates.append(res)
            except Exception as e:
                print(f"[ERR] {e}")
            if i % 50 == 0:
                print(f"[PIPELINE] {i}/{len(universe)} 처리, 후보 {len(candidates)}개")
    
    df = pd.DataFrame(candidates)
    if df.empty:
        print("[PIPELINE] 후보 없음")
        return
    
    # 정렬: total_score DESC, timing_true 우선
    df["timing_sort"] = df["timing_true"].astype(int)
    df = df.sort_values(["timing_sort","total_score"], ascending=[False, False])
    df = df.head(50).reset_index(drop=True)
    df["rank"] = df.index + 1
    
    save_candidates(df)
    print(f"[PIPELINE] 완료: 상위 {len(df)}종목 저장")

if __name__ == "__main__":
    run_pipeline()
