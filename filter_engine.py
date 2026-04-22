import os, json, time, requests
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed
import pandas as pd
import FinanceDataReader as fdr
from data_store import save_candidates

DART_KEY = os.environ.get("DART_API_KEY", "")

THEMES = {
    "방산":    ["방산","무기","전투기","미사일","K-방산","방위산업","한화","현대로템"],
    "로봇/AI": ["로봇","AI","인공지능","자율주행","옵티머스","로보틱스"],
    "2차전지": ["배터리","2차전지","전기차","양극재","음극재","LFP","NCM","리튬"],
    "반도체":  ["반도체","HBM","파운드리","메모리","DRAM","낸드","칩"],
    "바이오":  ["바이오","신약","임상","FDA","항암","mRNA","세포치료"],
    "에너지":  ["원유","가스","LPG","석유","에너지","태양광","풍력","원전"],
    "건설":    ["건설","부동산","아파트","재건축","리모델링"],
    "조선":    ["조선","LNG선","해운","컨테이너","벌크선"],
    "게임":    ["게임","메타버스","NFT","유니티","콘솔","모바일게임"],
    "화장품":  ["화장품","K뷰티","코스메틱","피부","뷰티","ODM"],
}

BAD_KW = ["스팩","SPAC","우","중국","홀딩스","리츠","기업인수","인수목적"]

HEADERS = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)"}


def get_dart_corp_list():
    try:
        url = f"https://opendart.fss.or.kr/api/company.json?crtfc_key={DART_KEY}&corp_cls=Y"
        r = requests.get(url, timeout=20)
        corps = {}
        for item in r.json().get("list", []):
            code = str(item.get("stock_code", "")).zfill(6)
            corps[code] = item.get("corp_code", "")
        return corps
    except Exception as e:
        print(f"[DART corp list 오류] {e}")
        return {}


def layer0_universe():
    print("[L0] 유니버스 로딩...")
    dfs = []
    for mkt in ["KOSDAQ", "KOSPI"]:
        try:
            raw = fdr.StockListing(mkt)
            cols = raw.columns.tolist()
            code_col = next((c for c in cols if c.lower() in ["code","종목코드","ticker"]), cols[0])
            name_col = next((c for c in cols if c.lower() in ["name","종목명","company","corp"]),
                            cols[1] if len(cols) > 1 else cols[0])
            cap_col  = next((c for c in cols if "marcap" in c.lower() or "시가총액" in c.lower()), None)
            if not cap_col:
                print(f"[L0 {mkt}] 시총 컬럼 없음")
                continue

            df = raw[[code_col, name_col, cap_col]].copy()
            df.columns = ["code", "name", "marcap"]
            df["code"]   = df["code"].astype(str).str.zfill(6)
            df["name"]   = df["name"].astype(str)
            df["market"] = mkt
            df["marcap"] = pd.to_numeric(df["marcap"], errors="coerce").fillna(0)
            df = df[(df["marcap"] >= 15_000_000_000) & (df["marcap"] <= 200_000_000_000)]
            for kw in BAD_KW:
                df = df[~df["name"].str.contains(kw, na=False)]
            print(f"[L0 {mkt}] {len(df)}종목")
            dfs.append(df)
        except Exception as e:
            print(f"[L0 {mkt} 오류] {e}")

    if not dfs:
        return pd.DataFrame()
    result = pd.concat(dfs, ignore_index=True)
    result["_s"] = result["market"].map({"KOSDAQ": 0, "KOSPI": 1})
    result = result.sort_values(["_s", "marcap"]).drop("_s", axis=1)
    print(f"[L0] 총 {len(result)}종목")
    return result


def get_dart_disclosures(corp_code):
    try:
        today = datetime.now().strftime("%Y%m%d")
        url = (f"https://opendart.fss.or.kr/api/list.json"
               f"?crtfc_key={DART_KEY}&corp_code={corp_code}"
               f"&bgn_de=20240101&end_de={today}&page_count=20")
        r = requests.get(url, timeout=8)
        return r.json().get("list", [])
    except:
        return []


def get_financial_data(corp_code):
    try:
        year = str(datetime.now().year - 1)
        url = (f"https://opendart.fss.or.kr/api/fnlttSinglAcntAll.json"
               f"?crtfc_key={DART_KEY}&corp_code={corp_code}"
               f"&bsns_year={year}&reprt_code=11011&fs_div=CFS")
        r = requests.get(url, timeout=8)
        return r.json().get("list", [])
    except:
        return []


def layer1_hard_filter(corp_code):
    if not corp_code:
        return True, []
    disclosures = get_dart_disclosures(corp_code)
    bad_titles = ["관리종목","상장폐지","횡령","배임","감사의견거절","거래정지","불성실공시"]
    for d in disclosures:
        title = d.get("report_nm", "")
        for bad in bad_titles:
            if bad in title:
                return False, []
    return True, disclosures


def layer2_financial_score(corp_code):
    if not corp_code:
        return 5, []
    fin = get_financial_data(corp_code)
    if not fin:
        return 5, []

    score = 0

    # 매출 증가
    sales = [f for f in fin if f.get("account_nm", "") == "매출액"]
    if sales:
        try:
            curr = float(str(sales[0].get("thstrm_amount", "0")).replace(",", "").replace("-", "") or "0")
            prev = float(str(sales[0].get("frmtrm_amount", "0")).replace(",", "").replace("-", "") or "0")
            curr_neg = str(sales[0].get("thstrm_amount", "")).strip().startswith("-")
            prev_neg = str(sales[0].get("frmtrm_amount", "")).strip().startswith("-")
            if not curr_neg and not prev_neg and prev > 0 and curr > prev * 1.1:
                score += 3
        except:
            pass

    # 영업이익 흑자
    op = [f for f in fin if "영업이익" in f.get("account_nm", "")]
    if op:
        val_str = str(op[0].get("thstrm_amount", "0")).strip()
        if val_str and not val_str.startswith("-") and val_str not in ["0", ""]:
            try:
                if float(val_str.replace(",", "")) > 0:
                    score += 3
            except:
                pass

    # 당기순이익 흑자
    net = [f for f in fin if f.get("account_nm", "") == "당기순이익"]
    if net:
        val_str = str(net[0].get("thstrm_amount", "0")).strip()
        if val_str and not val_str.startswith("-"):
            try:
                if float(val_str.replace(",", "")) > 0:
                    score += 2
            except:
                pass

    return min(score, 10), []


def get_price_df(code):
    try:
        df = fdr.DataReader(code, "2024-01-01")
        if df is None or len(df) < 20:
            return None
        return df
    except:
        return None


def layer3_liquidity(code):
    df = get_price_df(code)
    if df is None:
        return False, "데이터없음"
    recent = df.tail(20)
    vol   = recent["Volume"].mean() if "Volume" in recent.columns else 0
    price = recent["Close"].mean()  if "Close"  in recent.columns else 0
    turn  = vol * price
    if turn < 500_000_000:
        return False, f"거래대금부족"
    return True, f"{turn/1e8:.0f}억"


def layer4_accumulation(code):
    df = get_price_df(code)
    if df is None:
        return 0, ""
    score = 0
    df60 = df.tail(60)
    if "Volume" in df60.columns and "Close" in df60.columns:
        vol20 = df60["Volume"].tail(20).mean()
        vol60 = df60["Volume"].mean()
        if vol60 > 0 and vol20 > vol60 * 1.3:
            score += 2
        low60 = df60["Close"].min()
        curr  = df60["Close"].iloc[-1]
        if low60 > 0 and 1.05 < (curr / low60) < 1.6:
            score += 2
        # OBV 상승 추세
        obv = 0
        obv_list = []
        closes = df60["Close"].tolist()
        volumes = df60["Volume"].tolist()
        for i in range(1, len(closes)):
            if closes[i] > closes[i-1]:
                obv += volumes[i]
            elif closes[i] < closes[i-1]:
                obv -= volumes[i]
            obv_list.append(obv)
        if len(obv_list) >= 10:
            if obv_list[-1] > obv_list[-10]:
                score += 2
    return min(score, 6), ""


def layer5_theme_score(name, disclosures):
    score = 0
    matched = []
    text = name + " " + " ".join([d.get("report_nm", "") for d in disclosures[:5]])
    for theme, keywords in THEMES.items():
        for kw in keywords:
            if kw in text:
                score += 2
                matched.append(theme)
                break
    return min(score, 10), list(set(matched))


def layer6_timing(code):
    df = get_price_df(code)
    if df is None or len(df) < 20:
        return False, "데이터부족"
    close = df["Close"]
    ma5   = close.tail(5).mean()
    ma20  = close.tail(20).mean()
    curr  = close.iloc[-1]
    low20 = close.tail(20).min()

    signals = 0
    # 신호 1: 골든크로스
    if ma5 > ma20 and curr > ma5:
        signals += 1
    # 신호 2: 저점 대비 5-30% 상승 (적정 돌파)
    if low20 > 0 and 1.05 < (curr / low20) < 1.30:
        signals += 1
    # 신호 3: RSI 40-65 구간 (과열 아닌 상승 초기)
    if len(close) >= 14:
        delta = close.diff()
        gain  = delta.clip(lower=0).tail(14).mean()
        loss  = (-delta.clip(upper=0)).tail(14).mean()
        if loss > 0:
            rsi = 100 - (100 / (1 + gain / loss))
            if 40 <= rsi <= 65:
                signals += 1

    return signals >= 2, f"신호{signals}개"


def analyze_stock(row, corp_map):
    code   = str(row["code"]).zfill(6)
    name   = str(row["name"])
    marcap = row["marcap"]
    market = row["market"]
    corp_code = corp_map.get(code, "")

    # L1
    l1_pass, disclosures = layer1_hard_filter(corp_code)
    if not l1_pass:
        return None

    # L2
    l2_score, _ = layer2_financial_score(corp_code)
    if l2_score < 4:
        return None

    # L3
    l3_pass, _ = layer3_liquidity(code)
    if not l3_pass:
        return None

    # L4
    l4_score, _ = layer4_accumulation(code)

    # L5
    l5_score, l5_themes = layer5_theme_score(name, disclosures)

    # L6
    l6_pass, l6_detail = layer6_timing(code)

    total = l2_score + l4_score + l5_score + (5 if l6_pass else 0)

    return {
        "code":        code,
        "name":        name,
        "marcap_억":   round(marcap / 1e8),
        "market":      market,
        "l2_score":    l2_score,
        "l4_score":    l4_score,
        "l5_score":    l5_score,
        "l5_themes":   ",".join(l5_themes),
        "l6_timing":   l6_pass,
        "l6_detail":   l6_detail,
        "total_score": total,
    }


def run_pipeline():
    print("=== ANDY JO's STOCK AI 스크리너 시작 ===")
    universe = layer0_universe()
    if universe.empty:
        print("[종료] 유니버스 없음")
        return

    corp_map = get_dart_corp_list()
    print(f"[DART] {len(corp_map)}건 매핑")

    results = []
    with ThreadPoolExecutor(max_workers=10) as executor:
        futures = {
            executor.submit(analyze_stock, row, corp_map): row["code"]
            for _, row in universe.iterrows()
        }
        for i, future in enumerate(as_completed(futures)):
            try:
                res = future.result(timeout=30)
                if res:
                    results.append(res)
            except Exception:
                pass
            if (i + 1) % 50 == 0:
                print(f"[진행] {i+1}/{len(universe)} — 후보 {len(results)}개")

    print(f"[완료] 후보 {len(results)}개")
    if results:
        df = pd.DataFrame(results)
        df = df.sort_values("total_score", ascending=False).head(50)
        df["code"] = df["code"].astype(str).str.zfill(6)
        save_candidates(df)
        print(f"[저장] 상위 {len(df)}종목 완료")
    else:
        print("[경고] 후보 없음")


if __name__ == "__main__":
    run_pipeline()
