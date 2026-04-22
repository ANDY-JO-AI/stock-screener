# filter_engine.py — 시간여행TV L0-L6 완전 반영
import os, time, warnings, re, json, zipfile, io
import xml.etree.ElementTree as ET
import pandas as pd
import FinanceDataReader as fdr
import requests
from concurrent.futures import ThreadPoolExecutor, as_completed
from data_store import save_candidates, load_news_data as get_saved_news

warnings.filterwarnings("ignore")

DART_KEY = os.environ.get("DART_API_KEY", "")

BAD_KW = [
    "스팩", "SPAC", "리츠", "우", "홀딩스", "제1", "제2",
    "ETF", "ETN", "인버스", "레버리지", "중국", "China",
]

MEZZANINE_KW = [
    "전환사채", "신주인수권부사채", "유상증자 결정",
    "신주인수권 행사", "전환가액 조정",
]

HYPE_KW = ["신사업 진출", "MOU", "업무협약", "양해각서"]

THEME_DICT = {
    "방산":      ["방산", "방위", "탄약", "레이더", "한화", "빅텍", "퍼스텍", "휴니드"],
    "로봇/AI":   ["로봇", "AI", "자율", "협동로봇", "뉴로", "레인보우"],
    "2차전지":   ["배터리", "전지", "양극재", "음극재", "에코프로", "엘앤에프"],
    "바이오":    ["바이오", "신약", "임상", "제약", "헬스케어"],
    "반도체":    ["반도체", "웨이퍼", "파운드리", "HBM", "메모리"],
    "조선":      ["조선", "선박", "LNG선", "해양"],
    "원전":      ["원전", "원자력", "SMR", "핵융합"],
    "정치테마":  ["정치", "대선", "총선", "후보", "여당", "야당"],
    "대북":      ["대북", "통일", "남북", "철도", "개성"],
    "미세먼지":  ["미세먼지", "공기청정", "마스크", "황사"],
    "조류독감":  ["조류독감", "구제역", "살처분", "수산"],
    "에너지":    ["태양광", "풍력", "수소", "신재생"],
    "건설":      ["건설", "재건축", "시공", "주택"],
    "엔터":      ["엔터", "K-POP", "드라마", "콘텐츠"],
    "저출산":    ["저출산", "출산", "육아", "보육"],
    "우주":      ["우주", "위성", "발사체", "항공"],
}


# ─── LAYER 0 ────────────────────────────────────────────────
def layer0_universe(kosdaq_idx: float = 700.0) -> pd.DataFrame:
    kosdaq = fdr.StockListing("KOSDAQ")[["Code", "Name", "Marcap"]].copy()
    kospi  = fdr.StockListing("KOSPI")[["Code",  "Name", "Marcap"]].copy()
    df = pd.concat([kosdaq, kospi], ignore_index=True)
    df["Marcap"] = pd.to_numeric(df["Marcap"], errors="coerce").fillna(0)
    upper = 700_0000_0000 if kosdaq_idx >= 600 else 500_0000_0000
    lower = 150_0000_0000
    df = df[(df["Marcap"] >= lower) & (df["Marcap"] <= upper)]
    for kw in BAD_KW:
        df = df[~df["Name"].str.contains(kw, na=False)]
    df["Code"] = df["Code"].astype(str).str.zfill(6)
    print(f"[L0] 유니버스: {len(df)}종목")
    return df.reset_index(drop=True)


# ─── DART 유틸 ──────────────────────────────────────────────
def get_dart_corp_map() -> dict:
    corp_map = {}
    try:
        r = requests.get(
            "https://opendart.fss.or.kr/api/corpCode.xml",
            params={"crtfc_key": DART_KEY}, timeout=15
        )
        z = zipfile.ZipFile(io.BytesIO(r.content))
        xml_data = z.read(z.namelist()[0])
        root = ET.fromstring(xml_data)
        for c in root.findall("list"):
            stock_code = c.findtext("stock_code", "").strip()
            corp_code  = c.findtext("corp_code",  "").strip()
            if stock_code:
                corp_map[stock_code] = corp_code
        print(f"[DART] corp_map: {len(corp_map)}건")
    except Exception as e:
        print(f"[DART] corp_map 오류: {e}")
    return corp_map


def get_dart_financials(corp_code: str) -> dict:
    results = {}
    for year in [2024, 2023, 2022]:
        url = "https://opendart.fss.or.kr/api/fnlttSinglAcntAll.json"
        params = {
            "crtfc_key":  DART_KEY,
            "corp_code":  corp_code,
            "bsns_year":  str(year),
            "reprt_code": "11011",
            "fs_div":     "CFS",
        }
        try:
            r = requests.get(url, params=params, timeout=8)
            data = r.json()
            if data.get("status") == "000":
                results[year] = data.get("list", [])
            else:
                params["fs_div"] = "OFS"
                r2 = requests.get(url, params=params, timeout=8)
                data2 = r2.json()
                if data2.get("status") == "000":
                    results[year] = data2.get("list", [])
        except:
            pass
        time.sleep(0.05)
    return results


def get_dart_disclosures(corp_code: str, years: int = 2) -> list:
    start = (pd.Timestamp.now() - pd.Timedelta(days=365 * years)).strftime("%Y%m%d")
    end   = pd.Timestamp.now().strftime("%Y%m%d")
    url   = "https://opendart.fss.or.kr/api/list.json"
    params = {
        "crtfc_key":  DART_KEY,
        "corp_code":  corp_code,
        "bgn_de":     start,
        "end_de":     end,
        "page_no":    1,
        "page_count": 100,
    }
    try:
        r = requests.get(url, params=params, timeout=8)
        data = r.json()
        if data.get("status") == "000":
            return data.get("list", [])
    except:
        pass
    return []


def get_val(year_data: list, account: str) -> float:
    for item in year_data:
        if item.get("account_nm", "") == account:
            try:
                return float(
                    item.get("thstrm_amount", "0")
                    .replace(",", "").replace(" ", "")
                )
            except:
                return 0.0
    return 0.0


# ─── LAYER 1 ────────────────────────────────────────────────
def layer1_hard_filter(corp_code: str, fin_data: dict,
                       code: str, name: str):
    if not fin_data:
        return False, "재무데이터없음"

    years = sorted(fin_data.keys(), reverse=True)
    op_profits = {y: get_val(fin_data[y], "영업이익") for y in years}
    consecutive_loss = sum(1 for v in list(op_profits.values())[:3] if v < 0)
    if consecutive_loss >= 3:
        return False, "3년연속영업적자"

    latest = years[0] if years else None
    if not latest or not fin_data[latest]:
        return False, "최신재무없음"

    total_liab   = get_val(fin_data[latest], "부채총계")
    total_equity = get_val(fin_data[latest], "자본총계")
    paid_capital = get_val(fin_data[latest], "자본금")

    if paid_capital > 0:
        erosion_rate = (paid_capital - total_equity) / paid_capital * 100
        if erosion_rate >= 50:
            return False, f"자본잠식{erosion_rate:.0f}%"

    if total_equity > 0:
        debt_ratio = total_liab / total_equity * 100
        if debt_ratio >= 100:
            return False, f"부채비율{debt_ratio:.0f}%"
    elif total_equity <= 0:
        return False, "완전자본잠식"

    disclosures = get_dart_disclosures(corp_code, years=2)
    for d in disclosures:
        rpt = d.get("report_nm", "")
        for kw in MEZZANINE_KW:
            if kw in rpt:
                return False, f"메자닌공시({kw})"

    disc_1y = get_dart_disclosures(corp_code, years=1)
    hype_cnt = sum(
        1 for d in disc_1y
        if any(kw in d.get("report_nm", "") for kw in HYPE_KW)
    )
    if hype_cnt >= 4:
        return False, f"호재남발({hype_cnt}건)"

    try:
        df_price = fdr.DataReader(
            code, pd.Timestamp.now() - pd.Timedelta(days=260)
        )
        if df_price is not None and len(df_price) >= 20:
            low52   = df_price["Low"].min()
            current = df_price["Close"].iloc[-1]
            if low52 > 0 and current > low52 * 1.5:
                return False, f"52주저가×1.5초과"
    except:
        pass

    try:
        df3y = fdr.DataReader(
            code, pd.Timestamp.now() - pd.Timedelta(days=1095)
        )
        if df3y is not None and len(df3y) > 0:
            max_val = (df3y["Volume"] * df3y["Close"]).max()
            if max_val < 10_000_000_000:
                return False, "거래대금100억이력없음"
    except:
        pass

    return True, "통과"


# ─── LAYER 2 ────────────────────────────────────────────────
def layer2_financial_score(fin_data: dict, marcap: float):
    score = 0
    bd = {}
    years = sorted(fin_data.keys(), reverse=True)
    if not years:
        return 0, {}

    latest = years[0]
    ld     = fin_data[latest]

    op = {y: get_val(fin_data[y], "영업이익") for y in years[:3]}
    if all(v > 0 for v in op.values()):
        score += 3; bd["영업이익3년흑자"] = "+3"
    elif sum(1 for v in list(op.values())[:2] if v > 0) == 2:
        score += 1; bd["영업이익2년흑자"] = "+1"

    revenue = get_val(ld, "매출액")
    if marcap > 0:
        rev_ratio = revenue / marcap
        if rev_ratio >= 1.0:
            score += 2; bd[f"매출/시총={rev_ratio:.2f}"] = "+2"
        elif rev_ratio >= 0.5:
            score += 1; bd[f"매출/시총={rev_ratio:.2f}"] = "+1"

    total_liab   = get_val(ld, "부채총계")
    total_equity = get_val(ld, "자본총계")
    if total_equity > 0:
        dr = total_liab / total_equity * 100
        if dr < 50:
            score += 2; bd[f"부채비율{dr:.0f}%"] = "+2"
        elif dr < 100:
            score += 1; bd[f"부채비율{dr:.0f}%"] = "+1"
        if len(years) >= 2:
            dr_prev = get_val(fin_data[years[1]], "부채총계") / \
                      max(get_val(fin_data[years[1]], "자본총계"), 1) * 100
            if dr < dr_prev:
                score += 1; bd["부채비율감소추세"] = "+1"

    paid_cap = get_val(ld, "자본금")
    retained = get_val(ld, "이익잉여금")
    if paid_cap > 0:
        reserve_ratio = retained / paid_cap * 100
        if reserve_ratio >= 500:
            score += 2; bd[f"유보율{reserve_ratio:.0f}%"] = "+2"
        elif reserve_ratio >= 300:
            score += 1; bd[f"유보율{reserve_ratio:.0f}%"] = "+1"

    net_income = get_val(ld, "당기순이익")
    if total_equity > 0:
        roe = net_income / total_equity * 100
        if roe >= 10:
            score += 1; bd[f"ROE{roe:.1f}%"] = "+1"

    net_assets = get_val(ld, "자본총계")
    if net_assets > marcap:
        score += 2; bd["순자산>시총"] = "+2"

    return score, bd


# ─── LAYER 3 ────────────────────────────────────────────────
def layer3_volume(code: str):
    bd = {}
    try:
        df3y = fdr.DataReader(
            code, pd.Timestamp.now() - pd.Timedelta(days=1095)
        )
        df10 = fdr.DataReader(
            code, pd.Timestamp.now() - pd.Timedelta(days=20)
        )
        if df3y is None or df3y.empty:
            return False, False, {"수급": "데이터없음"}

        max_val = (df3y["Volume"] * df3y["Close"]).max()
        bd["3년최대거래대금"] = f"{max_val/1e8:.0f}억"
        passes = max_val >= 10_000_000_000

        timing = False
        if df10 is not None and len(df10) >= 5:
            avg10 = (df10["Volume"].tail(10) * df10["Close"].tail(10)).mean()
            bd["최근10일평균거래대금"] = f"{avg10/1e8:.1f}억"
            timing = avg10 <= 8_000_000_000

        return passes, timing, bd
    except Exception as e:
        return False, False, {"수급오류": str(e)}


# ─── LAYER 4 ────────────────────────────────────────────────
def layer4_shareholder(corp_code: str):
    score = 0
    bd = {}
    url = "https://opendart.fss.or.kr/api/majorstock.json"
    params = {"crtfc_key": DART_KEY, "corp_code": corp_code}
    try:
        r = requests.get(url, params=params, timeout=8)
        data = r.json()
        if data.get("status") != "000":
            return 0, {"주주구조": "데이터없음"}
        items = data.get("list", [])
        if not items:
            return 0, {"주주구조": "없음"}
        largest   = items[0]
        ratio_str = largest.get("stkqy_irds", "0").replace(",", "")
        try:
            ratio = float(ratio_str)
        except:
            ratio = 0.0
        bd["최대주주지분율"] = f"{ratio:.1f}%"
        if ratio < 30:
            score += 2; bd["지분플래그"] = "30%미만"
        elif ratio > 70:
            score += 2; bd["지분플래그"] = "70%초과"
        elif 30 <= ratio <= 50:
            score -= 1; bd["지분플래그"] = "30-50%경고"
        return score, bd
    except Exception as e:
        return 0, {"주주구조오류": str(e)}


# ─── LAYER 5 ────────────────────────────────────────────────
def layer5_theme(name: str, news_list: list = None):
    score = 0
    matched = []
    bd = {}
    for theme, keywords in THEME_DICT.items():
        for kw in keywords:
            if kw in name:
                matched.append(theme)
                score += 2
                bd[f"종목명테마({theme})"] = "+2"
                break
    if news_list:
        for news in news_list[:50]:
            title = news.get("title", "")
            if name in title or (len(name) >= 3 and name[:3] in title):
                score += 1
                bd["뉴스언급"] = "+1"
                break
    return min(score, 7), matched, bd


# ─── LAYER 6 ────────────────────────────────────────────────
def layer6_timing(code: str):
    bd = {}
    try:
        df = fdr.DataReader(
            code, pd.Timestamp.now() - pd.Timedelta(days=260)
        )
        if df is None or len(df) < 20:
            return False, {"타이밍": "데이터부족"}

        close  = df["Close"].iloc[-1]
        low52  = df["Low"].min()
        high52 = df["High"].max()

        threshold = low52 * 1.5
        cond_a    = close <= threshold
        pos_pct   = (close - low52) / (high52 - low52) * 100 \
                    if high52 != low52 else 50
        bd["52주위치"]    = f"{pos_pct:.0f}%"
        bd["조건A_저점근방"] = cond_a

        avg10  = (df["Volume"].tail(10) * df["Close"].tail(10)).mean()
        cond_b = avg10 <= 8_000_000_000
        bd["최근10일거래대금"] = f"{avg10/1e8:.1f}억"
        bd["조건B_소외상태"]   = cond_b

        body     = abs(df["Close"] - df["Open"])
        avg_body = body.mean()
        cond_c   = True
        for i in range(1, min(10, len(df))):
            prev      = df.iloc[-i - 1]
            curr      = df.iloc[-i]
            prev_bull = (prev["Close"] > prev["Open"]) and \
                        (prev["Close"] - prev["Open"] > avg_body * 1.5)
            curr_bear = (curr["Close"] < curr["Open"]) and \
                        (curr["Open"] - curr["Close"] > avg_body * 1.5)
            if prev_bull and curr_bear:
                cond_c = False
                break
        bd["조건C_패턴없음"] = cond_c

        timing_true = cond_a and cond_b and cond_c
        bd["TimingTrue"] = timing_true
        return timing_true, bd
    except Exception as e:
        return False, {"타이밍오류": str(e)}


# ─── 종목 통합 분석 ─────────────────────────────────────────
def analyze_stock(row: pd.Series, corp_map: dict,
                  news_list: list = None):
    code      = str(row["Code"]).zfill(6)
    name      = row["Name"]
    marcap    = float(row["Marcap"])
    corp_code = corp_map.get(code)
    if not corp_code:
        return None

    fin_data = get_dart_financials(corp_code)

    passes, reason = layer1_hard_filter(corp_code, fin_data, code, name)
    if not passes:
        return None

    l2_score, l2_bd = layer2_financial_score(fin_data, marcap)
    if l2_score < 7:
        return None

    l3_passes, l3_timing, l3_bd = layer3_volume(code)
    if not l3_passes:
        return None

    l4_score, l4_bd = layer4_shareholder(corp_code)
    l5_score, l5_themes, l5_bd = layer5_theme(name, news_list)
    if l5_score < 3:
        return None

    timing_true, l6_bd = layer6_timing(code)
    total = l2_score + max(l4_score, 0) + l5_score + (5 if timing_true else 0)

    return {
        "code":        code,
        "name":        name,
        "marcap_억":   round(marcap / 1e8, 1),
        "total_score": total,
        "l2_score":    l2_score,
        "l4_score":    l4_score,
        "l5_score":    l5_score,
        "l5_themes":   ",".join(l5_themes),
        "timing_true": timing_true,
        "l3_timing":   l3_timing,
        "score_breakdown": {
            "L2_재무":     l2_bd,
            "L3_수급":     l3_bd,
            "L4_주주구조":  l4_bd,
            "L5_테마":     l5_bd,
            "L6_타이밍":   l6_bd,
        },
    }


# ─── 파이프라인 ─────────────────────────────────────────────
def run_pipeline():
    try:
        news_list = get_saved_news()
    except Exception:
        news_list = []

    print("[PIPELINE] 유니버스 로드...")
    universe = layer0_universe()

    print("[PIPELINE] DART 매핑...")
    corp_map = get_dart_corp_map()

    candidates = []
    with ThreadPoolExecutor(max_workers=6) as ex:
        futures = {
            ex.submit(analyze_stock, row, corp_map, news_list): row
            for _, row in universe.iterrows()
        }
        for i, f in enumerate(as_completed(futures), 1):
            try:
                res = f.result()
                if res:
                    candidates.append(res)
            except Exception as e:
                print(f"[ERR] {e}")
            if i % 50 == 0:
                print(f"[PIPELINE] {i}/{len(universe)} | 후보 {len(candidates)}개")

    if not candidates:
        print("[PIPELINE] 후보 없음")
        return

    df = pd.DataFrame(candidates)
    df["_t"] = df["timing_true"].astype(int)
    df = df.sort_values(["_t", "total_score"], ascending=[False, False])
    df = df.drop(columns=["_t"]).head(50).reset_index(drop=True)
    df["rank"] = df.index + 1

    save_candidates(df)
    print(f"[PIPELINE] 완료: 상위 {len(df)}종목 저장")


if __name__ == "__main__":
    run_pipeline()
