import requests
import pandas as pd
import time
import FinanceDataReader as fdr
from datetime import datetime, timedelta
from bs4 import BeautifulSoup
import zipfile
import io
import xml.etree.ElementTree as ET

DART_API_KEY = "7d2191837b9373fc6f049fd6fa30d7678f2f96f6"
TODAY        = datetime.today()
TODAY_STR    = TODAY.strftime('%Y-%m-%d')
THREE_YRS    = (TODAY - timedelta(days=365*3)).strftime('%Y-%m-%d')
ONE_YR       = (TODAY - timedelta(days=365)).strftime('%Y-%m-%d')
TWO_YRS      = (TODAY - timedelta(days=365*2)).strftime('%Y-%m-%d')

THEME_DICT = {
    "🏛️정치/선거":    ["정치","선거","국회","대선","총선","지방선거","여당","야당","정권"],
    "🛡️방산/안보":    ["방산","국방","무기","미사일","드론","안보","군사","K-방산"],
    "🤖AI/로봇":      ["AI","인공지능","로봇","자동화","챗GPT","딥러닝","머신러닝","LLM"],
    "⚡에너지/전력":  ["전력","에너지","태양광","풍력","ESS","배터리","전기차","수소"],
    "💊바이오/헬스":  ["바이오","헬스","신약","임상","의약","의료기기","제약","mRNA"],
    "🌾농업/식품":    ["농업","식품","곡물","사료","비료","식량","유기농"],
    "🚗자동차부품":   ["자동차","부품","EV","전기차부품","모터","변속기","자율주행"],
    "🎬미디어/콘텐츠":["드라마","웹툰","콘텐츠","엔터","방송","OTT","K-콘텐츠","영화"],
    "🏗️건설/부동산":  ["건설","부동산","재개발","분양","아파트","GTX","리츠"],
    "❄️계절/기후":    ["폭염","한파","홍수","기후","냉방","난방","방재","재해"],
    "💰화폐/금융":    ["코인","가상화폐","비트코인","금융","핀테크","증권","토큰"],
    "🏭제조/소재":    ["소재","화학","철강","알루미늄","희토류","반도체소재","부품"],
    "📡IT/통신":      ["통신","5G","6G","네트워크","위성","IoT","클라우드","데이터센터"],
    "👶저출산/복지":  ["저출산","복지","육아","보육","노인","실버","교육"],
    "🕊️대북/통일":   ["대북","통일","남북","북한","경협","개성"],
}

BAD_DISCLOSURE_KW = [
    "전환사채","신주인수권부사채","유상증자",
    "전환가액 조정","제3자배정","사모사채"
]

GOOD_NEWS_KW = [
    "계약","수주","MOU","협약","공급","납품",
    "신사업","진출","개발","출시","허가","승인"
]

# ══════════════════════════════════════════════════════
# DART 유틸
# ══════════════════════════════════════════════════════
def load_corp_code_map():
    try:
        url  = f"https://opendart.fss.or.kr/api/corpCode.xml?crtfc_key={DART_API_KEY}"
        r    = requests.get(url, timeout=30)
        z    = zipfile.ZipFile(io.BytesIO(r.content))
        xml  = z.read('CORPCODE.xml')
        root = ET.fromstring(xml)
        return {
            item.findtext('stock_code','').strip(): item.findtext('corp_code','').strip()
            for item in root.findall('list')
            if len(item.findtext('stock_code','').strip()) == 6
        }
    except:
        return {}

def dart_get(endpoint, params):
    try:
        params['crtfc_key'] = DART_API_KEY
        r = requests.get(
            f"https://opendart.fss.or.kr/api/{endpoint}",
            params=params, timeout=15
        )
        return r.json()
    except:
        return {}

def get_dart_financials(corp_code, year):
    data = dart_get("fnlttSinglAcntAll.json", {
        "corp_code":  corp_code,
        "bsns_year":  str(year),
        "reprt_code": "11011",
        "fs_div":     "OFS"
    })
    return data.get("list", [])

def extract_amount(fin_list, account_name):
    for item in fin_list:
        if item.get("account_nm","") == account_name:
            val = item.get("thstrm_amount","").replace(",","").replace(" ","")
            try:
                return round(int(val) / 1e8, 2)
            except:
                return None
    return None

# ══════════════════════════════════════════════════════
# LAYER 0 — 유니버스 (KRX 직접 호출 제거, FDR만 사용)
# ══════════════════════════════════════════════════════
def layer0_universe():
    results, logs = [], []
    for market in ['KOSDAQ', 'KOSPI']:
        try:
            df = fdr.StockListing(market)
        except Exception as e:
            logs.append(('', market, f"L0: 종목 로드 실패 {e}"))
            continue
        for _, row in df.iterrows():
            code   = str(row.get('Code', '')).zfill(6)
            name   = str(row.get('Name', ''))
            marcap = float(row.get('Marcap', 0) or 0)
            if not code or not name:
                continue
            if marcap < 150_0000_0000:
                logs.append((code, name, "L0: 시총 150억 미달"))
                continue
            if market == 'KOSDAQ':
                limit = 500_0000_0000 if marcap < 600_0000_0000 else 700_0000_0000
            else:
                limit = 700_0000_0000
            if marcap > limit:
                logs.append((code, name, f"L0: 시총 {round(marcap/1e8)}억 상한 초과"))
                continue
            excl_kw = ['우','스팩','SPAC','우선','홀딩스우','차이나','중국','CR']
            if any(kw in name for kw in excl_kw):
                logs.append((code, name, "L0: 우선주/스팩/중국 제외"))
                continue
            results.append({
                '종목코드': code, '종목명': name,
                '시장': market, '시가총액(억)': round(marcap / 1e8)
            })
    return pd.DataFrame(results), logs

# ══════════════════════════════════════════════════════
# LAYER 1 — 하드 탈락 필터
# ══════════════════════════════════════════════════════
def layer1_hard_filter(df, corp_map):
    results, logs = [], []
    for _, row in df.iterrows():
        code   = row['종목코드']
        name   = row['종목명']
        marcap = row['시가총액(억)'] * 1e8
        market = row['시장']
        corp   = corp_map.get(code)
        fail   = False

        # 1-A. 52주 저가 대비 주가 위치
        try:
            dp    = fdr.DataReader(code, ONE_YR, TODAY_STR)
            if dp is None or len(dp) < 20:
                logs.append((code, name, "L1: 주가 데이터 부족"))
                continue
            low_52  = dp['Low'].min()
            cur_prc = dp['Close'].iloc[-1]
            ratio   = 1.5 if market == 'KOSPI' else 1.7
            if cur_prc > low_52 * ratio:
                logs.append((code, name, f"L1: 현재가>{ratio}배 탈락"))
                continue
        except Exception as e:
            logs.append((code, name, f"L1: 주가 오류 {e}"))
            continue

        # 1-B. DART 재무 (3년)
        op_list, debt_list, equity_list = [], [], []
        paid_in_cap = None
        if corp:
            for yr in [TODAY.year-3, TODAY.year-2, TODAY.year-1]:
                fin  = get_dart_financials(corp, yr)
                time.sleep(0.15)
                op   = extract_amount(fin, "영업이익")
                de   = extract_amount(fin, "부채총계")
                eq   = extract_amount(fin, "자본총계")
                pc   = extract_amount(fin, "납입자본금")
                op_list.append(op)
                debt_list.append(de)
                equity_list.append(eq)
                if pc:
                    paid_in_cap = pc

            valid_op = [op for op in op_list if op is not None]
            if len(valid_op) >= 3 and all(op <= 0 for op in valid_op):
                logs.append((code, name, "L1: 3년 연속 영업손실"))
                fail = True

            if not fail and debt_list[-1] and equity_list[-1] and equity_list[-1] > 0:
                dr = debt_list[-1] / equity_list[-1] * 100
                if dr >= 100:
                    logs.append((code, name, f"L1: 부채비율 {round(dr)}%"))
                    fail = True

            if not fail and paid_in_cap and equity_list[-1] is not None and paid_in_cap > 0:
                erosion = (paid_in_cap - equity_list[-1]) / paid_in_cap * 100
                if erosion >= 50:
                    logs.append((code, name, f"L1: 자본잠식 {round(erosion)}%"))
                    fail = True

        if fail:
            continue

        # 1-C. CB/BW/유증 공시
        if corp:
            disc = dart_get("list.json", {
                "corp_code":  corp,
                "bgn_de":     (TODAY - timedelta(days=730)).strftime('%Y%m%d'),
                "end_de":     TODAY.strftime('%Y%m%d'),
                "page_count": 100
            })
            time.sleep(0.15)
            for item in disc.get("list", []):
                title = item.get("report_nm", "")
                if any(kw in title for kw in BAD_DISCLOSURE_KW):
                    logs.append((code, name, f"L1: 불량공시 [{title[:30]}]"))
                    fail = True
                    break
        if fail:
            continue

        # 1-D. 호재 뉴스 남발
        good_count = count_good_news(name)
        time.sleep(0.2)
        if good_count >= 4:
            logs.append((code, name, f"L1: 호재뉴스 {good_count}건"))
            continue

        row_out = row.to_dict()
        row_out.update({
            '_dp': dp, '_corp': corp,
            '현재가': cur_prc, '52주최저가': low_52,
            '영업이익_3년': op_list,
            '부채_3년': debt_list,
            '자본_3년': equity_list,
            '납입자본금': paid_in_cap,
        })
        results.append(row_out)

    return pd.DataFrame(results), logs

# ══════════════════════════════════════════════════════
# LAYER 2 — 재무 점수
# ══════════════════════════════════════════════════════
def layer2_finance_score(df):
    for idx, row in df.iterrows():
        score, detail = 0, []
        op_list   = row.get('영업이익_3년', [])
        debt_list = row.get('부채_3년', [])
        eq_list   = row.get('자본_3년', [])
        marcap    = row['시가총액(억)'] * 1e8
        corp      = row.get('_corp')

        valid_op = [op for op in op_list if op is not None]
        if len(valid_op) >= 3 and all(op > 0 for op in valid_op):
            score += 3; detail.append("3년흑자+3")
        elif len(valid_op) >= 2 and all(op > 0 for op in valid_op[-2:]):
            score += 1; detail.append("2년흑자+1")

        rev = None
        if corp:
            fin_l = get_dart_financials(corp, TODAY.year-1)
            time.sleep(0.15)
            rev = extract_amount(fin_l, "매출액")
            if rev is None:
                rev = extract_amount(fin_l, "수익(매출액)")
        if rev and marcap > 0:
            ratio = (rev * 1e8) / marcap
            if ratio >= 1.0:   score += 2; detail.append("매출/시총≥1+2")
            elif ratio >= 0.5: score += 1; detail.append("매출/시총≥0.5+1")

        if len(debt_list) >= 2 and len(eq_list) >= 2:
            dr_prev = (debt_list[-2]/eq_list[-2]*100) if eq_list[-2] else None
            dr_cur  = (debt_list[-1]/eq_list[-1]*100) if eq_list[-1] else None
            if dr_prev and dr_cur:
                if dr_cur < dr_prev: score += 1; detail.append("부채감소+1")
                if dr_cur < 50:      score += 2; detail.append("부채<50%+2")
                elif dr_cur < 100:   score += 1; detail.append("부채<100%+1")

        if corp:
            fin_l2 = get_dart_financials(corp, TODAY.year-1)
            time.sleep(0.15)
            retained = extract_amount(fin_l2, "이익잉여금")
            paid_cap = row.get('납입자본금')
            if retained and paid_cap and paid_cap > 0:
                rr = retained / paid_cap * 100
                if rr >= 500:   score += 2; detail.append("적립금≥500%+2")
                elif rr >= 300: score += 1; detail.append("적립금≥300%+1")

        if eq_list and eq_list[-1] and eq_list[-1] > 0 and corp:
            fin_l3 = get_dart_financials(corp, TODAY.year-1)
            time.sleep(0.15)
            ni = extract_amount(fin_l3, "당기순이익")
            if ni and ni / eq_list[-1] * 100 >= 10:
                score += 1; detail.append("ROE≥10%+1")

        if corp:
            div = dart_get("alotMatter.json", {
                "corp_code": corp,
                "bgn_de": TWO_YRS.replace('-',''),
                "end_de": TODAY_STR.replace('-',''),
            })
            time.sleep(0.15)
            if div.get("list"):
                score += 1; detail.append("배당이력+1")

        if eq_list and eq_list[-1] and (eq_list[-1] * 1e8) > marcap:
            score += 2; detail.append("순자산>시총+2")

        df.at[idx, '재무점수'] = score
        df.at[idx, '재무상세'] = ' / '.join(detail)
        df.at[idx, '매출액(억)'] = rev

    return df

# ══════════════════════════════════════════════════════
# LAYER 3 — 유동성
# ══════════════════════════════════════════════════════
def layer3_liquidity(df):
    results, logs = [], []
    for _, row in df.iterrows():
        code, name = row['종목코드'], row['종목명']
        try:
            dp3 = fdr.DataReader(code, THREE_YRS, TODAY_STR)
            dp3['거래대금'] = dp3['Close'] * dp3['Volume']
        except:
            logs.append((code, name, "L3: 3년 데이터 실패"))
            continue
        peak_vol   = dp3['거래대금'].max()
        recent_avg = dp3['거래대금'].tail(10).mean()
        if peak_vol < 100_0000_0000:
            logs.append((code, name, f"L3: 최대거래대금 {round(peak_vol/1e8,1)}억"))
            continue
        row_out = row.to_dict()
        row_out.update({
            '_dp3': dp3,
            '3년최대거래대금(억)':      round(peak_vol/1e8, 1),
            '최근10일평균거래대금(억)': round(recent_avg/1e8, 1),
        })
        results.append(row_out)
    return pd.DataFrame(results), logs

# ══════════════════════════════════════════════════════
# LAYER 4 — 주주 구조
# ══════════════════════════════════════════════════════
def layer4_shareholder(df):
    for idx, row in df.iterrows():
        corp = row.get('_corp')
        if not corp:
            df.at[idx, '최대주주지분(%)'] = None
            df.at[idx, '주주구조등급']    = '확인불가'
            continue
        try:
            data  = dart_get("majorstock.json", {
                "corp_code":  corp,
                "bsns_year":  str(TODAY.year-1),
                "reprt_code": "11011"
            })
            time.sleep(0.15)
            items = data.get("list", [])
            if not items:
                df.at[idx, '최대주주지분(%)'] = None
                df.at[idx, '주주구조등급']    = '데이터없음'
                continue
            top   = float(str(items[0].get('trmend_posesn_stock_co','0')).replace(',','') or 0)
            total = float(str(items[0].get('trmend_tot_stock_co','1')).replace(',','') or 1)
            ratio = round(top / total * 100, 1) if total > 0 else 0
            df.at[idx, '최대주주지분(%)'] = ratio
            if ratio < 30 or ratio >= 70:
                df.at[idx, '주주구조등급'] = '✅선호'
            elif ratio < 50:
                df.at[idx, '주주구조등급'] = '⚠️경고'
            else:
                df.at[idx, '주주구조등급'] = '🔴주의'
        except:
            df.at[idx, '최대주주지분(%)'] = None
            df.at[idx, '주주구조등급']    = '조회오류'
    return df

# ══════════════════════════════════════════════════════
# LAYER 5 — 테마
# ══════════════════════════════════════════════════════
def get_naver_news(name):
    hdrs = {"User-Agent": "Mozilla/5.0"}
    url  = f"https://search.naver.com/search.naver?where=news&query={requests.utils.quote(name)}&sort=1"
    try:
        soup   = BeautifulSoup(requests.get(url, headers=hdrs, timeout=10).text, 'html.parser')
        items  = soup.select('.news_tit')[:10]
        titles = [i.get_text() for i in items]
        return " ".join(titles), titles[:3]
    except:
        return "", []

def count_good_news(name):
    text, _ = get_naver_news(name)
    return sum(1 for kw in GOOD_NEWS_KW if kw in text)

def layer5_theme(df):
    for idx, row in df.iterrows():
        name = row['종목명']
        code = row['종목코드']
        text, headlines = get_naver_news(name)
        time.sleep(0.3)
        matched = [t for t, kws in THEME_DICT.items() if any(k in text for k in kws)]
        score   = 0
        political = [t for t in matched if "정치" in t or "선거" in t]
        policy    = [t for t in matched if any(x in t for x in ["방산","에너지","AI","로봇"])]
        seasonal  = [t for t in matched if "계절" in t or "기후" in t]
        social    = [t for t in matched if any(x in t for x in ["저출산","복지","대북"])]
        score += min(len(political)*2, 4)
        score += min(len(policy)*2, 4)
        score += min(len(seasonal), 2)
        score += min(len(social), 2)
        try:
            dp5   = fdr.DataReader(code, (TODAY-timedelta(days=365*5)).strftime('%Y-%m-%d'), TODAY_STR)
            max_r = (dp5['Close'].max() - dp5['Close'].iloc[0]) / dp5['Close'].iloc[0] * 100
            if max_r >= 200:
                score += 2; matched.append("📈5년급등이력")
        except:
            pass
        df.at[idx, '테마분류'] = ' / '.join(matched) if matched else '해당없음'
        df.at[idx, '테마점수'] = min(score, 10)
        df.at[idx, '최신뉴스'] = ' | '.join(headlines)
    return df

# ══════════════════════════════════════════════════════
# LAYER 6 — 매수 타이밍 + 세력 매집
# ══════════════════════════════════════════════════════
def get_market_change():
    try:
        kq = fdr.DataReader('KQ11', (TODAY-timedelta(days=10)).strftime('%Y-%m-%d'), TODAY_STR)
        if len(kq) >= 2:
            return round((kq['Close'].iloc[-1]-kq['Close'].iloc[-2])/kq['Close'].iloc[-2]*100, 2)
    except:
        pass
    return 0.0

def layer6_timing_accumulation(df):
    market_chg = get_market_change()
    for idx, row in df.iterrows():
        dp      = row.get('_dp3') or row.get('_dp')
        cur_prc = row.get('현재가', 0)
        low_52  = row.get('52주최저가', 0)
        score, signals = 0, []
        if dp is not None and len(dp) >= 20:
            dp['거래대금'] = dp['Close'] * dp['Volume']
            vol_60 = dp['Volume'].tail(60).mean()
            vol_5  = dp['Volume'].tail(5).mean()
            if vol_60 > 0:
                vr = vol_5 / vol_60
                if vr >= 5.0:   score += 3; signals.append(f"🔥거래량{round(vr,1)}배급증")
                elif vr >= 3.0: score += 2; signals.append(f"⚡거래량{round(vr,1)}배증가")
                elif vr >= 1.5: score += 1; signals.append(f"거래량{round(vr,1)}배")
            r5 = dp.tail(5)
            if r5['Low'].min() > 0:
                rng = (r5['High'].max()-r5['Low'].min())/r5['Low'].min()*100
                if rng < 3.0:   score += 2; signals.append(f"횡보({round(rng,1)}%)")
                elif rng < 5.0: score += 1; signals.append(f"좁은범위({round(rng,1)}%)")
            r3 = dp.tail(3)
            if all(r3['Close'].values[i] >= r3['Open'].values[i] for i in range(len(r3))):
                score += 2; signals.append("3일연속양봉")
            if cur_prc and low_52 and low_52 > 0:
                prx = cur_prc / low_52
                if prx <= 1.3:   score += 2; signals.append(f"저가×{round(prx,2)}(근접)")
                elif prx <= 1.5: score += 1; signals.append(f"저가×{round(prx,2)}")
            recent10 = dp['거래대금'].tail(10).mean()
            if recent10 <= 80_0000_0000:
                score += 1; signals.append("거래대금80억↓")
            if len(dp) >= 2:
                last_chg = (dp['Close'].iloc[-1]-dp['Close'].iloc[-2])/dp['Close'].iloc[-2]*100
                if last_chg <= -5:
                    score -= 2; signals.append("⚠️대형음봉")
        if market_chg <= -1.5:
            score += 1; signals.append(f"시장하락{market_chg}%")
        df.at[idx, '매집점수']   = max(score, 0)
        df.at[idx, '매집신호']   = ' | '.join(signals) if signals else '신호없음'
        df.at[idx, '시장등락률'] = market_chg
    return df

# ══════════════════════════════════════════════════════
# 종합 등급
# ══════════════════════════════════════════════════════
def calc_final_grade(df):
    df['재무점수'] = pd.to_numeric(df.get('재무점수', 0), errors='coerce').fillna(0)
    df['테마점수'] = pd.to_numeric(df.get('테마점수', 0), errors='coerce').fillna(0)
    df['매집점수'] = pd.to_numeric(df.get('매집점수', 0), errors='coerce').fillna(0)
    df['종합점수'] = df['재무점수'] + df['테마점수'] + df['매집점수']
    def grade(s):
        if s >= 16:  return "⭐핵심후보"
        elif s >= 11: return "🔵우선후보"
        elif s >= 6:  return "🟡관심"
        else:         return "⚪참고"
    df['등급'] = df['종합점수'].apply(grade)
    return df.sort_values('종합점수', ascending=False)

# ══════════════════════════════════════════════════════
# 메인 파이프라인
# ══════════════════════════════════════════════════════
def run_pipeline(progress_callback=None):
    all_logs = []
    def log(msg):
        if progress_callback:
            progress_callback(msg)

    log("📦 DART 기업코드 로드 중...")
    corp_map = load_corp_code_map()
    log(f"  → {len(corp_map)}개 로드 완료")

    log("\n🔵 LAYER 0: 유니버스 구성 중...")
    df0, l0 = layer0_universe()
    all_logs += l0
    log(f"  → 통과 {len(df0)}개 | 탈락 {len(l0)}개")

    log("\n🔴 LAYER 1: 하드 탈락 필터...")
    df1, l1 = layer1_hard_filter(df0, corp_map)
    all_logs += l1
    log(f"  → 통과 {len(df1)}개 | 탈락 {len(l1)}개")
    if df1.empty:
        return pd.DataFrame(), pd.DataFrame(all_logs, columns=['코드','종목명','사유'])

    log("\n🟡 LAYER 2: 재무 점수 계산 중...")
    df2 = layer2_finance_score(df1)
    log("  → 완료")

    log("\n🟢 LAYER 3: 유동성 필터...")
    df3, l3 = layer3_liquidity(df2)
    all_logs += l3
    log(f"  → 통과 {len(df3)}개 | 탈락 {len(l3)}개")
    if df3.empty:
        return pd.DataFrame(), pd.DataFrame(all_logs, columns=['코드','종목명','사유'])

    log("\n👥 LAYER 4: 주주 구조 분석 중...")
    df4 = layer4_shareholder(df3)
    log("  → 완료")

    log("\n🎯 LAYER 5: 테마 분류 중...")
    df5 = layer5_theme(df4)
    log("  → 완료")

    log("\n🔥 LAYER 6: 매집 패턴 감지 중...")
    df6 = layer6_timing_accumulation(df5)
    log("  → 완료")

    log("\n🏁 종합 등급 산정 중...")
    df_final = calc_final_grade(df6)
    drop_cols = ['_dp','_dp3','_corp','영업이익_3년','부채_3년','자본_3년','납입자본금']
    df_final  = df_final.drop(columns=[c for c in drop_cols if c in df_final.columns])
    fail_df   = pd.DataFrame(all_logs, columns=['코드','종목명','탈락사유'])
    log(f"\n✅ 완료! 최종 후보: {len(df_final)}개")
    return df_final, fail_df
