"""
Andy Jo Stock AI — DART 공시·재무 엔진 v3
시간여행TV 기준 완전 구현

핵심 개선:
1. 공시 배치 조회 (전체 1회) — 종목별 반복 호출 제거
2. 재무 조회는 L0-L1 통과 종목만 대상
3. 즉시탈락 조건 완전 구현
"""

import os
import time
import logging
import zipfile
import io
import requests
import xml.etree.ElementTree as ET
from datetime import datetime, timedelta

log = logging.getLogger(__name__)

DART_API_KEY = os.environ.get("DART_API_KEY", "")
DART_BASE    = "https://opendart.fss.or.kr/api"

# ────────────────────────────────────────
# 즉시탈락 공시 키워드 (시간여행TV 기준)
# ────────────────────────────────────────
HARD_REJECT_KEYWORDS = [
    # 상장폐지·관리
    "상장폐지", "관리종목", "거래정지",
    # 횡령·배임
    "횡령", "배임", "불성실공시법인",
    # 구조적 위험
    "워크아웃", "회생절차", "파산신청", "영업정지",
    # 감사 위험
    "감사의견 부적정", "감사의견 거절", "감사범위 제한",
]

CB_BW_KEYWORDS = [
    "전환사채", "신주인수권부사채", "유상증자",
    "신주인수권 행사", "전환가액 조정", "제3자배정",
]

MONEY_LEAK_KEYWORDS = [
    "자금 대여", "자회사 대여", "계열사 대여금",
    "특수관계인 대여",
]

IR_PUFF_KEYWORDS = [
    "업무협약", "MOU", "양해각서", "신사업 진출",
    "사업 다각화",
]

# ────────────────────────────────────────
# 1. 전체 공시 배치 조회 (핵심 — 1회만 호출)
# ────────────────────────────────────────
def fetch_disclosure_batch(days: int = 14) -> dict:
    """
    DART 전체 공시 배치 조회 (타임아웃 + 빠른 실패 적용)
    반환: {corp_code: [title1, title2, ...]}
    """
    import datetime as dt

    end_dt   = dt.date.today()
    start_dt = end_dt - dt.timedelta(days=days)
    bgn_de   = start_dt.strftime("%Y%m%d")
    end_de   = end_dt.strftime("%Y%m%d")

    disclosure_map: dict = {}
    total_items = 0

    log.info(f"[dart_engine] 공시 배치 조회: {bgn_de} - {end_de}")

    for page in range(1, 21):   # 최대 20페이지
        try:
            resp = requests.get(
                f"{DART_BASE}/list.json",
                params={
                    "crtfc_key": DART_API_KEY,
                    "bgn_de":    bgn_de,
                    "end_de":    end_de,
                    "page_no":   page,
                    "page_count": 100,
                },
                timeout=10   # ★ 10초 타임아웃
            )
            data = resp.json()
        except requests.exceptions.Timeout:
            log.warning(f"  → 페이지 {page} 타임아웃 — 배치 조회 중단")
            break
        except Exception as e:
            log.warning(f"  → 페이지 {page} 오류: {e} — 중단")
            break

        if data.get("status") != "000":
            log.info(f"  → 페이지 {page} 종료 (status={data.get('status')})")
            break

        items = data.get("list", [])
        if not items:
            break

        for item in items:
            corp_code = item.get("corp_code", "")
            title     = item.get("report_nm", "")
            if corp_code:
                disclosure_map.setdefault(corp_code, []).append(title)
        total_items += len(items)

        if len(items) < 100:
            break   # 마지막 페이지

    log.info(f"[dart_engine] 공시 배치 완료: {len(disclosure_map)}개 기업 / {total_items}건")
    return disclosure_map


# ────────────────────────────────────────
# 2. 공시 위험도 분석 (메모리 처리 — API 추가 호출 없음)
# ────────────────────────────────────────
def analyze_disclosure_risk(corp_code, disclosure_map):
    """
    반환: {
        "hard_reject": bool,
        "reject_reason": str,
        "has_cb_bw": bool,
        "cb_bw_detail": str,
        "has_money_leak": bool,
        "ir_puff_count": int,
        "warn_flags": [str]
    }
    """
    titles = disclosure_map.get(corp_code, [])
    result = {
        "hard_reject":   False,
        "reject_reason": "",
        "has_cb_bw":     False,
        "cb_bw_detail":  "",
        "has_money_leak": False,
        "ir_puff_count": 0,
        "warn_flags":    []
    }

    if not titles:
        return result

    full_text = " ".join(titles)

    # 즉시탈락 체크
    for kw in HARD_REJECT_KEYWORDS:
        if kw in full_text:
            result["hard_reject"]   = True
            result["reject_reason"] = f"공시위험: {kw}"
            return result  # 즉시 반환

    # CB/BW/유증 체크 (2년 기준은 배치에서 14일 조회 중 — 추가 확인 필요)
    cb_found = []
    for kw in CB_BW_KEYWORDS:
        if kw in full_text:
            cb_found.append(kw)
    if cb_found:
        result["has_cb_bw"]    = True
        result["cb_bw_detail"] = ", ".join(cb_found)
        result["warn_flags"].append(f"CB/BW: {result['cb_bw_detail']}")

    # 자금대여 체크
    for kw in MONEY_LEAK_KEYWORDS:
        if kw in full_text:
            result["has_money_leak"] = True
            result["warn_flags"].append(f"자금대여 의심: {kw}")
            break

    # 호재 남발 체크
    puff_count = sum(1 for kw in IR_PUFF_KEYWORDS if kw in full_text)
    result["ir_puff_count"] = puff_count
    if puff_count >= 3:
        result["warn_flags"].append(f"IR남발 의심: {puff_count}건")

    return result


# ────────────────────────────────────────
# 3. 재무 데이터 배치 조회 (L1 통과 종목만)
# ────────────────────────────────────────
def fetch_financial_batch(stock_codes: list, corp_map: dict):
    """
    시간여행TV 재무 기준 항목 일괄 조회
    반환: {종목코드: 재무데이터 dict}
    """
    if not DART_API_KEY or not stock_codes:
        return {}

    results = {}
    total   = len(stock_codes)

    log.info(f"재무 데이터 조회 시작: {total}종목")

    for i, code in enumerate(stock_codes):
        corp_code = corp_map.get(code, "")
        if not corp_code:
            results[code] = _empty_financial()
            continue

        try:
            fin = _fetch_single_financial(corp_code)
            results[code] = fin
            time.sleep(0.3)  # DART API 과부하 방지
        except Exception as e:
            log.debug(f"재무 조회 실패 ({code}): {e}")
            results[code] = _empty_financial()

        if (i + 1) % 50 == 0:
            log.info(f"  재무 조회 진행: {i+1}/{total}")

    log.info(f"재무 조회 완료: {len(results)}종목")
    return results


def _empty_financial():
    """데이터 없을 때 기본값 — 통과 처리"""
    return {
        "op_profit_3y":    [None, None, None],  # 최근 3년 영업이익
        "op_profit_ok":    True,   # 3년 연속 적자 여부 (True=통과)
        "debt_ratio":      None,   # 부채비율 (%)
        "debt_ratio_ok":   True,   # 100% 미만 여부
        "capital_erosion": 0.0,    # 자본잠식률 (%)
        "erosion_ok":      True,   # 50% 미만 여부
        "revenue":         None,   # 매출액
        "equity":          None,   # 자기자본
        "net_assets":      None,   # 순자산
        "retained_earning": None,  # 이익잉여금
        "paid_in_capital": None,   # 납입자본금
        "roe":             None,   # ROE (%)
        "reserve_ratio":   None,   # 유보율 (%)
        "has_dividend":    False,  # 배당 여부
        "fin_score":       5,      # 재무점수 (데이터 없으면 중간값)
        "fin_score_detail": "재무데이터 없음(기본통과)",
        "data_available":  False,
    }


def _fetch_single_financial(corp_code: str):
    """단일 기업 재무 데이터 조회 및 점수 계산"""
    year = datetime.now().year

    # 3개년 재무 데이터 조회
    fin_data = {}
    for y in [year - 1, year - 2, year - 3]:
        try:
            params = {
                "crtfc_key": DART_API_KEY,
                "corp_code": corp_code,
                "bsns_year": str(y),
                "reprt_code": "11011",  # 사업보고서
                "fs_div":    "OFS",     # 별도재무제표
            }
            resp = requests.get(
                f"{DART_BASE}/fnlttSinglAcntAll.json",
                params=params, timeout=10
            )
            data = resp.json()
            if data.get("status") == "000":
                fin_data[y] = data.get("list", [])
        except Exception:
            fin_data[y] = []

    # 항목 파싱
    def get_amount(items, account_nm_keywords):
        """계정명 키워드로 금액 추출"""
        for item in items:
            nm = item.get("account_nm", "")
            for kw in account_nm_keywords:
                if kw in nm:
                    try:
                        val = item.get("thstrm_amount", "0")
                        return float(str(val).replace(",", "")) if val else None
                    except Exception:
                        return None
        return None

    result = _empty_financial()
    result["data_available"] = bool(fin_data)

    # 최근 3년 영업이익
    op_profits = []
    for y in [year - 1, year - 2, year - 3]:
        items = fin_data.get(y, [])
        op = get_amount(items, ["영업이익", "영업손익"])
        op_profits.append(op)

    result["op_profit_3y"] = op_profits

    # 3년 연속 적자 판단
    valid_profits = [p for p in op_profits if p is not None]
    if len(valid_profits) >= 2:
        loss_count = sum(1 for p in valid_profits if p < 0)
        result["op_profit_ok"] = not (loss_count >= len(valid_profits))
    
    # 최근 1년 재무 데이터 (부채비율 등)
    latest_items = fin_data.get(year - 1, [])

    if latest_items:
        # 자기자본
        equity = get_amount(latest_items, ["자본총계", "자기자본"])
        result["equity"] = equity

        # 총부채
        total_debt = get_amount(latest_items, ["부채총계"])

        # 부채비율
        if equity and equity > 0 and total_debt is not None:
            result["debt_ratio"] = round(total_debt / equity * 100, 1)
            result["debt_ratio_ok"] = result["debt_ratio"] < 100

        # 납입자본금
        paid_in = get_amount(latest_items, ["납입자본금", "자본금"])
        result["paid_in_capital"] = paid_in

        # 자본잠식률
        if paid_in and paid_in > 0 and equity is not None:
            erosion = (paid_in - equity) / paid_in * 100
            result["capital_erosion"] = round(erosion, 1)
            result["erosion_ok"] = erosion < 50

        # 매출액
        revenue = get_amount(latest_items, ["매출액", "영업수익"])
        result["revenue"] = revenue

        # 이익잉여금
        retained = get_amount(latest_items, ["이익잉여금", "결손금"])
        result["retained_earning"] = retained

        # 유보율 = 이익잉여금 / 납입자본금 × 100
        if retained and paid_in and paid_in > 0:
            result["reserve_ratio"] = round(retained / paid_in * 100, 1)

        # 당기순이익 (ROE 계산용)
        net_income = get_amount(latest_items, ["당기순이익", "당기순손익"])
        if net_income and equity and equity > 0:
            result["roe"] = round(net_income / equity * 100, 1)

    # ── 재무 점수 계산 (시간여행TV 기준) ──────────────
    score      = 0
    score_detail = []

    # 영업이익 흑자
    valid = [p for p in op_profits if p is not None]
    if len(valid) >= 3 and all(p > 0 for p in valid):
        score += 3
        score_detail.append("3년연속흑자(+3)")
    elif len(valid) >= 2 and all(p > 0 for p in valid[:2]):
        score += 1
        score_detail.append("2년흑자(+1)")
    elif valid and valid[0] and valid[0] > 0:
        score += 0
        score_detail.append("최근1년흑자만(+0)")
    else:
        score -= 2
        score_detail.append("적자이력(-2)")

    # 매출/시총 비율 (시총은 filter_engine에서 주입 — 여기선 매출만 저장)
    # (실제 비율 계산은 filter_engine에서 수행)

    # 부채비율
    dr = result.get("debt_ratio")
    if dr is not None:
        if dr < 50:
            score += 2
            score_detail.append(f"부채비율{dr:.0f}%<50%(+2)")
        elif dr < 100:
            score += 1
            score_detail.append(f"부채비율{dr:.0f}%<100%(+1)")
        else:
            score -= 2
            score_detail.append(f"부채비율{dr:.0f}%초과(-2)")

    # 유보율
    rr = result.get("reserve_ratio")
    if rr is not None:
        if rr >= 500:
            score += 2
            score_detail.append(f"유보율{rr:.0f}%(+2)")
        elif rr >= 300:
            score += 1
            score_detail.append(f"유보율{rr:.0f}%(+1)")

    # ROE
    roe = result.get("roe")
    if roe and roe >= 10:
        score += 1
        score_detail.append(f"ROE{roe:.1f}%(+1)")

    # 자본잠식
    erosion = result.get("capital_erosion", 0)
    if erosion > 50:
        score -= 3
        score_detail.append(f"자본잠식{erosion:.0f}%(-3)")
    elif erosion > 30:
        score -= 1
        score_detail.append(f"부분잠식{erosion:.0f}%(-1)")

    result["fin_score"]        = max(score, 0)
    result["fin_score_detail"] = " / ".join(score_detail)

    return result


# ────────────────────────────────────────
# 4. CB/BW 2년 이내 상세 확인
# ────────────────────────────────────────
def check_cb_bw_2years(corp_code):
    """
    최근 2년 CB/BW/유증 공시 존재 여부 확인
    반환: (bool, str) — (존재여부, 상세내용)
    """
    if not DART_API_KEY:
        return False, ""

    end   = datetime.now()
    start = end - timedelta(days=730)  # 2년

    try:
        params = {
            "crtfc_key": DART_API_KEY,
            "corp_code": corp_code,
            "bgn_de":    start.strftime("%Y%m%d"),
            "end_de":    end.strftime("%Y%m%d"),
            "page_count": 100,
        }
        resp  = requests.get(f"{DART_BASE}/list.json", params=params, timeout=10)
        data  = resp.json()
        items = data.get("list", [])

        found = []
        for item in items:
            title = item.get("report_nm", "")
            for kw in CB_BW_KEYWORDS:
                if kw in title:
                    found.append(f"{title[:30]}")
                    break

        return bool(found), " / ".join(found[:3])

    except Exception as e:
        log.debug(f"CB/BW 확인 실패 ({corp_code}): {e}")
        return False, ""
