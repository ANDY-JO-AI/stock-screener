"""
Andy Jo Stock AI — DART 공시 분석 엔진
대표이사 재임기간, CB/BW 감지, 자본잠식, 감사의견 등 핵심 조건 자동 분석
"""

import os
import time
import logging
import requests
import pandas as pd
from datetime import datetime, timedelta

log = logging.getLogger(__name__)

DART_API_KEY = os.environ.get("DART_API_KEY", "")
BASE_URL = "https://opendart.fss.or.kr/api"

# 즉시 탈락 공시 키워드 (제목 기반 1차 필터)
REJECT_KEYWORDS = [
    "전환사채", "신주인수권부사채", "CB", "BW",
    "유상증자", "제3자배정", "주식매수선택권",
    "횡령", "배임", "불성실공시", "상장폐지",
    "감사의견", "자본잠식", "워크아웃", "회생절차",
    "최대주주변경", "경영권변경"
]

# 위험 공시 키워드 (감점 처리)
WARNING_KEYWORDS = [
    "대표이사변경", "사임", "해임", "임원변경",
    "단기차입금", "채무보증", "담보제공"
]


# ────────────────────────────────────────
# 1. 기업코드 맵 로드
# ────────────────────────────────────────
def load_corp_code_map() -> dict:
    """DART 기업코드 XML 다운로드 → {종목코드: 기업코드} 딕셔너리 반환"""
    import zipfile
    import io
    from xml.etree import ElementTree as ET

    url = f"{BASE_URL}/corpCode.xml?crtfc_key={DART_API_KEY}"
    try:
        resp = requests.get(url, timeout=30)
        with zipfile.ZipFile(io.BytesIO(resp.content)) as z:
            with z.open("CORPCODE.xml") as f:
                tree = ET.parse(f)

        corp_map = {}
        for item in tree.getroot().findall("list"):
            stock_code = item.findtext("stock_code", "").strip()
            corp_code  = item.findtext("corp_code", "").strip()
            if stock_code:
                corp_map[stock_code] = corp_code

        log.info(f"기업코드 맵 로드 완료: {len(corp_map)}개")
        return corp_map

    except Exception as e:
        log.error(f"기업코드 맵 로드 실패: {e}")
        return {}


# ────────────────────────────────────────
# 2. 최근 공시 목록 조회 (제목 기반 필터)
# ────────────────────────────────────────
def fetch_recent_disclosures(corp_code: str, days: int = 90) -> list:
    """최근 N일 공시 목록 반환"""
    end_date   = datetime.now().strftime("%Y%m%d")
    start_date = (datetime.now() - timedelta(days=days)).strftime("%Y%m%d")

    url = (
        f"{BASE_URL}/list.json"
        f"?crtfc_key={DART_API_KEY}"
        f"&corp_code={corp_code}"
        f"&bgn_de={start_date}"
        f"&end_de={end_date}"
        f"&page_count=40"
    )
    try:
        resp = requests.get(url, timeout=10)
        data = resp.json()
        if data.get("status") == "000":
            return data.get("list", [])
    except Exception as e:
        log.debug(f"공시 목록 조회 실패 ({corp_code}): {e}")
    return []


# ────────────────────────────────────────
# 3. 대표이사 재임기간 분석 (10년 이상 = 합격)
# ────────────────────────────────────────
def fetch_ceo_tenure(corp_code: str) -> dict:
    """
    임원 현황 조회 → 대표이사 취임일 기준 재임기간 계산
    반환: {"name": 이름, "tenure_years": 재임년수, "pass": True/False}
    """
    url = (
        f"{BASE_URL}/exctvSttus.json"
        f"?crtfc_key={DART_API_KEY}"
        f"&corp_code={corp_code}"
        f"&bsns_year={datetime.now().year - 1}"
        f"&reprt_code=11011"
    )
    try:
        resp = requests.get(url, timeout=10)
        data = resp.json()
        if data.get("status") != "000":
            return {"name": "확인불가", "tenure_years": 0, "pass": False}

        ceo_list = [
            item for item in data.get("list", [])
            if "대표" in item.get("ofcps", "")
        ]

        if not ceo_list:
            return {"name": "확인불가", "tenure_years": 0, "pass": False}

        # 가장 오래된 대표이사 기준
        best = None
        max_tenure = 0

        for ceo in ceo_list:
            tenure_str = ceo.get("tenure_end_at", "") or ceo.get("rgbdt", "")
            # 취임일 파싱
            appoint_str = ceo.get("rgbdt", "")
            if not appoint_str:
                continue
            try:
                appoint_date = datetime.strptime(appoint_str[:10], "%Y.%m.%d")
            except Exception:
                try:
                    appoint_date = datetime.strptime(appoint_str[:8], "%Y%m%d")
                except Exception:
                    continue

            tenure_years = (datetime.now() - appoint_date).days / 365
            if tenure_years > max_tenure:
                max_tenure = tenure_years
                best = ceo

        if best:
            return {
                "name": best.get("nm", ""),
                "tenure_years": round(max_tenure, 1),
                "pass": max_tenure >= 10  # 시간여행TV 기준: 10년 이상
            }

    except Exception as e:
        log.debug(f"대표이사 조회 실패 ({corp_code}): {e}")

    return {"name": "확인불가", "tenure_years": 0, "pass": False}


# ────────────────────────────────────────
# 4. 재무제표 분석 (3년 연속 손실, 자본잠식)
# ────────────────────────────────────────
def fetch_financial_status(corp_code: str) -> dict:
    """
    최근 3개년 재무제표 조회
    반환: {
        "capital_erosion": True/False,   # 자본잠식
        "consecutive_loss": True/False,  # 3년 연속 영업손실
        "debt_ratio": float,             # 부채비율
        "revenue_growth": float,         # 매출 성장률
        "pass": True/False
    }
    """
    results = []
    current_year = datetime.now().year

    for year in [current_year - 1, current_year - 2, current_year - 3]:
        url = (
            f"{BASE_URL}/fnlttSinglAcntAll.json"
            f"?crtfc_key={DART_API_KEY}"
            f"&corp_code={corp_code}"
            f"&bsns_year={year}"
            f"&reprt_code=11011"
            f"&fs_div=CFS"
        )
        try:
            resp = requests.get(url, timeout=10)
            data = resp.json()
            if data.get("status") != "000":
                continue

            items = {
                item["account_nm"]: item
                for item in data.get("list", [])
                if item.get("fs_div") == "CFS"
            }

            def get_val(key):
                for k in items:
                    if key in k:
                        try:
                            return float(
                                items[k].get("thstrm_amount", "0")
                                .replace(",", "").replace("-", "-")
                            )
                        except Exception:
                            return 0
                return 0

            op_profit  = get_val("영업이익")
            equity     = get_val("자본총계")
            total_debt = get_val("부채총계")
            revenue    = get_val("매출액")

            results.append({
                "year": year,
                "op_profit": op_profit,
                "equity": equity,
                "total_debt": total_debt,
                "revenue": revenue
            })
            time.sleep(0.3)

        except Exception as e:
            log.debug(f"재무제표 조회 실패 ({corp_code}, {year}): {e}")

    if not results:
        return {"capital_erosion": False, "consecutive_loss": False,
                "debt_ratio": 0, "revenue_growth": 0, "pass": True}

    latest = results[0]

    # 자본잠식 판단
    capital_erosion = latest["equity"] <= 0

    # 3년 연속 영업손실 판단
    consecutive_loss = all(r["op_profit"] < 0 for r in results)

    # 부채비율
    debt_ratio = (
        (latest["total_debt"] / latest["equity"] * 100)
        if latest["equity"] > 0 else 9999
    )

    # 매출 성장률 (최신 vs 2년 전)
    if len(results) >= 2 and results[-1]["revenue"] > 0:
        revenue_growth = (
            (results[0]["revenue"] - results[-1]["revenue"])
            / results[-1]["revenue"] * 100
        )
    else:
        revenue_growth = 0

    passed = (
        not capital_erosion
        and not consecutive_loss
        and debt_ratio < 200
    )

    return {
        "capital_erosion": capital_erosion,
        "consecutive_loss": consecutive_loss,
        "debt_ratio": round(debt_ratio, 1),
        "revenue_growth": round(revenue_growth, 1),
        "pass": passed
    }


# ────────────────────────────────────────
# 5. 감사의견 조회
# ────────────────────────────────────────
def fetch_audit_opinion(corp_code: str) -> dict:
    """
    감사보고서 의견 조회
    반환: {"opinion": "적정"/"한정"/"부적정"/"의견거절", "pass": True/False}
    """
    url = (
        f"{BASE_URL}/hyslrSttus.json"
        f"?crtfc_key={DART_API_KEY}"
        f"&corp_code={corp_code}"
        f"&bsns_year={datetime.now().year - 1}"
        f"&reprt_code=11011"
    )
    try:
        resp = requests.get(url, timeout=10)
        data = resp.json()
        if data.get("status") == "000" and data.get("list"):
            opinion = data["list"][0].get("opinion", "확인불가")
            return {
                "opinion": opinion,
                "pass": opinion == "적정"
            }
    except Exception as e:
        log.debug(f"감사의견 조회 실패 ({corp_code}): {e}")

    return {"opinion": "확인불가", "pass": True}  # 조회 실패 시 통과 처리


# ────────────────────────────────────────
# 6. 최대주주 지분율 조회
# ────────────────────────────────────────
def fetch_major_shareholder(corp_code: str) -> dict:
    """
    최대주주 지분율 조회
    반환: {"name": 이름, "ratio": 지분율, "pass": True/False}
    조건: 최대주주 지분율 15% 이상 (시간여행TV 기준)
    """
    url = (
        f"{BASE_URL}/majorstock.json"
        f"?crtfc_key={DART_API_KEY}"
        f"&corp_code={corp_code}"
        f"&bsns_year={datetime.now().year - 1}"
        f"&reprt_code=11011"
    )
    try:
        resp = requests.get(url, timeout=10)
        data = resp.json()
        if data.get("status") == "000" and data.get("list"):
            item = data["list"][0]
            ratio_str = item.get("posesn_stock_co", "0").replace(",", "")
            total_str  = item.get("issu_stock_co", "0").replace(",", "")
            try:
                ratio = float(ratio_str) / float(total_str) * 100
            except Exception:
                ratio = 0

            return {
                "name": item.get("nm", ""),
                "ratio": round(ratio, 1),
                "pass": ratio >= 15  # 15% 이상 보유 = 경영 안정성
            }
    except Exception as e:
        log.debug(f"최대주주 조회 실패 ({corp_code}): {e}")

    return {"name": "확인불가", "ratio": 0, "pass": False}


# ────────────────────────────────────────
# 7. 공시 위험 신호 스캔 (제목 기반)
# ────────────────────────────────────────
def scan_disclosure_risk(corp_code: str) -> dict:
    """
    최근 90일 공시 제목 스캔
    반환: {
        "reject": True/False,        # 즉시 탈락 키워드 발견
        "warning": True/False,       # 경고 키워드 발견
        "reject_reasons": [...],     # 탈락 사유
        "warning_reasons": [...]     # 경고 사유
    }
    """
    disclosures = fetch_recent_disclosures(corp_code, days=90)
    titles = [d.get("report_nm", "") for d in disclosures]

    reject_reasons  = []
    warning_reasons = []

    for title in titles:
        for kw in REJECT_KEYWORDS:
            if kw in title:
                reject_reasons.append(f"{kw} ({title[:30]})")
        for kw in WARNING_KEYWORDS:
            if kw in title:
                warning_reasons.append(f"{kw} ({title[:30]})")

    return {
        "reject": len(reject_reasons) > 0,
        "warning": len(warning_reasons) > 0,
        "reject_reasons": list(set(reject_reasons)),
        "warning_reasons": list(set(warning_reasons))
    }


# ────────────────────────────────────────
# 8. 종목 전체 DART 분석 (메인 함수)
# ────────────────────────────────────────
def analyze_dart(stock_code: str, corp_code_map: dict) -> dict:
    """
    단일 종목 DART 전체 분석
    반환: {
        "stock_code": str,
        "dart_pass": True/False,      # 전체 합격 여부
        "dart_score": int,            # DART 점수 (최대 10점)
        "ceo": {...},
        "financial": {...},
        "audit": {...},
        "shareholder": {...},
        "disclosure_risk": {...},
        "reject_reason": str          # 탈락 사유 (탈락 시)
    }
    """
    corp_code = corp_code_map.get(stock_code)
    if not corp_code:
        return {
            "stock_code": stock_code,
            "dart_pass": False,
            "dart_score": 0,
            "reject_reason": "DART 기업코드 없음"
        }

    time.sleep(0.5)  # API 호출 간격 준수

    # 1. 공시 위험 스캔 (즉시 탈락 가능)
    risk = scan_disclosure_risk(corp_code)
    if risk["reject"]:
        return {
            "stock_code": stock_code,
            "dart_pass": False,
            "dart_score": 0,
            "disclosure_risk": risk,
            "reject_reason": f"위험공시: {risk['reject_reasons'][0]}"
        }

    # 2. 재무제표 분석
    financial = fetch_financial_status(corp_code)
    if not financial["pass"]:
        reason = []
        if financial["capital_erosion"]:
            reason.append("자본잠식")
        if financial["consecutive_loss"]:
            reason.append("3년 연속 영업손실")
        if financial["debt_ratio"] >= 200:
            reason.append(f"부채비율 {financial['debt_ratio']}%")
        return {
            "stock_code": stock_code,
            "dart_pass": False,
            "dart_score": 0,
            "financial": financial,
            "reject_reason": " / ".join(reason)
        }

    # 3. 감사의견
    audit = fetch_audit_opinion(corp_code)

    # 4. 대표이사 재임기간
    ceo = fetch_ceo_tenure(corp_code)

    # 5. 최대주주 지분율
    shareholder = fetch_major_shareholder(corp_code)

    # ── DART 점수 계산 (최대 10점) ──
    score = 0

    # 재무 건전성 (4점)
    if financial["debt_ratio"] < 100:
        score += 2
    elif financial["debt_ratio"] < 150:
        score += 1
    if financial["revenue_growth"] > 10:
        score += 2
    elif financial["revenue_growth"] > 0:
        score += 1

    # 대표이사 재임 (3점)
    if ceo["tenure_years"] >= 10:
        score += 3
    elif ceo["tenure_years"] >= 5:
        score += 2
    elif ceo["tenure_years"] >= 3:
        score += 1

    # 최대주주 지분율 (2점)
    if shareholder["ratio"] >= 30:
        score += 2
    elif shareholder["ratio"] >= 15:
        score += 1

    # 감사의견 적정 (1점)
    if audit["pass"]:
        score += 1

    # 경고 공시 감점
    if risk["warning"]:
        score = max(0, score - 1)

    dart_pass = (
        audit["pass"]
        and financial["pass"]
        and score >= 3
    )

    return {
        "stock_code": stock_code,
        "dart_pass": dart_pass,
        "dart_score": score,
        "ceo": ceo,
        "financial": financial,
        "audit": audit,
        "shareholder": shareholder,
        "disclosure_risk": risk,
        "reject_reason": "" if dart_pass else f"DART 점수 부족 ({score}점)"
    }


# ────────────────────────────────────────
# 9. 전체 종목 DART 신호 수집 (main.py 호출용)
# ────────────────────────────────────────
def fetch_dart_signals(stock_codes: list = None) -> dict:
    """
    주어진 종목 리스트에 대해 DART 분석 수행
    반환: {종목코드: analyze_dart 결과}
    """
    if not DART_API_KEY:
        log.warning("DART_API_KEY 없음 — 공시 분석 건너뜀")
        return {}

    corp_map = load_corp_code_map()
    if not corp_map:
        return {}

    # stock_codes 없으면 corp_map 전체 대상
    targets = stock_codes if stock_codes else list(corp_map.keys())

    results = {}
    total = len(targets)

    for i, code in enumerate(targets):
        if i % 50 == 0:
            log.info(f"  DART 분석 진행: {i}/{total}")
        results[code] = analyze_dart(code, corp_map)
        time.sleep(0.3)

    log.info(f"DART 분석 완료: 총 {len(results)}종목")
    return results
