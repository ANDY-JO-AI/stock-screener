"""
Andy Jo Stock AI — DART 공시 분석 엔진 v2
핵심 개선: 배치 공시 목록 선처리 + 위험 종목만 상세 분석
"""

import os, time, logging, zipfile, io, requests
import xml.etree.ElementTree as ET
import pandas as pd
from datetime import datetime, timedelta
from concurrent.futures import ThreadPoolExecutor, as_completed

log = logging.getLogger(__name__)

DART_API_KEY = os.environ.get("DART_API_KEY", "")
DART_BASE = "https://opendart.fss.or.kr/api"

# 즉시 탈락 공시 키워드
REJECT_KEYWORDS = [
    "횡령", "배임", "불성실공시법인", "상장폐지", "관리종목지정",
    "워크아웃", "회생절차", "영업정지"
]
# 주의 공시 키워드 (점수 감점)
WARN_KEYWORDS = [
    "전환사채", "신주인수권부사채", "유상증자", "제3자배정",
    "최대주주변경", "자본잠식"
]

# ────────────────────────────────────────
# 1. 기업코드 맵 로드
# ────────────────────────────────────────
def load_corp_code_map():
    try:
        url = f"{DART_BASE}/corpCode.xml?crtfc_key={DART_API_KEY}"
        resp = requests.get(url, timeout=30)
        with zipfile.ZipFile(io.BytesIO(resp.content)) as z:
            with z.open("CORPCODE.xml") as f:
                tree = ET.parse(f)
        corp_map = {}
        for item in tree.getroot().findall("list"):
            code = item.findtext("stock_code", "").strip()
            corp = item.findtext("corp_code", "").strip()
            if code:
                corp_map[code] = corp
        log.info(f"기업코드 맵 로드 완료: {len(corp_map)}개")
        return corp_map
    except Exception as e:
        log.error(f"기업코드 맵 로드 실패: {e}")
        return {}

# ────────────────────────────────────────
# 2. 전체 공시 목록 배치 조회 (핵심 개선)
#    → 종목별 반복 호출 대신 날짜 기준 전체 1회 조회
# ────────────────────────────────────────
def fetch_all_disclosures_batch(days=7):
    """
    최근 N일간 전체 공시 목록을 한 번에 가져와
    {corp_code: [공시제목, ...]} 딕셔너리 반환
    """
    try:
        end = datetime.now()
        start = end - timedelta(days=days)
        params = {
            "crtfc_key": DART_API_KEY,
            "bgn_de": start.strftime("%Y%m%d"),
            "end_de": end.strftime("%Y%m%d"),
            "page_count": 100,
            "page_no": 1,
        }
        all_items = {}
        for page in range(1, 11):  # 최대 10페이지 = 1,000건
            params["page_no"] = page
            resp = requests.get(f"{DART_BASE}/list.json", params=params, timeout=15)
            data = resp.json()
            if data.get("status") != "000":
                break
            items = data.get("list", [])
            if not items:
                break
            for item in items:
                corp = item.get("corp_code", "")
                title = item.get("report_nm", "")
                if corp not in all_items:
                    all_items[corp] = []
                all_items[corp].append(title)
        log.info(f"공시 배치 조회 완료: {len(all_items)}개 기업, {sum(len(v) for v in all_items.values())}건")
        return all_items
    except Exception as e:
        log.error(f"공시 배치 조회 실패: {e}")
        return {}

# ────────────────────────────────────────
# 3. 공시 위험도 분석 (메모리에서 즉시 처리)
# ────────────────────────────────────────
def analyze_disclosure_risk(corp_code, all_disclosures):
    titles = all_disclosures.get(corp_code, [])
    for title in titles:
        for kw in REJECT_KEYWORDS:
            if kw in title:
                return "REJECT", kw
    warn_found = []
    for title in titles:
        for kw in WARN_KEYWORDS:
            if kw in title:
                warn_found.append(kw)
    if warn_found:
        return "WARN", ", ".join(warn_found)
    return "OK", ""

# ────────────────────────────────────────
# 4. 메인 실행 함수 (전체 속도 대폭 개선)
# ────────────────────────────────────────
def fetch_dart_signals(stock_codes=None):
    """
    반환: {종목코드: {"pass": bool, "score": int, "reason": str, "risk": str}}
    """
    if not DART_API_KEY:
        log.warning("DART_API_KEY 없음 — 전체 통과 처리")
        if stock_codes:
            return {c: {"pass": True, "score": 5, "reason": "API키없음", "risk": "OK"} for c in stock_codes}
        return {}

    # STEP A: 기업코드 맵 로드 (1회)
    corp_map = load_corp_code_map()
    if not corp_map:
        log.warning("기업코드 맵 없음 — 전체 통과 처리")
        if stock_codes:
            return {c: {"pass": True, "score": 5, "reason": "코드맵없음", "risk": "OK"} for c in stock_codes}
        return {}

    # STEP B: 전체 공시 목록 배치 조회 (1회 — 핵심 개선)
    log.info("공시 목록 배치 조회 시작 (전체 1회)")
    all_disclosures = fetch_all_disclosures_batch(days=7)

    # STEP C: 각 종목 위험도 즉시 분석 (메모리 처리 — API 추가 호출 없음)
    results = {}
    targets = stock_codes if stock_codes else list(corp_map.keys())
    total = len(targets)

    log.info(f"DART 분석 시작: {total}종목 (메모리 처리)")
    for i, code in enumerate(targets):
        corp_code = corp_map.get(code, "")
        if not corp_code:
            results[code] = {"pass": True, "score": 5, "reason": "기업코드없음", "risk": "OK"}
            continue

        risk, reason = analyze_disclosure_risk(corp_code, all_disclosures)

        if risk == "REJECT":
            results[code] = {"pass": False, "score": 0, "reason": f"즉시탈락: {reason}", "risk": "REJECT"}
        elif risk == "WARN":
            results[code] = {"pass": True, "score": 3, "reason": f"주의: {reason}", "risk": "WARN"}
        else:
            results[code] = {"pass": True, "score": 7, "reason": "공시이상없음", "risk": "OK"}

        if (i + 1) % 100 == 0:
            log.info(f"  DART 분석 진행: {i+1}/{total}")

    reject_count = sum(1 for v in results.values() if not v["pass"])
    warn_count = sum(1 for v in results.values() if v["risk"] == "WARN")
    log.info(f"DART 분석 완료: 총 {len(results)}종목 | 탈락 {reject_count} / 주의 {warn_count} / 정상 {len(results)-reject_count-warn_count}")
    return results
