"""
Andy Jo Stock AI — 시장 데이터 배치 엔진
pykrx 전종목 1회 배치 조회 전담
종목별 개별 API 호출 완전 제거
"""

import os
import logging
import zipfile
import io
import requests
import pandas as pd
from datetime import datetime, timedelta

log = logging.getLogger(__name__)

DART_API_KEY = os.environ.get("DART_API_KEY", "")
DART_BASE = "https://opendart.fss.or.kr/api"

# 시간여행TV 기준 시총 범위
MKTCAP_MIN_억 = 150
MKTCAP_MAX_억 = 700

# 즉시 제거 종목명 패턴
REJECT_NAME_PATTERNS = [
    "스팩", "SPAC", "spac",
    "리츠", "REIT",
    "ETF", "ETN",
    "우", "B"  # 우선주 (예: 삼성전자우)
]

# ────────────────────────────────────────
# 1. 영업일 계산
# ────────────────────────────────────────
def get_recent_business_day(offset=0):
    """
    최근 영업일 반환 (주말 건너뜀)
    offset=0: 오늘 또는 가장 최근 영업일
    offset=1: 그 전 영업일
    """
    date = datetime.now() - timedelta(days=offset)
    # 최대 10일 전까지 탐색
    for _ in range(10):
        if date.weekday() < 5:  # 월-금
            return date.strftime("%Y%m%d")
        date -= timedelta(days=1)
    return datetime.now().strftime("%Y%m%d")


# ────────────────────────────────────────
# 2. pykrx 전종목 배치 조회 (핵심)
# ────────────────────────────────────────
def fetch_market_batch(market="KOSDAQ"):
    """
    pykrx로 전종목 OHLCV + 시가총액 1회 배치 조회
    반환: DataFrame (Code, Name, Close, Volume, ChangeRatio, Marcap_억, ...)
    """
    try:
        from pykrx import stock as krx

        today_str = get_recent_business_day(0)
        prev_str  = get_recent_business_day(3)

        log.info(f"pykrx 배치 조회: {market} ({today_str})")

        # 전종목 OHLCV (1회 호출)
        df_ohlcv = krx.get_market_ohlcv_by_ticker(today_str, market=market)
        if df_ohlcv is None or df_ohlcv.empty:
            log.warning(f"OHLCV 데이터 없음 — 전일 재시도")
            today_str = get_recent_business_day(1)
            df_ohlcv = krx.get_market_ohlcv_by_ticker(today_str, market=market)

        # 전종목 시가총액 (1회 호출)
        df_cap = krx.get_market_cap_by_ticker(today_str, market=market)

        # 전종목 종목명 (1회 호출)
        ticker_list = krx.get_market_ticker_list(today_str, market=market)
        name_map = {}
        for ticker in ticker_list:
            try:
                name_map[ticker] = krx.get_market_ticker_name(ticker)
            except Exception:
                name_map[ticker] = ticker

        # 병합
        df = df_ohlcv.copy()
        df.index.name = "Code"
        df = df.reset_index()

        # 컬럼 한글 → 영문 변환
        col_map = {
            "시가": "Open",
            "고가": "High",
            "저가": "Low",
            "종가": "Close",
            "거래량": "Volume",
            "거래대금": "Turnover",
            "등락률": "ChangeRatio"
        }
        df = df.rename(columns=col_map)

        # 시가총액 병합
        if df_cap is not None and not df_cap.empty:
            df_cap = df_cap.reset_index()
            df_cap.columns = ["Code"] + list(df_cap.columns[1:])
            cap_col_map = {"시가총액": "Marcap", "상장주식수": "Shares"}
            df_cap = df_cap.rename(columns=cap_col_map)
            df = df.merge(
                df_cap[["Code", "Marcap"]],
                on="Code", how="left"
            )
        else:
            # 시가총액 없으면 추정
            df["Marcap"] = df["Close"] * df.get("Shares", 1000000)

        # 종목명 추가
        df["Name"] = df["Code"].map(name_map).fillna("")

        # 시가총액 억원 변환
        df["Marcap_억"] = df["Marcap"] / 1e8

        # 전일 대비 거래대금 (Turnover 있으면 사용)
        if "Turnover" not in df.columns:
            df["Turnover"] = df["Close"] * df["Volume"]
        df["Turnover_억"] = df["Turnover"] / 1e8

        log.info(f"pykrx 배치 조회 완료: {len(df)}종목")
        return df

    except Exception as e:
        log.error(f"pykrx 배치 조회 실패: {e}")
        return pd.DataFrame()


# ────────────────────────────────────────
# 3. 52주 고저가 배치 조회
# ────────────────────────────────────────
def fetch_52week_data(code_list: list):
    """
    52주 최저가·최고가 배치 조회
    반환: {종목코드: {"low52": float, "high52": float}}
    """
    result = {}
    try:
        from pykrx import stock as krx

        end_str   = get_recent_business_day(0)
        start_str = (datetime.now() - timedelta(days=365)).strftime("%Y%m%d")

        log.info(f"52주 데이터 조회: {len(code_list)}종목")

        # 종목별 조회 (불가피 — 단, L0-L1 통과 종목만 대상)
        for i, code in enumerate(code_list):
            try:
                df = krx.get_market_ohlcv_by_date(start_str, end_str, code)
                if df is not None and not df.empty:
                    low_col  = "저가" if "저가" in df.columns else "Low"
                    high_col = "고가" if "고가" in df.columns else "High"
                    result[code] = {
                        "low52":  float(df[low_col].min()),
                        "high52": float(df[high_col].max())
                    }
                else:
                    result[code] = {"low52": 0, "high52": 0}
            except Exception:
                result[code] = {"low52": 0, "high52": 0}

            if (i + 1) % 50 == 0:
                log.info(f"  52주 조회 진행: {i+1}/{len(code_list)}")

        log.info(f"52주 데이터 완료: {len(result)}종목")
        return result

    except Exception as e:
        log.error(f"52주 데이터 조회 실패: {e}")
        return {}


# ────────────────────────────────────────
# 4. 과거 최대 거래대금 캐시 조회
# ────────────────────────────────────────
def fetch_max_turnover_history(code_list: list, use_cache=True):
    """
    최근 3년 내 일 거래대금 100억 돌파 이력 확인
    캐시 파일 사용으로 속도 최적화
    반환: {종목코드: {"max_turnover_억": float, "has_100억": bool}}
    """
    import json

    cache_path = "cache/volume_history.json"
    cache = {}

    # 캐시 로드
    if use_cache and os.path.exists(cache_path):
        try:
            with open(cache_path, "r", encoding="utf-8") as f:
                cache = json.load(f)
            log.info(f"거래대금 캐시 로드: {len(cache)}종목")
        except Exception:
            cache = {}

    # 캐시에 없는 종목만 조회
    missing = [c for c in code_list if c not in cache]

    if missing:
        log.info(f"거래대금 신규 조회: {len(missing)}종목")
        try:
            from pykrx import stock as krx
            end_str   = get_recent_business_day(0)
            start_str = (datetime.now() - timedelta(days=365*3)).strftime("%Y%m%d")

            for i, code in enumerate(missing):
                try:
                    df = krx.get_market_ohlcv_by_date(start_str, end_str, code)
                    if df is not None and not df.empty:
                        turn_col = "거래대금" if "거래대금" in df.columns else "Turnover"
                        if turn_col in df.columns:
                            max_t = float(df[turn_col].max()) / 1e8
                        else:
                            close_col = "종가" if "종가" in df.columns else "Close"
                            vol_col   = "거래량" if "거래량" in df.columns else "Volume"
                            max_t = float(
                                (df[close_col] * df[vol_col]).max()
                            ) / 1e8
                        cache[code] = {
                            "max_turnover_억": round(max_t, 1),
                            "has_100억": max_t >= 100,
                            "updated": datetime.now().strftime("%Y-%m-%d")
                        }
                    else:
                        cache[code] = {
                            "max_turnover_억": 0,
                            "has_100억": False,
                            "updated": datetime.now().strftime("%Y-%m-%d")
                        }
                except Exception:
                    cache[code] = {
                        "max_turnover_억": 0,
                        "has_100억": False,
                        "updated": datetime.now().strftime("%Y-%m-%d")
                    }

                if (i + 1) % 30 == 0:
                    log.info(f"  거래대금 조회 진행: {i+1}/{len(missing)}")

            # 캐시 저장
            os.makedirs("cache", exist_ok=True)
            with open(cache_path, "w", encoding="utf-8") as f:
                json.dump(cache, f, ensure_ascii=False, indent=2)
            log.info(f"거래대금 캐시 저장 완료: {len(cache)}종목")

        except Exception as e:
            log.error(f"거래대금 조회 실패: {e}")

    return {c: cache.get(c, {"max_turnover_억": 0, "has_100억": False})
            for c in code_list}


# ────────────────────────────────────────
# 5. DART 기업코드 맵 (market_engine에서 관리)
# ────────────────────────────────────────
def load_corp_code_map():
    """
    DART 기업코드 맵 로드
    반환: {종목코드: corp_code}
    """
    try:
        url = f"{DART_BASE}/corpCode.xml?crtfc_key={DART_API_KEY}"
        resp = requests.get(url, timeout=30)
        import xml.etree.ElementTree as ET
        with zipfile.ZipFile(io.BytesIO(resp.content)) as z:
            with z.open("CORPCODE.xml") as f:
                tree = ET.parse(f)
        corp_map = {}
        for item in tree.getroot().findall("list"):
            code = item.findtext("stock_code", "").strip()
            corp = item.findtext("corp_code", "").strip()
            if code:
                corp_map[code] = corp
        log.info(f"DART 기업코드 맵: {len(corp_map)}개")
        return corp_map
    except Exception as e:
        log.error(f"기업코드 맵 로드 실패: {e}")
        return {}


# ────────────────────────────────────────
# 6. 메인 유니버스 로드 (main.py에서 호출)
# ────────────────────────────────────────
def load_market_universe():
    """
    전체 유니버스 로드 + 기본 필터 적용
    반환: DataFrame
    """
    # KOSDAQ 배치 조회
    df = fetch_market_batch("KOSDAQ")
    if df.empty:
        # FinanceDataReader 폴백
        log.warning("pykrx 실패 — FinanceDataReader 폴백")
        try:
            import FinanceDataReader as fdr
            df = fdr.StockListing("KOSDAQ")
            df["Code"] = df["Code"].astype(str).str.zfill(6)
            df["Marcap_억"] = df["Marcap"] / 1e8
            df["ChangeRatio"] = df.get("ChangeCode", 0)
        except Exception as e:
            log.error(f"FinanceDataReader 폴백도 실패: {e}")
            return pd.DataFrame()

    total_before = len(df)

    # 시총 필터 (150억-700억)
    df = df[
        (df["Marcap_억"] >= MKTCAP_MIN_억) &
        (df["Marcap_억"] <= MKTCAP_MAX_억)
    ].copy()

    # 종목명 패턴 제거
    for pat in REJECT_NAME_PATTERNS:
        df = df[~df["Name"].str.contains(pat, na=False)]

    # 거래량 0 제거 (거래 정지 종목)
    df = df[df["Volume"] > 0]

    # Code 6자리 정규화
    df["Code"] = df["Code"].astype(str).str.zfill(6)

    df = df.reset_index(drop=True)

    log.info(
        f"유니버스 확정: {total_before}종목 → "
        f"시총·패턴 필터 후 {len(df)}종목"
    )
    return df
