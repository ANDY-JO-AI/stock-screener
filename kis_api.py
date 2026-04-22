# kis_api.py
import os, requests, json, time
import pandas as pd
from data_store import save_price_data

KIS_APP_KEY    = os.environ.get("KIS_APP_KEY", "")
KIS_APP_SECRET = os.environ.get("KIS_APP_SECRET", "")
KIS_ACCOUNT_NO = os.environ.get("KIS_ACCOUNT_NO", "")  # 예: 12345678-01
BASE_URL       = "https://openapi.koreainvestment.com:9443"

_token_cache = {"access_token": "", "expired_at": 0}


def get_access_token() -> str:
    """OAuth 토큰 발급 (캐시 30분)"""
    now = time.time()
    if _token_cache["access_token"] and now < _token_cache["expired_at"]:
        return _token_cache["access_token"]
    url = f"{BASE_URL}/oauth2/tokenP"
    body = {
        "grant_type": "client_credentials",
        "appkey":     KIS_APP_KEY,
        "appsecret":  KIS_APP_SECRET,
    }
    try:
        r = requests.post(url, json=body, timeout=10)
        data = r.json()
        token = data.get("access_token", "")
        _token_cache["access_token"] = token
        _token_cache["expired_at"]   = now + 1700  # 약 28분
        print("[KIS] 토큰 발급 완료")
        return token
    except Exception as e:
        print(f"[KIS] 토큰 발급 실패: {e}")
        return ""


def _headers(tr_id: str) -> dict:
    token = get_access_token()
    return {
        "Content-Type":  "application/json",
        "authorization": f"Bearer {token}",
        "appkey":        KIS_APP_KEY,
        "appsecret":     KIS_APP_SECRET,
        "tr_id":         tr_id,
        "custtype":      "P",
    }


def get_current_price(code: str) -> dict:
    """주식 현재가 조회 (실전)"""
    url = f"{BASE_URL}/uapi/domestic-stock/v1/quotations/inquire-price"
    params = {"FID_COND_MRKT_DIV_CODE": "J", "FID_INPUT_ISCD": code}
    try:
        r = requests.get(url, headers=_headers("FHKST01010100"),
                         params=params, timeout=8)
        out = r.json().get("output", {})
        return {
            "code":         code,
            "current":      int(out.get("stck_prpr", 0)),
            "change_rate":  float(out.get("prdy_ctrt", 0)),
            "change":       int(out.get("prdy_vrss", 0)),
            "volume":       int(out.get("acml_vol", 0)),
            "high":         int(out.get("stck_hgpr", 0)),
            "low":          int(out.get("stck_lwpr", 0)),
            "open":         int(out.get("stck_oprc", 0)),
        }
    except Exception as e:
        print(f"[KIS] 현재가 오류 {code}: {e}")
        return {"code": code, "current": 0, "change_rate": 0}


def get_investor_trend(code: str) -> dict:
    """기관·외인 순매수 조회"""
    url = f"{BASE_URL}/uapi/domestic-stock/v1/quotations/inquire-investor"
    params = {"FID_COND_MRKT_DIV_CODE": "J", "FID_INPUT_ISCD": code}
    try:
        r = requests.get(url, headers=_headers("FHKST01010900"),
                         params=params, timeout=8)
        out = r.json().get("output", {})
        return {
            "code":            code,
            "inst_net_buy":    int(out.get("frgn_ntby_qty",  0)),  # 외국인 순매수
            "foreign_net_buy": int(out.get("orgn_ntby_qty",  0)),  # 기관 순매수
            "inst_net_amt":    int(out.get("orgn_ntby_tr_pbmn", 0)),
        }
    except Exception as e:
        print(f"[KIS] 투자자 동향 오류 {code}: {e}")
        return {"code": code, "inst_net_buy": 0, "foreign_net_buy": 0}


def get_upper_limit_stocks() -> list:
    """상한가 종목 조회"""
    url = f"{BASE_URL}/uapi/domestic-stock/v1/ranking/fluctuation"
    params = {
        "fid_aply_rang_prc_2": "",
        "fid_cond_mrkt_div_code": "J",
        "fid_cond_scr_div_code": "20170",
        "fid_input_iscd": "0001",
        "fid_rank_sort_cls_code": "0",
        "fid_rsfl_rate1": "29",
        "fid_rsfl_rate2": "30",
        "fid_trgt_cls_code": "0",
        "fid_trgt_exls_cls_code": "0",
        "fid_vol_cnt": "0",
        "fid_aply_rang_prc_1": "",
    }
    try:
        r = requests.get(url, headers=_headers("FHPST01700000"),
                         params=params, timeout=10)
        items = r.json().get("output", [])
        result = []
        for item in items[:30]:
            result.append({
                "name":        item.get("hts_kor_isnm", ""),
                "code":        item.get("stck_shrn_iscd", ""),
                "change_rate": float(item.get("prdy_ctrt", 0)),
                "price":       int(item.get("stck_prpr", 0)),
                "volume":      int(item.get("acml_vol", 0)),
            })
        return result
    except Exception as e:
        print(f"[KIS] 상한가 조회 오류: {e}")
        return []


def get_volume_surge_stocks() -> list:
    """거래량 급증 종목 조회"""
    url = f"{BASE_URL}/uapi/domestic-stock/v1/ranking/volume"
    params = {
        "fid_cond_mrkt_div_code": "J",
        "fid_cond_scr_div_code": "20171",
        "fid_input_iscd": "0001",
        "fid_rank_sort_cls_code": "0",
        "fid_trgt_cls_code": "0",
        "fid_trgt_exls_cls_code": "0",
        "fid_vol_cnt": "100000",
        "fid_aply_rang_prc_1": "",
        "fid_aply_rang_prc_2": "",
    }
    try:
        r = requests.get(url, headers=_headers("FHPST01710000"),
                         params=params, timeout=10)
        items = r.json().get("output", [])
        result = []
        for item in items[:30]:
            result.append({
                "name":        item.get("hts_kor_isnm", ""),
                "code":        item.get("stck_shrn_iscd", ""),
                "change_rate": float(item.get("prdy_ctrt", 0)),
                "volume_ratio":float(item.get("vol_inrt", 0)),
                "price":       int(item.get("stck_prpr", 0)),
            })
        return result
    except Exception as e:
        print(f"[KIS] 거래량급증 조회 오류: {e}")
        return []


def fetch_and_save_prices(code_list: list):
    """후보종목 코드 리스트 현재가 일괄 조회 후 저장"""
    results = []
    for code in code_list:
        price = get_current_price(code)
        inv   = get_investor_trend(code)
        merged = {**price, **{k: v for k, v in inv.items() if k != "code"}}
        results.append(merged)
        time.sleep(0.05)  # API 호출 간격
    save_price_data(results)
    print(f"[KIS] 현재가 {len(results)}건 저장")
    return results


if __name__ == "__main__":
    # 테스트 실행
    print(get_current_price("005930"))  # 삼성전자
    print(get_upper_limit_stocks()[:3])
