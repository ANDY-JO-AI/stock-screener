"""
Andy Jo Stock AI — 필터링 엔진 v3
시간여행TV 소형주 선정 기준 7레이어 완전 구현

[LAYER 0] 유니버스 정의 (market_engine에서 처리)
[LAYER 1] 즉시탈락 — HARD REJECT
[LAYER 2] 재무 점수 — 7점 이상 통과
[LAYER 3] 수급·거래량
[LAYER 4] 주주구조
[LAYER 5] 테마성 평가
[LAYER 6] 매수 타이밍
"""

import logging
import pandas as pd
from datetime import datetime

log = logging.getLogger(__name__)

# ────────────────────────────────────────
# 상수
# ────────────────────────────────────────
FIN_SCORE_MIN      = 4   # 재무 점수 최소 (7점 기준 → 데이터 부족 감안 4점으로 완화)
THEME_SCORE_MIN    = 2   # 테마 점수 최소
TURNOVER_10D_MAX   = 80  # 현재 10일 평균 거래대금 (억) — 이하면 소외 상태
PRICE_52W_RATIO    = 1.7 # 52주 최저가 × 1.7 이내 (코스닥 기준)
TOP_N              = 50  # 트랙별 최대 출력 종목 수

# 테마 키워드 → 종목명 매칭
THEME_KEYWORDS = {
    "방산":     ["방산", "방위", "무기", "미사일", "탄약", "군수"],
    "원전":     ["원전", "원자력", "SMR", "핵융합", "방사선"],
    "로봇_AI":  ["로봇", "자동화", "AI", "인공지능", "드론"],
    "2차전지":  ["배터리", "전지", "양극재", "음극재", "리튬"],
    "바이오":   ["바이오", "제약", "의약", "신약", "헬스"],
    "조선":     ["조선", "선박", "해양", "LNG"],
    "우주항공": ["우주", "항공", "위성", "발사체"],
    "수소":     ["수소", "연료전지", "그린"],
    "반도체":   ["반도체", "웨이퍼", "칩", "파운드리"],
    "건설":     ["건설", "건축", "토목", "플랜트"],
    "게임":     ["게임", "엔터", "콘텐츠", "미디어"],
    "정치":     ["테마", "정치", "대선", "총선"],
    "미세먼지": ["마스크", "공기청정", "필터", "환경"],
    "조류독감": ["조류", "독감", "백신", "수산"],
    "철강":     ["철강", "제철", "금속", "스틸"],
}


# ════════════════════════════════════════
# LAYER 1 — 즉시탈락 필터 (HARD REJECT)
# ════════════════════════════════════════
def check_l1_hard_reject(row, disclosure_risk, financial_data, week52_data):
    """
    시간여행TV 즉시탈락 9개 조건 체크
    반환: (탈락여부, 탈락사유)
    """
    code  = str(row.get("Code", ""))
    name  = str(row.get("Name", ""))
    price = float(row.get("Close", 0) or 0)

    # ① 공시 위험 (횡령·상장폐지·거래정지 등)
    if disclosure_risk.get("hard_reject", False):
        return True, disclosure_risk.get("reject_reason", "공시위험")

    # ② CB/BW/유상증자 (최근 14일 배치에서 확인)
    #    → 2년치는 별도 확인이 필요하나 배치에서 감지된 것만 우선 적용
    if disclosure_risk.get("has_cb_bw", False):
        return True, f"CB/BW감지: {disclosure_risk.get('cb_bw_detail','')}"

    # ③ 자금대여 의심
    if disclosure_risk.get("has_money_leak", False):
        return True, "자금대여 의심 공시"

    # ④ 3년 연속 적자
    fin = financial_data or {}
    if not fin.get("op_profit_ok", True):
        profits = fin.get("op_profit_3y", [])
        valid   = [p for p in profits if p is not None]
        if len(valid) >= 2 and all(p < 0 for p in valid):
            return True, f"연속적자: {[round(p/1e8,1) if p else None for p in profits]}"

    # ⑤ 자본잠식 50% 이상
    erosion = fin.get("capital_erosion", 0) or 0
    if erosion >= 50:
        return True, f"자본잠식 {erosion:.0f}%"

    # ⑥ 부채비율 100% 이상
    debt_ratio = fin.get("debt_ratio")
    if debt_ratio is not None and debt_ratio >= 100:
        return True, f"부채비율 {debt_ratio:.0f}%"

    # ⑦ 주가 위치 — 52주 최저가 × 1.7 초과 (이미 많이 오른 종목)
    w52 = week52_data.get(code, {})
    low52 = w52.get("low52", 0)
    if low52 > 0 and price > 0:
        ratio = price / low52
        if ratio > PRICE_52W_RATIO:
            return True, f"주가위치 과도: 52주최저대비 {ratio:.1f}배"

    # ⑧ 당일 급락 -15% 이하
    change = float(row.get("ChangeRatio", 0) or 0)
    if change <= -15:
        return True, f"당일급락: {change:.1f}%"

    # ⑨ IR 남발 (4건 이상)
    puff = disclosure_risk.get("ir_puff_count", 0)
    if puff >= 4:
        return True, f"IR남발 {puff}건"

    return False, ""


# ════════════════════════════════════════
# LAYER 2 — 재무 점수
# ════════════════════════════════════════
def calc_l2_financial_score(row, financial_data):
    """
    시간여행TV 재무 점수 계산
    반환: (점수, 상세내용, 통과여부)
    """
    fin     = financial_data or {}
    score   = fin.get("fin_score", 3)
    detail  = fin.get("fin_score_detail", "기본점수")

    # 매출/시총 비율 추가 점수 (filter_engine에서 계산)
    marcap  = float(row.get("Marcap_억", 0) or 0) * 1e8
    revenue = fin.get("revenue")
    if revenue and marcap and marcap > 0:
        ratio = revenue / marcap
        if ratio >= 1.0:
            score  += 2
            detail += f" / 매출/시총={ratio:.1f}(+2)"
        elif ratio >= 0.5:
            score  += 1
            detail += f" / 매출/시총={ratio:.1f}(+1)"

    # 순자산 > 시총 추가 점수
    equity = fin.get("equity")
    if equity and marcap and equity > marcap:
        score  += 2
        detail += " / 순자산>시총(+2)"

    passed = score >= FIN_SCORE_MIN
    return score, detail, passed


# ════════════════════════════════════════
# LAYER 3 — 수급·거래량
# ════════════════════════════════════════
def check_l3_volume(row, volume_history):
    """
    반환: (통과여부, 상태설명, 플래그dict)
    """
    code      = str(row.get("Code", ""))
    turnover  = float(row.get("Turnover_억", 0) or 0)
    flags     = {}

    # ① 과거 3년 내 거래대금 100억 돌파 이력
    hist = volume_history.get(code, {})
    has_100억 = hist.get("has_100억", True)  # 캐시 없으면 통과 처리
    max_t     = hist.get("max_turnover_억", 0)

    if not has_100억 and max_t > 0:
        return False, f"거래대금이력없음(최대{max_t:.0f}억)", flags

    flags["has_volume_history"] = has_100억
    flags["max_turnover_억"]    = max_t

    # ② 현재 소외 상태 확인 (매수 적기)
    flags["current_turnover_억"] = round(turnover, 1)
    flags["is_neglected"]        = turnover <= TURNOVER_10D_MAX

    if not flags["is_neglected"]:
        flags["volume_warn"] = f"현재거래대금 {turnover:.0f}억 (소외아님)"

    return True, "수급OK", flags


# ════════════════════════════════════════
# LAYER 4 — 주주구조
# ════════════════════════════════════════
def calc_l4_shareholder(dart_shareholder_info):
    """
    반환: (점수, 상태설명, 플래그dict)
    """
    score = 0
    flags = {}
    detail_parts = []

    if not dart_shareholder_info:
        return 3, "주주정보없음(기본)", {"note": "데이터없음"}

    share_pct   = dart_shareholder_info.get("major_share_pct", None)
    tenure_year = dart_shareholder_info.get("ceo_tenure_year", None)

    # 최대주주 지분율
    if share_pct is not None:
        flags["major_share_pct"] = share_pct
        if share_pct < 30:
            score += 2
            detail_parts.append(f"지분{share_pct:.1f}%<30%(+2, 경영권압박)")
        elif share_pct > 70:
            score += 2
            detail_parts.append(f"지분{share_pct:.1f}%>70%(+2, 유통희박)")
        elif 30 <= share_pct <= 50:
            score += 0
            detail_parts.append(f"지분{share_pct:.1f}%(보통)")
            flags["share_warn"] = "매도 가능성 주의"

    # CEO 재임기간
    if tenure_year is not None:
        flags["ceo_tenure_year"] = tenure_year
        if tenure_year >= 10:
            score += 2
            detail_parts.append(f"재임{tenure_year}년(+2)")
        elif tenure_year >= 5:
            score += 1
            detail_parts.append(f"재임{tenure_year}년(+1)")

    detail = " / ".join(detail_parts) if detail_parts else "주주정보기본"
    return score, detail, flags


# ════════════════════════════════════════
# LAYER 5 — 테마성 평가
# ════════════════════════════════════════
def calc_l5_theme(row, news_data, theme_scores):
    """
    반환: (점수, 매칭테마list, 상태설명)
    """
    name  = str(row.get("Name", ""))
    score = 0
    matched_themes = []
    detail_parts   = []

    # 종목명 기반 테마 매칭
    for theme, keywords in THEME_KEYWORDS.items():
        for kw in keywords:
            if kw in name:
                matched_themes.append(theme)
                # 해당 테마 온도 점수 반영
                t_data  = theme_scores.get(theme, {})
                t_score = t_data.get("score", 0) if isinstance(t_data, dict) else 0
                score  += t_score * 0.6
                detail_parts.append(f"{theme}테마(온도{t_score:.1f})")
                break

    # 뉴스 언급 보너스
    mention = sum(
        1 for a in news_data[:500]
        if name in a.get("title", "") and len(name) >= 3
    )
    if mention >= 5:
        score += 3
        detail_parts.append(f"뉴스{mention}건(+3)")
    elif mention >= 3:
        score += 2
        detail_parts.append(f"뉴스{mention}건(+2)")
    elif mention >= 1:
        score += 1
        detail_parts.append(f"뉴스{mention}건(+1)")

    # 과거 상한가 이력 보너스 (theme_scores에 포함된 경우)
    for theme in matched_themes:
        t_data = theme_scores.get(theme, {})
        if isinstance(t_data, dict) and t_data.get("has_upper_limit"):
            score += 2
            detail_parts.append(f"{theme} 상한가이력(+2)")
            break

    detail = " / ".join(detail_parts) if detail_parts else "테마미해당"
    return round(score, 2), matched_themes, detail


# ════════════════════════════════════════
# LAYER 6 — 매수 타이밍
# ════════════════════════════════════════
def check_l6_timing(row, week52_data):
    """
    반환: (통과여부, 상태설명, 플래그dict)
    """
    code   = str(row.get("Code", ""))
    price  = float(row.get("Close", 0) or 0)
    change = float(row.get("ChangeRatio", 0) or 0)
    flags  = {}

    w52   = week52_data.get(code, {})
    low52 = w52.get("low52", 0)

    # 52주 최저가 대비 위치
    if low52 > 0 and price > 0:
        ratio = price / low52
        flags["price_52w_ratio"] = round(ratio, 2)
        flags["low52"]           = low52

        if ratio <= 1.5:
            flags["timing_grade"] = "최적"
        elif ratio <= PRICE_52W_RATIO:
            flags["timing_grade"] = "양호"
        else:
            # L1에서 이미 걸러졌어야 하지만 안전망
            return False, f"52주최저대비{ratio:.1f}배(과도)", flags
    else:
        flags["timing_grade"] = "확인불가"

    # 소외 상태 재확인
    turnover = float(row.get("Turnover_억", 0) or 0)
    flags["is_neglected"] = turnover <= TURNOVER_10D_MAX

    # 당일 급등 여부 (LAUNCHED 후보)
    flags["change"] = change

    detail = f"52주위치{flags.get('timing_grade','?')} / {'소외' if flags['is_neglected'] else '관심집중'}"
    return True, detail, flags


# ════════════════════════════════════════
# 결과 카드 생성 — 이용자에게 보여줄 텍스트
# ════════════════════════════════════════
def build_result_card(
    row, track,
    l1_reason, fin_score, fin_detail,
    l3_flags, l4_score, l4_detail, l4_flags,
    theme_score, themes, theme_detail,
    l6_flags, disclosure_risk, financial_data
):
    """
    이용자가 한눈에 파악할 수 있는 결과 카드 생성
    """
    name   = str(row.get("Name", ""))
    code   = str(row.get("Code", ""))
    price  = float(row.get("Close", 0) or 0)
    change = float(row.get("ChangeRatio", 0) or 0)
    marcap = float(row.get("Marcap_억", 0) or 0)
    volume = float(row.get("Volume", 0) or 0)

    # 즉시탈락 체크리스트
    checks = []
    fin = financial_data or {}

    op_ok   = fin.get("op_profit_ok", True)
    debt_ok = fin.get("debt_ratio_ok", True)
    eros_ok = fin.get("erosion_ok", True)
    cb_ok   = not disclosure_risk.get("has_cb_bw", False)
    dr_ok   = not disclosure_risk.get("hard_reject", False)
    w52_ok  = l6_flags.get("price_52w_ratio", 1.0) <= PRICE_52W_RATIO
    vol_ok  = l3_flags.get("has_volume_history", True)

    checks.append(f"{'✅' if op_ok else '❌'} 연속적자없음")
    checks.append(
        f"{'✅' if debt_ok else '❌'} "
        f"부채비율 {fin.get('debt_ratio','N/A')}%"
    )
    checks.append(
        f"{'✅' if eros_ok else '❌'} "
        f"자본잠식률 {fin.get('capital_erosion', 0):.0f}%"
    )
    checks.append(f"{'✅' if cb_ok else '⚠️'} CB/BW 없음")
    checks.append(f"{'✅' if dr_ok else '❌'} 공시위험없음")
    checks.append(
        f"{'✅' if w52_ok else '⚠️'} "
        f"52주최저대비 {l6_flags.get('price_52w_ratio', '?')}배"
    )
    checks.append(
        f"{'✅' if vol_ok else '⚠️'} "
        f"거래대금이력(최대{l3_flags.get('max_turnover_억',0):.0f}억)"
    )

    # 주의사항
    warns = disclosure_risk.get("warn_flags", [])
    if not l3_flags.get("is_neglected", True):
        warns.append(
            f"현재거래대금 {l3_flags.get('current_turnover_억',0):.0f}억"
            f" (소외아님 — 매수주의)"
        )
    if l4_flags.get("share_warn"):
        warns.append(l4_flags["share_warn"])

    # 직접확인 필요 항목
    manual_checks = [
        "□ 자회사 금전대여 여부 (DART 공시 직접 확인)",
        "□ 장대양봉→장대음봉 패턴 없음 확인",
    ]
    if not l4_flags.get("major_share_pct"):
        manual_checks.append("□ 최대주주 지분율 확인")

    naver_url = f"https://finance.naver.com/item/main.naver?code={code}"
    dart_url  = f"https://dart.fss.or.kr/dsab007/main.do?autoSearch=true&textCrpNm={name}"

    return {
        "code":          code,
        "name":          name,
        "track":         track,
        "price":         price,
        "change":        change,
        "marcap_억":     round(marcap, 1),
        "volume":        int(volume),
        "themes":        themes,
        "fin_score":     fin_score,
        "fin_detail":    fin_detail,
        "theme_score":   theme_score,
        "theme_detail":  theme_detail,
        "l4_score":      l4_score,
        "l4_detail":     l4_detail,
        "total_score":   round(
            fin_score * 0.4 + theme_score * 0.4 + l4_score * 0.2, 2
        ),
        "checks":        checks,
        "warns":         warns,
        "manual_checks": manual_checks,
        "naver_url":     naver_url,
        "dart_url":      dart_url,
        "timing_grade":  l6_flags.get("timing_grade", "확인불가"),
        "is_neglected":  l6_flags.get("is_neglected", False),
        "price_52w_ratio": l6_flags.get("price_52w_ratio", None),
        "reject_reason": l1_reason,
        "debt_ratio":    fin.get("debt_ratio"),
        "reserve_ratio": fin.get("reserve_ratio"),
        "roe":           fin.get("roe"),
        "op_profit_3y":  fin.get("op_profit_3y", []),
    }


# ════════════════════════════════════════
# L0-L1 필터 (main.py STEP 4에서 호출)
# ════════════════════════════════════════
def apply_l0_l1_filter(universe_df, disclosure_map):
    """
    공시 기반 즉시탈락만 우선 적용
    재무·52주 데이터는 아직 없으므로 공시 위험만 제거
    반환: 통과 DataFrame
    """
    from dart_engine import analyze_disclosure_risk
    from market_engine import load_corp_code_map

    corp_map = load_corp_code_map()
    pass_rows = []

    for _, row in universe_df.iterrows():
        code      = str(row.get("Code", ""))
        corp_code = corp_map.get(code, "")

        disc_risk = analyze_disclosure_risk(corp_code, disclosure_map) \
            if corp_code else {}

        if disc_risk.get("hard_reject", False):
            continue  # 탈락

        pass_rows.append(row)

    result = pd.DataFrame(pass_rows).reset_index(drop=True)
    log.info(f"L0-L1 필터 완료: {len(universe_df)} → {len(result)}종목")
    return result


# ════════════════════════════════════════
# L2-L6 전체 필터 (main.py STEP 6에서 호출)
# ════════════════════════════════════════
def apply_l2_l6_filter(
    candidates_df, financial_map,
    disclosure_map, news_data,
    theme_scores=None, shareholder_map=None,
    volume_history=None, week52_data=None
):
    """
    전체 필터 적용 및 트랙 분류
    반환: {"READY": [], "BUY_NOW": [], "CORE": [], "LAUNCHED": [], "REJECTED": []}
    """
    from dart_engine import analyze_disclosure_risk
    from market_engine import load_corp_code_map

    if theme_scores is None:
        theme_scores = {}
    if shareholder_map is None:
        shareholder_map = {}
    if volume_history is None:
        volume_history = {}
    if week52_data is None:
        week52_data = {}

    corp_map = load_corp_code_map()

    ready, buy_now, core, launched, rejected = [], [], [], [], []
    total = len(candidates_df)

    log.info(f"L2-L6 필터링 시작: {total}종목")

    for i, (_, row) in enumerate(candidates_df.iterrows()):
        code      = str(row.get("Code", ""))
        corp_code = corp_map.get(code, "")
        fin_data  = financial_map.get(code, {})
        disc_risk = analyze_disclosure_risk(corp_code, disclosure_map) \
            if corp_code else {}
        shareholder = shareholder_map.get(code, {})

        # ── L1 재확인 (재무 데이터 포함) ──
        l1_reject, l1_reason = check_l1_hard_reject(
            row, disc_risk, fin_data, week52_data
        )
        if l1_reject:
            rejected.append({
                "code": code,
                "name": str(row.get("Name", "")),
                "reject_reason": l1_reason,
                "track": "REJECTED"
            })
            continue

        # ── L2 재무 점수 ──
        fin_score, fin_detail, fin_pass = calc_l2_financial_score(row, fin_data)
        if not fin_pass:
            rejected.append({
                "code": code,
                "name": str(row.get("Name", "")),
                "reject_reason": f"재무점수미달({fin_score}점)",
                "track": "REJECTED"
            })
            continue

        # ── L3 수급 ──
        l3_pass, l3_detail, l3_flags = check_l3_volume(row, volume_history)
        if not l3_pass:
            rejected.append({
                "code": code,
                "name": str(row.get("Name", "")),
                "reject_reason": l3_detail,
                "track": "REJECTED"
            })
            continue

        # ── L4 주주구조 ──
        l4_score, l4_detail, l4_flags = calc_l4_shareholder(shareholder)

        # ── L5 테마성 ──
        theme_score, themes, theme_detail = calc_l5_theme(
            row, news_data, theme_scores
        )

        # ── L6 타이밍 ──
        l6_pass, l6_detail, l6_flags = check_l6_timing(row, week52_data)
        if not l6_pass:
            rejected.append({
                "code": code,
                "name": str(row.get("Name", "")),
                "reject_reason": l6_detail,
                "track": "REJECTED"
            })
            continue

        # ── 결과 카드 생성 ──
        card = build_result_card(
            row, None,
            "", fin_score, fin_detail,
            l3_flags, l4_score, l4_detail, l4_flags,
            theme_score, themes, theme_detail,
            l6_flags, disc_risk, fin_data
        )

        # ── 트랙 분류 ──
        change = float(row.get("ChangeRatio", 0) or 0)

        if change >= 15:
            card["track"] = "LAUNCHED"
            launched.append(card)
        elif (
            card["total_score"] >= 6
            and theme_score >= THEME_SCORE_MIN
            and themes
            and l3_flags.get("is_neglected", False)
        ):
            card["track"] = "CORE"
            core.append(card)
        elif (
            card["total_score"] >= 4
            and themes
        ):
            card["track"] = "BUY_NOW"
            buy_now.append(card)
        else:
            card["track"] = "READY"
            ready.append(card)

        if (i + 1) % 30 == 0:
            log.info(
                f"  진행: {i+1}/{total} | "
                f"CORE {len(core)} / BUY_NOW {len(buy_now)} / "
                f"READY {len(ready)} / 탈락 {len(rejected)}"
            )

    # 점수 정렬
    for lst in [core, buy_now, ready, launched]:
        lst.sort(key=lambda x: -x["total_score"])

    result = {
        "CORE":     core[:TOP_N],
        "BUY_NOW":  buy_now[:TOP_N],
        "READY":    ready[:TOP_N],
        "LAUNCHED": launched[:TOP_N],
        "REJECTED": rejected[:100],
    }

    log.info(
        f"필터링 완료 | "
        f"CORE {len(result['CORE'])} / "
        f"BUY_NOW {len(result['BUY_NOW'])} / "
        f"READY {len(result['READY'])} / "
        f"LAUNCHED {len(result['LAUNCHED'])} / "
        f"탈락 {len(result['REJECTED'])}"
    )
    return result
