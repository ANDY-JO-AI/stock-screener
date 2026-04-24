"""
alert.py — 알림 엔진 v1
시간여행TV 기준: 종목 선정 결과 로그 알림
"""

import logging
import os
from datetime import datetime

log = logging.getLogger(__name__)

def send_alert(message: str, level: str = "INFO") -> None:
    """알림 발송 (현재: 로그 출력 / 추후 텔레그램 연동 예정)"""
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    formatted = f"[ALERT][{level}] {timestamp} | {message}"
    if level == "ERROR":
        log.error(formatted)
    elif level == "WARNING":
        log.warning(formatted)
    else:
        log.info(formatted)

def send_buy_alert(stock_name: str, code: str, track: str, score: float, reason: str) -> None:
    """매수 신호 알림"""
    msg = (
        f"🚨 매수신호 | {track} | "
        f"종목: {stock_name}({code}) | "
        f"점수: {score:.1f} | "
        f"사유: {reason}"
    )
    send_alert(msg, level="INFO")
    log.info(msg)

def send_pipeline_summary(
    total: int,
    core: int,
    buy_now: int,
    ready: int,
    launched: int,
    top_theme: str,
    elapsed_sec: float
) -> None:
    """파이프라인 완료 요약 알림"""
    elapsed_min = elapsed_sec / 60
    msg = (
        f"✅ 파이프라인 완료 | "
        f"총 {total}종목 분석 | "
        f"CORE={core} BUY_NOW={buy_now} READY={ready} LAUNCHED={launched} | "
        f"TOP테마={top_theme} | "
        f"소요={elapsed_min:.1f}분"
    )
    send_alert(msg, level="INFO")

def send_error_alert(step: str, error_msg: str) -> None:
    """오류 알림"""
    msg = f"❌ 오류발생 | {step} | {error_msg}"
    send_alert(msg, level="ERROR")
