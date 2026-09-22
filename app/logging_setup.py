"""判定日志：统一打印幂等键、租户标识与判定依据，便于审计与排障。"""

from __future__ import annotations

import logging
import sys

LOGGER_NAME = "governance"


def configure_logging(level: int = logging.INFO) -> logging.Logger:
    logger = logging.getLogger(LOGGER_NAME)
    logger.setLevel(level)
    logger.propagate = False
    if not any(
        isinstance(h, logging.StreamHandler) and getattr(h, "_governance", False)
        for h in logger.handlers
    ):
        handler = logging.StreamHandler(sys.stderr)
        handler._governance = True  # type: ignore[attr-defined]
        handler.setFormatter(
            logging.Formatter(
                "%(asctime)s %(levelname)s [%(name)s] %(message)s",
                datefmt="%Y-%m-%dT%H:%M:%S",
            )
        )
        logger.addHandler(handler)
    return logger


def get_logger() -> logging.Logger:
    return logging.getLogger(LOGGER_NAME)


def log_decision(
    event: str,
    *,
    tenant_id: str = "-",
    idempotency_key: str = "-",
    api: str = "-",
    decision: str = "-",
    reason: str = "-",
    **extra: object,
) -> None:
    """打印一条结构化判定日志（单测也通过同一入口输出判定依据）。"""
    fields = [
        f"tenant={tenant_id}",
        f"idem_key={idempotency_key}",
        f"api={api}",
        f"decision={decision}",
        f"reason={reason}",
    ]
    for k, v in extra.items():
        fields.append(f"{k}={v}")
    get_logger().info("event=%s %s", event, " ".join(fields))
