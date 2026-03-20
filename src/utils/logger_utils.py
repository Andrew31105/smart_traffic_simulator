"""
logger_utils.py - Hệ thống log tập trung
Cung cấp logger chuẩn hoá cho toàn bộ dự án.
"""

import logging
import sys
from datetime import datetime
from pathlib import Path


LOG_DIR = Path(__file__).resolve().parent.parent.parent / "logs"
LOG_FORMAT = "%(asctime)s | %(levelname)-8s | %(name)-25s | %(message)s"
LOG_DATE_FORMAT = "%Y-%m-%d %H:%M:%S"


def get_logger(
    name: str,
    level: int = logging.INFO,
    log_to_file: bool = True,
) -> logging.Logger:
    """
    Tạo và trả về logger chuẩn hoá.

    Args:
        name: Tên module / component (vd: 'tomtom_producer', 'kafka_to_minio')
        level: Logging level (default: INFO)
        log_to_file: Ghi log ra file ngoài console (default: True)

    Returns:
        logging.Logger đã được cấu hình.
    """
    logger = logging.getLogger(name)

    # Tránh thêm handler trùng lặp
    if logger.handlers:
        return logger

    logger.setLevel(level)
    formatter = logging.Formatter(LOG_FORMAT, datefmt=LOG_DATE_FORMAT)

    # --- Console Handler ---
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setLevel(level)
    console_handler.setFormatter(formatter)
    logger.addHandler(console_handler)

    # --- File Handler ---
    if log_to_file:
        LOG_DIR.mkdir(parents=True, exist_ok=True)
        today = datetime.now().strftime("%Y-%m-%d")
        log_file = LOG_DIR / f"{name}_{today}.log"

        file_handler = logging.FileHandler(log_file, encoding="utf-8")
        file_handler.setLevel(level)
        file_handler.setFormatter(formatter)
        logger.addHandler(file_handler)

    return logger
