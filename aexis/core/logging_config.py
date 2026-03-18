import logging
import os
from logging.handlers import RotatingFileHandler
from pathlib import Path

def setup_logging(service_type: str, service_id: str = None):

    root_log_dir = Path("/home/godelhaze/dev/megalith/aexis/.aexis/log")

    if service_id:
        if service_id.startswith(f"{service_type}_"):
            log_dir = root_log_dir / service_id
        else:
            log_dir = root_log_dir / f"{service_type}_{service_id}"
    else:
        log_dir = root_log_dir / service_type

    log_dir.mkdir(parents=True, exist_ok=True)

    log_file = log_dir / "active.log"

    logger = logging.getLogger()
    logger.setLevel(logging.INFO)

    for handler in logger.handlers[:]:
        logger.removeHandler(handler)

    formatter = logging.Formatter(
        "%(asctime)s [%(name)s] %(levelname)s %(message)s"
    )

    file_handler = RotatingFileHandler(
        log_file, maxBytes=10*1024*1024, backupCount=5
    )
    file_handler.setFormatter(formatter)
    logger.addHandler(file_handler)

    stream_handler = logging.StreamHandler()
    stream_handler.setFormatter(formatter)
    logger.addHandler(stream_handler)

    logging.info(f"Logging initialized. Service: {service_type}, ID: {service_id}, Path: {log_file}")

    return logger
