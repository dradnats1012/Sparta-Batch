import logging
import os
from datetime import datetime


def setup_logging():
    log_dir = "logs"
    os.makedirs(log_dir, exist_ok=True)
    today = datetime.now().strftime("%Y-%m-%d")

    success_handler = logging.FileHandler(os.path.join(log_dir, f"batch_{today}_success.log"))
    success_handler.setLevel(logging.INFO)
    success_formatter = logging.Formatter('[%(asctime)s] %(message)s')
    success_handler.setFormatter(success_formatter)

    success_logger = logging.getLogger("success")
    success_logger.setLevel(logging.INFO)
    success_logger.addHandler(success_handler)

    fail_handler = logging.FileHandler(os.path.join(log_dir, f"batch_{today}_fail.log"))
    fail_handler.setLevel(logging.INFO)
    fail_formatter = logging.Formatter('[%(asctime)s] %(message)s')
    fail_handler.setFormatter(fail_formatter)

    fail_logger = logging.getLogger("fail")
    fail_logger.setLevel(logging.INFO)
    fail_logger.addHandler(fail_handler)
