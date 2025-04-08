import re
from datetime import datetime
from pathlib import Path
from src.logger import app_logger as logger
from src.excel_data.service import ExcelDataService
import json
import os


def get_wb_tokens() -> dict:
    tokens_path = Path(__file__).parent / "tokens.json"

    with tokens_path.open("r", encoding="utf-8") as file:
        return json.load(file)


def process_local_vendor_code(s):
    # Шаблон для извлечения "wild" и цифр
    wild_pattern = r'^wild(\d+).*$'
    word_pattern = r'^[a-zA-Z\s]+$'
    wild_match = re.match(wild_pattern, s)
    if wild_match:
        return f"wild{wild_match.group(1)}"
    word_match = re.match(word_pattern, s)
    if word_match:
        return s
    return s


def format_date(iso_date: str) -> str:
    dt = datetime.strptime(iso_date, "%Y-%m-%dT%H:%M:%SZ")
    return dt.strftime("%d.%m.%Y")


def get_information_to_data():
    wild_data = ExcelDataService()._read_data()
    return {item["wild"]: item['модель'] for item in wild_data}
