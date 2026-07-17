from __future__ import annotations

"""
16_download_margin_trading_v5.py

功能：
1. 下載 TWSE 上市融資融券。
2. 下載 TPEx 上櫃融資融券。
3. 只使用 calendar 內的台股交易日。
4. 將 source_trade_date 對齊下一個 target_trade_date。
5. 只保留四位數普通股票。
6. 輸出一般 CSV，不使用 .gz。
7. 有效舊檔直接跳過。
8. 無效舊檔自動刪除並重抓。
9. 支援 TWSE 歷史 JSON 不同回傳格式。
10. 遇到 TWSE HTTP 307 安全阻擋時，自動等待後重試。
11. 掃描整張 TWSE 資料表，不只掃描前 200 列，避免 ETF 排在前面造成誤判。

重要：
2018-01-19 的融資融券資料，只能讓模型在下一交易日
2018-01-22 早上使用。
"""

import argparse
import os
import random
import re
import time
from datetime import date, datetime
from pathlib import Path
from typing import Any

import pandas as pd
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry


# ======================================================================================
# 版本
# ======================================================================================
SCRIPT_VERSION = "2026-07-17-fixed-v4"


# ======================================================================================
# 專案設定
# ======================================================================================
PROJECT_ROOT = Path(
    r"D:\project\project_master_dataset_v5"
)

CALENDAR_FILE = Path(
    r"D:\project\project_master_dataset_v4\data\calendars\tw_us_entry_date_map.csv"
)

CALENDAR_TW_COL = "tw_trade_date"

DATE_START = "2018-01-01"
DATE_END: str | None = None

ENCODING = "utf-8-sig"


# ======================================================================================
# 官方資料網址
# ======================================================================================
TWSE_URLS = (
    "https://www.twse.com.tw/exchangeReport/MI_MARGN",
    "https://www.twse.com.tw/rwd/zh/marginTrading/MI_MARGN",
)

TPEX_URL = (
    "https://www.tpex.org.tw/"
    "www/zh-tw/margin/balance"
)


# ======================================================================================
# HTTP 與限速
# ======================================================================================
HTTP_TIMEOUT = 30
HTTP_MAX_ATTEMPTS = 6

MARKET_SLEEP_MIN = 0.60
MARKET_SLEEP_MAX = 1.00

DAY_SLEEP_MIN = 1.50
DAY_SLEEP_MAX = 2.50


# ======================================================================================
# 每日檔案合理筆數
# ======================================================================================
MIN_TWSE_ROWS = 300
MAX_TWSE_ROWS = 2500

MIN_TPEX_ROWS = 200
MAX_TPEX_ROWS = 2500

MAX_TOTAL_ROWS = 4000

MIN_BALANCE_IDENTITY_RATE = 0.80


# ======================================================================================
# 輸出欄位
# ======================================================================================
OUTPUT_COLUMNS = [
    "source_trade_date",
    "target_trade_date",
    "stock_code",
    "stock_name",
    "market",
    "margin_buy",
    "margin_sell",
    "margin_cash_repay",
    "margin_balance_prev",
    "margin_balance",
    "margin_quota",
    "short_buy",
    "short_sell",
    "short_stock_repay",
    "short_balance_prev",
    "short_balance",
    "short_quota",
    "offset",
    "note",
    "source",
]

NUMERIC_COLUMNS = [
    "margin_buy",
    "margin_sell",
    "margin_cash_repay",
    "margin_balance_prev",
    "margin_balance",
    "margin_quota",
    "short_buy",
    "short_sell",
    "short_stock_repay",
    "short_balance_prev",
    "short_balance",
    "short_quota",
    "offset",
]


# ======================================================================================
# 顯示工具
# ======================================================================================
def banner(text: str) -> None:
    print("=" * 118)
    print(text)
    print("=" * 118)


# ======================================================================================
# 字串與數字處理
# ======================================================================================
def norm_text(value: Any) -> str:
    text = "" if value is None else str(value)

    text = (
        text
        .replace("\ufeff", "")
        .replace("\u3000", "")
        .replace("（", "(")
        .replace("）", ")")
    )

    text = re.sub(
        r"\s+",
        "",
        text,
    )

    return text.strip()


def normalize_stock_code(value: Any) -> str:
    text = norm_text(value)

    text = (
        text
        .replace('="', "")
        .replace('"', "")
    )

    if (
        text.endswith(".0")
        and text[:-2].isdigit()
    ):
        text = text[:-2]

    return text


def is_common_stock(value: Any) -> bool:
    """
    只保留四位數，而且第一碼不是 0 的股票。
    """
    code = normalize_stock_code(
        value
    )

    return bool(
        re.fullmatch(
            r"[1-9]\d{3}",
            code,
        )
    )


def to_int(value: Any) -> int:
    if value is None:
        return 0

    try:
        if pd.isna(value):
            return 0
    except Exception:
        pass

    if isinstance(value, bool):
        return int(value)

    if isinstance(value, int):
        return value

    if isinstance(value, float):
        return int(round(value))

    text = str(value).strip()

    if text in {
        "",
        "--",
        "---",
        "N/A",
        "nan",
        "None",
        "null",
    }:
        return 0

    text = (
        text
        .replace(",", "")
        .replace("+", "")
        .replace("−", "-")
        .replace("－", "-")
    )

    if re.fullmatch(
        r"\([\d.]+\)",
        text,
    ):
        text = "-" + text.strip("()")

    text = re.sub(
        r"[^0-9.\-]",
        "",
        text,
    )

    if text in {
        "",
        "-",
        ".",
        "-.",
    }:
        return 0

    try:
        return int(
            round(
                float(text)
            )
        )
    except ValueError:
        return 0


# ======================================================================================
# 日期處理
# ======================================================================================
def canonical_date(value: Any) -> str:
    """
    統一轉為 YYYY-MM-DD。

    支援：
    20180102
    2018/01/02
    2018-01-02
    107/01/02
    1070102
    107年01月02日
    """
    text = norm_text(value)

    if not text:
        return ""

    digits = re.sub(
        r"\D",
        "",
        text,
    )

    try:
        if len(digits) == 8:
            year = int(digits[:4])
            month = int(digits[4:6])
            day = int(digits[6:8])

        elif len(digits) == 7:
            year = int(digits[:3]) + 1911
            month = int(digits[3:5])
            day = int(digits[5:7])

        elif len(digits) == 6:
            year = int(digits[:2]) + 1911
            month = int(digits[2:4])
            day = int(digits[4:6])

        else:
            return ""

        return date(
            year,
            month,
            day,
        ).isoformat()

    except ValueError:
        return ""


# ======================================================================================
# DataFrame 與 CSV
# ======================================================================================
def empty_df() -> pd.DataFrame:
    return pd.DataFrame(
        columns=OUTPUT_COLUMNS,
    )


def write_csv(
    df: pd.DataFrame,
    path: Path,
) -> None:
    """
    使用同目錄的唯一暫存檔安全寫入。

    Windows 偶爾會因 Excel、檔案預覽或防毒掃描短暫鎖住目標檔。
    遇到 PermissionError 時會等待後重試，而不是立刻中止整個下載流程。
    """
    path.parent.mkdir(
        parents=True,
        exist_ok=True,
    )

    temp_path = path.with_name(
        f"{path.name}.{os.getpid()}.{time.time_ns()}.tmp"
    )

    try:
        df.to_csv(
            temp_path,
            index=False,
            encoding=ENCODING,
        )

        max_attempts = 12

        for attempt in range(
            1,
            max_attempts + 1,
        ):
            try:
                os.replace(
                    temp_path,
                    path,
                )
                return

            except PermissionError as exc:
                if attempt >= max_attempts:
                    raise PermissionError(
                        f"檔案持續被占用：{path}"
                    ) from exc

                wait_seconds = min(
                    3.0,
                    0.25 * attempt,
                )

                print(
                    "  [FILE LOCK] "
                    f"{path.name} 被占用，"
                    f"{wait_seconds:.2f} 秒後重試 "
                    f"({attempt}/{max_attempts})"
                )

                time.sleep(
                    wait_seconds
                )

    finally:
        if temp_path.exists():
            try:
                temp_path.unlink()
            except OSError:
                pass


def finalize(
    df: pd.DataFrame,
) -> pd.DataFrame:
    if df.empty:
        return empty_df()

    df = df.copy()

    for column in OUTPUT_COLUMNS:
        if column not in df.columns:
            if column in NUMERIC_COLUMNS:
                df[column] = 0
            else:
                df[column] = ""

    df["stock_code"] = (
        df["stock_code"]
        .map(normalize_stock_code)
    )

    df = df[
        df["stock_code"]
        .map(is_common_stock)
    ].copy()

    for column in NUMERIC_COLUMNS:
        df[column] = (
            pd.to_numeric(
                df[column],
                errors="coerce",
            )
            .fillna(0)
            .astype("int64")
        )

    for column in [
        "source_trade_date",
        "target_trade_date",
        "stock_code",
        "stock_name",
        "market",
        "note",
        "source",
    ]:
        df[column] = (
            df[column]
            .fillna("")
            .astype(str)
        )

    df = df.drop_duplicates(
        subset=[
            "source_trade_date",
            "market",
            "stock_code",
        ],
        keep="last",
    )

    df = df[
        OUTPUT_COLUMNS
    ]

    df = df.sort_values(
        [
            "market",
            "stock_code",
        ],
        kind="stable",
    )

    return df.reset_index(
        drop=True,
    )


# ======================================================================================
# 融資融券加減關係檢查
# ======================================================================================
def balance_identity_rates(
    df: pd.DataFrame,
) -> tuple[float, float]:
    if df.empty:
        return 0.0, 0.0

    margin_expected = (
        df["margin_balance_prev"]
        + df["margin_buy"]
        - df["margin_sell"]
        - df["margin_cash_repay"]
    )

    short_expected = (
        df["short_balance_prev"]
        + df["short_sell"]
        - df["short_buy"]
        - df["short_stock_repay"]
    )

    margin_rate = float(
        (
            margin_expected
            == df["margin_balance"]
        ).mean()
    )

    short_rate = float(
        (
            short_expected
            == df["short_balance"]
        ).mean()
    )

    return (
        margin_rate,
        short_rate,
    )


# ======================================================================================
# 每日檔驗證
# ======================================================================================
def validate_daily(
    df: pd.DataFrame,
    expected_source_date: str | None = None,
    expected_target_date: str | None = None,
) -> tuple[bool, str]:
    missing_columns = [
        column
        for column in OUTPUT_COLUMNS
        if column not in df.columns
    ]

    if missing_columns:
        return (
            False,
            f"MISSING_COLUMNS:{missing_columns[:5]}",
        )

    if df.empty:
        return (
            False,
            "EMPTY",
        )

    codes = (
        df["stock_code"]
        .astype(str)
        .map(normalize_stock_code)
    )

    if not codes.map(
        is_common_stock
    ).all():
        return (
            False,
            "INVALID_STOCK_CODE",
        )

    duplicate_count = int(
        df.duplicated(
            subset=[
                "source_trade_date",
                "market",
                "stock_code",
            ],
            keep=False,
        ).sum()
    )

    if duplicate_count > 0:
        return (
            False,
            f"DUPLICATES:{duplicate_count}",
        )

    markets = (
        df["market"]
        .astype(str)
        .str.upper()
    )

    twse_rows = int(
        (markets == "TWSE").sum()
    )

    tpex_rows = int(
        (markets == "TPEX").sum()
    )

    total_rows = len(df)

    if not (
        MIN_TWSE_ROWS
        <= twse_rows
        <= MAX_TWSE_ROWS
    ):
        return (
            False,
            f"BAD_TWSE_ROWS:{twse_rows}",
        )

    if not (
        MIN_TPEX_ROWS
        <= tpex_rows
        <= MAX_TPEX_ROWS
    ):
        return (
            False,
            f"BAD_TPEX_ROWS:{tpex_rows}",
        )

    if total_rows > MAX_TOTAL_ROWS:
        return (
            False,
            f"BAD_TOTAL_ROWS:{total_rows}",
        )

    if expected_source_date is not None:
        source_dates = set(
            df["source_trade_date"]
            .astype(str)
        )

        if source_dates != {
            expected_source_date
        }:
            return (
                False,
                (
                    "BAD_SOURCE_DATE:"
                    f"{source_dates}"
                ),
            )

    if expected_target_date is not None:
        target_dates = set(
            df["target_trade_date"]
            .astype(str)
        )

        if target_dates != {
            expected_target_date
        }:
            return (
                False,
                (
                    "BAD_TARGET_DATE:"
                    f"{target_dates}"
                ),
            )

    numeric_df = df.copy()

    for column in NUMERIC_COLUMNS:
        numeric_df[column] = (
            pd.to_numeric(
                numeric_df[column],
                errors="coerce",
            )
            .fillna(0)
            .astype("int64")
        )

    margin_rate, short_rate = (
        balance_identity_rates(
            numeric_df
        )
    )

    if (
        margin_rate
        < MIN_BALANCE_IDENTITY_RATE
    ):
        return (
            False,
            (
                "BAD_MARGIN_IDENTITY_RATE:"
                f"{margin_rate:.4f}"
            ),
        )

    if (
        short_rate
        < MIN_BALANCE_IDENTITY_RATE
    ):
        return (
            False,
            (
                "BAD_SHORT_IDENTITY_RATE:"
                f"{short_rate:.4f}"
            ),
        )

    return (
        True,
        "OK",
    )


# ======================================================================================
# HTTP Session
# ======================================================================================
def make_session() -> requests.Session:
    retry = Retry(
        total=2,
        connect=2,
        read=2,
        status=2,
        backoff_factor=1.0,
        status_forcelist=(
            429,
            500,
            502,
            503,
            504,
        ),
        allowed_methods=frozenset(
            ["GET"]
        ),
        raise_on_status=False,
    )

    adapter = HTTPAdapter(
        max_retries=retry,
    )

    session = requests.Session()

    session.mount(
        "https://",
        adapter,
    )

    session.mount(
        "http://",
        adapter,
    )

    session.headers.update(
        {
            "User-Agent": (
                "Mozilla/5.0 "
                "(Windows NT 10.0; Win64; x64) "
                "AppleWebKit/537.36 "
                "(KHTML, like Gecko) "
                "Chrome/150.0.0.0 "
                "Safari/537.36"
            ),
            "Accept": (
                "application/json,"
                "text/plain,"
                "*/*"
            ),
            "Accept-Language": (
                "zh-TW,"
                "zh;q=0.9,"
                "en;q=0.8"
            ),
            "Connection": "close",
        }
    )

    return session


# ======================================================================================
# HTTP JSON 下載
# ======================================================================================
def get_json(
    session: requests.Session,
    url: str,
    params: dict[str, Any],
    referer: str,
) -> dict[str, Any]:
    last_error: Exception | None = None

    for attempt in range(
        1,
        HTTP_MAX_ATTEMPTS + 1,
    ):
        try:
            response = session.get(
                url,
                params=params,
                headers={
                    "Referer": referer,
                    "Connection": "close",
                    "Cache-Control": "no-cache",
                },
                timeout=HTTP_TIMEOUT,
                allow_redirects=True,
            )

            response_text = (
                response.text
                if response.text is not None
                else ""
            )

            security_blocked = (
                response.status_code
                in {
                    301,
                    302,
                    303,
                    307,
                    308,
                }
                or "因為安全性考量"
                in response_text
                or "FOR SECURITY REASONS"
                in response_text
            )

            if security_blocked:
                wait_seconds = min(
                    90,
                    15 * attempt,
                )

                print(
                    "  [SECURITY BLOCK] "
                    f"HTTP={response.status_code}，"
                    f"{wait_seconds} 秒後重試，"
                    f"attempt={attempt}/"
                    f"{HTTP_MAX_ATTEMPTS}"
                )

                session.cookies.clear()

                time.sleep(
                    wait_seconds
                )

                last_error = RuntimeError(
                    "SECURITY_BLOCK"
                )

                continue

            if response.status_code in {
                429,
                500,
                502,
                503,
                504,
            }:
                wait_seconds = min(
                    60,
                    3 * (2 ** (attempt - 1)),
                )

                print(
                    "  [HTTP RETRY] "
                    f"HTTP={response.status_code}，"
                    f"{wait_seconds} 秒後重試，"
                    f"attempt={attempt}/"
                    f"{HTTP_MAX_ATTEMPTS}"
                )

                time.sleep(
                    wait_seconds
                )

                last_error = RuntimeError(
                    f"HTTP {response.status_code}"
                )

                continue

            if response.status_code != 200:
                preview = (
                    response_text[:400]
                    .replace("\n", " ")
                )

                raise RuntimeError(
                    f"HTTP {response.status_code}: "
                    f"{preview}"
                )

            stripped = (
                response_text
                .lstrip()
            )

            if not (
                stripped.startswith("{")
                or stripped.startswith("[")
            ):
                preview = (
                    stripped[:400]
                    .replace("\n", " ")
                )

                raise RuntimeError(
                    f"回傳不是 JSON：{preview}"
                )

            try:
                payload = response.json()

            except Exception as exc:
                preview = (
                    response_text[:400]
                    .replace("\n", " ")
                )

                raise RuntimeError(
                    f"JSON 解析失敗：{preview}"
                ) from exc

            if not isinstance(
                payload,
                dict,
            ):
                raise RuntimeError(
                    "JSON 最外層不是 object"
                )

            return payload

        except Exception as exc:
            last_error = exc

            if attempt >= HTTP_MAX_ATTEMPTS:
                break

            wait_seconds = min(
                30,
                2 * attempt,
            )

            print(
                "  [REQUEST ERROR] "
                f"{type(exc).__name__}: {exc}，"
                f"{wait_seconds} 秒後重試"
            )

            session.cookies.clear()

            time.sleep(
                wait_seconds
            )

    raise RuntimeError(
        f"API 重試失敗：{last_error}"
    )


# ======================================================================================
# 找股票資料表
# ======================================================================================
def candidate_tables(
    payload: dict[str, Any],
) -> list[tuple[str, dict[str, Any]]]:
    output: list[
        tuple[str, dict[str, Any]]
    ] = []

    tables = (
        payload.get("tables")
        or []
    )

    for index, table in enumerate(
        tables
    ):
        if not isinstance(
            table,
            dict,
        ):
            continue

        output.append(
            (
                f"tables[{index}]",
                table,
            )
        )

    top_data = payload.get("data")

    if isinstance(
        top_data,
        list,
    ):
        output.append(
            (
                "top_level_data",
                {
                    "fields": (
                        payload.get("fields")
                        or []
                    ),
                    "data": top_data,
                    "date": (
                        payload.get("date")
                        or ""
                    ),
                },
            )
        )

    return output


def choose_stock_table(
    payload: dict[str, Any],
    min_row_length: int,
) -> tuple[str, dict[str, Any]] | None:
    """
    找出真正的個股資料表。

    不能只掃描前 100 或前 200 列。
    selectType=ALL 時，0 開頭 ETF 等商品可能排在前面，
    普通股票會出現在更後面。
    """
    best_name: str | None = None
    best_table: dict[str, Any] | None = None
    best_common_stock_count = 0

    for name, table in candidate_tables(
        payload
    ):
        rows = (
            table.get("data")
            or []
        )

        common_stock_count = 0

        # 修正重點：掃描整張表
        for row in rows:
            if not isinstance(
                row,
                list,
            ):
                continue

            if len(row) < min_row_length:
                continue

            if is_common_stock(
                row[0]
            ):
                common_stock_count += 1

        if (
            common_stock_count
            > best_common_stock_count
        ):
            best_common_stock_count = (
                common_stock_count
            )

            best_name = name
            best_table = table

    if (
        best_name is None
        or best_table is None
        or best_common_stock_count == 0
    ):
        return None

    return (
        best_name,
        best_table,
    )


def table_debug_text(
    payload: dict[str, Any],
) -> str:
    items: list[str] = []

    for name, table in candidate_tables(
        payload
    ):
        rows = (
            table.get("data")
            or []
        )

        first_length = 0

        if (
            rows
            and isinstance(
                rows[0],
                list,
            )
        ):
            first_length = len(
                rows[0]
            )

        fields = (
            table.get("fields")
            or []
        )

        common_stock_count = 0

        for row in rows:
            if not isinstance(
                row,
                list,
            ):
                continue

            if not row:
                continue

            if is_common_stock(
                row[0]
            ):
                common_stock_count += 1

        items.append(
            (
                f"{name}:"
                f"rows={len(rows)},"
                f"fields={len(fields)},"
                f"first_len={first_length},"
                f"common_stocks="
                f"{common_stock_count}"
            )
        )

    return " | ".join(items)


# ======================================================================================
# TWSE 一列解析
# ======================================================================================
def parse_twse_margin_row(
    row: list[Any],
    source_date: pd.Timestamp,
    target_date: pd.Timestamp,
    source_label: str,
) -> dict[str, Any] | None:
    if len(row) < 14:
        return None

    code = normalize_stock_code(
        row[0]
    )

    if not is_common_stock(
        code
    ):
        return None

    if len(row) >= 16:
        margin_buy = to_int(row[2])
        margin_sell = to_int(row[3])
        margin_cash_repay = to_int(row[4])
        margin_balance_prev = to_int(row[5])
        margin_balance = to_int(row[6])
        margin_quota = to_int(row[7])

        short_buy = to_int(row[8])
        short_sell = to_int(row[9])
        short_stock_repay = to_int(row[10])
        short_balance_prev = to_int(row[11])
        short_balance = to_int(row[12])
        short_quota = to_int(row[13])

        offset = to_int(row[14])
        note = str(row[15]).strip()

    elif len(row) == 15:
        margin_buy = to_int(row[2])
        margin_sell = to_int(row[3])
        margin_cash_repay = to_int(row[4])
        margin_balance_prev = to_int(row[5])
        margin_balance = to_int(row[6])
        margin_quota = to_int(row[7])

        short_buy = to_int(row[8])
        short_sell = to_int(row[9])
        short_stock_repay = to_int(row[10])
        short_balance_prev = to_int(row[11])
        short_balance = to_int(row[12])
        short_quota = to_int(row[13])

        offset = to_int(row[14])
        note = ""

    else:
        margin_buy = to_int(row[2])
        margin_sell = to_int(row[3])
        margin_cash_repay = to_int(row[4])
        margin_balance_prev = to_int(row[5])
        margin_balance = to_int(row[6])
        margin_quota = 0

        short_buy = to_int(row[7])
        short_sell = to_int(row[8])
        short_stock_repay = to_int(row[9])
        short_balance_prev = to_int(row[10])
        short_balance = to_int(row[11])
        short_quota = 0

        offset = to_int(row[12])
        note = str(row[13]).strip()

    return {
        "source_trade_date": (
            source_date.strftime(
                "%Y-%m-%d"
            )
        ),
        "target_trade_date": (
            target_date.strftime(
                "%Y-%m-%d"
            )
        ),
        "stock_code": code,
        "stock_name": (
            str(row[1]).strip()
        ),
        "market": "TWSE",
        "margin_buy": margin_buy,
        "margin_sell": margin_sell,
        "margin_cash_repay": (
            margin_cash_repay
        ),
        "margin_balance_prev": (
            margin_balance_prev
        ),
        "margin_balance": (
            margin_balance
        ),
        "margin_quota": (
            margin_quota
        ),
        "short_buy": short_buy,
        "short_sell": short_sell,
        "short_stock_repay": (
            short_stock_repay
        ),
        "short_balance_prev": (
            short_balance_prev
        ),
        "short_balance": (
            short_balance
        ),
        "short_quota": (
            short_quota
        ),
        "offset": offset,
        "note": note,
        "source": source_label,
    }


# ======================================================================================
# TWSE 上市融資融券
# ======================================================================================
def fetch_twse(
    session: requests.Session,
    source_date: pd.Timestamp,
    target_date: pd.Timestamp,
) -> tuple[pd.DataFrame, str]:
    errors: list[str] = []

    for twse_url in TWSE_URLS:
        try:
            payload = get_json(
                session=session,
                url=twse_url,
                params={
                    "response": "json",
                    "date": (
                        source_date.strftime(
                            "%Y%m%d"
                        )
                    ),
                    "selectType": "ALL",
                },
                referer=(
                    "https://www.twse.com.tw/"
                    "zh/trading/margin/"
                    "mi-margn.html"
                ),
            )

            stat = str(
                payload.get("stat")
                or ""
            )

            if (
                stat
                and stat != "OK"
            ):
                errors.append(
                    (
                        f"{twse_url}:"
                        f"STAT={stat}"
                    )
                )

                continue

            selected = choose_stock_table(
                payload=payload,
                min_row_length=14,
            )

            if selected is None:
                errors.append(
                    (
                        f"{twse_url}:"
                        "NO_STOCK_TABLE:"
                        f"{table_debug_text(payload)}"
                    )
                )

                continue

            table_name, table = selected

            if (
                twse_url
                == TWSE_URLS[0]
            ):
                source_label = (
                    "TWSE_MI_MARGN_EXCHANGE_REPORT"
                )
            else:
                source_label = (
                    "TWSE_MI_MARGN_RWD"
                )

            records: list[
                dict[str, Any]
            ] = []

            layout_counts: dict[
                str,
                int,
            ] = {}

            for row in (
                table.get("data")
                or []
            ):
                if not isinstance(
                    row,
                    list,
                ):
                    continue

                if len(row) >= 16:
                    layout = "16_plus"
                elif len(row) == 15:
                    layout = "15"
                elif len(row) == 14:
                    layout = "14"
                else:
                    layout = "too_short"

                layout_counts[layout] = (
                    layout_counts.get(
                        layout,
                        0,
                    )
                    + 1
                )

                record = parse_twse_margin_row(
                    row=row,
                    source_date=source_date,
                    target_date=target_date,
                    source_label=source_label,
                )

                if record is not None:
                    records.append(
                        record
                    )

            df = pd.DataFrame(
                records,
                columns=OUTPUT_COLUMNS,
            )

            df = finalize(
                df
            )

            if df.empty:
                errors.append(
                    (
                        f"{twse_url}:"
                        f"NO_COMMON_STOCK_DATA:"
                        f"table={table_name}:"
                        f"layouts={layout_counts}"
                    )
                )

                continue

            if not (
                MIN_TWSE_ROWS
                <= len(df)
                <= MAX_TWSE_ROWS
            ):
                errors.append(
                    (
                        f"{twse_url}:"
                        f"BAD_ROW_COUNT={len(df)}:"
                        f"table={table_name}:"
                        f"layouts={layout_counts}"
                    )
                )

                continue

            margin_rate, short_rate = (
                balance_identity_rates(
                    df
                )
            )

            if (
                margin_rate
                < MIN_BALANCE_IDENTITY_RATE
                or short_rate
                < MIN_BALANCE_IDENTITY_RATE
            ):
                errors.append(
                    (
                        f"{twse_url}:"
                        "BAD_IDENTITY_RATE:"
                        f"margin={margin_rate:.4f},"
                        f"short={short_rate:.4f},"
                        f"table={table_name},"
                        f"layouts={layout_counts}"
                    )
                )

                continue

            return (
                df,
                "OK",
            )

        except Exception as exc:
            errors.append(
                (
                    f"{twse_url}:"
                    f"{type(exc).__name__}:"
                    f"{exc}"
                )
            )

    return (
        empty_df(),
        " | ".join(errors)
        if errors
        else "NO_STOCK_TABLE",
    )


# ======================================================================================
# TPEx 上櫃融資融券
# ======================================================================================
def fetch_tpex(
    session: requests.Session,
    source_date: pd.Timestamp,
    target_date: pd.Timestamp,
) -> tuple[pd.DataFrame, str]:
    payload = get_json(
        session=session,
        url=TPEX_URL,
        params={
            "date": (
                source_date.strftime(
                    "%Y/%m/%d"
                )
            ),
            "response": "json",
        },
        referer=(
            "https://www.tpex.org.tw/"
            "zh-tw/mainboard/trading/"
            "margin-trading/"
            "transactions.html"
        ),
    )

    selected = choose_stock_table(
        payload=payload,
        min_row_length=20,
    )

    if selected is None:
        message = str(
            payload.get("message")
            or payload.get("stat")
            or "NO_STOCK_TABLE"
        )

        return (
            empty_df(),
            (
                f"{message}:"
                f"{table_debug_text(payload)}"
            ),
        )

    _, table = selected

    raw_returned_date = (
        table.get("date")
        or payload.get("date")
        or ""
    )

    returned_date = canonical_date(
        raw_returned_date
    )

    expected_date = (
        source_date.strftime(
            "%Y-%m-%d"
        )
    )

    if (
        returned_date
        and returned_date
        != expected_date
    ):
        return (
            empty_df(),
            (
                "WRONG_DATE:"
                f"{raw_returned_date}"
            ),
        )

    records: list[
        dict[str, Any]
    ] = []

    for row in (
        table.get("data")
        or []
    ):
        if not isinstance(
            row,
            list,
        ):
            continue

        if len(row) < 20:
            continue

        code = normalize_stock_code(
            row[0]
        )

        if not is_common_stock(
            code
        ):
            continue

        records.append(
            {
                "source_trade_date": (
                    source_date.strftime(
                        "%Y-%m-%d"
                    )
                ),
                "target_trade_date": (
                    target_date.strftime(
                        "%Y-%m-%d"
                    )
                ),
                "stock_code": code,
                "stock_name": (
                    str(row[1]).strip()
                ),
                "market": "TPEX",
                "margin_balance_prev": (
                    to_int(row[2])
                ),
                "margin_buy": (
                    to_int(row[3])
                ),
                "margin_sell": (
                    to_int(row[4])
                ),
                "margin_cash_repay": (
                    to_int(row[5])
                ),
                "margin_balance": (
                    to_int(row[6])
                ),
                "margin_quota": (
                    to_int(row[9])
                ),
                "short_balance_prev": (
                    to_int(row[10])
                ),
                "short_sell": (
                    to_int(row[11])
                ),
                "short_buy": (
                    to_int(row[12])
                ),
                "short_stock_repay": (
                    to_int(row[13])
                ),
                "short_balance": (
                    to_int(row[14])
                ),
                "short_quota": (
                    to_int(row[17])
                ),
                "offset": (
                    to_int(row[18])
                ),
                "note": (
                    str(row[19]).strip()
                ),
                "source": (
                    "TPEX_MARGIN_BALANCE"
                ),
            }
        )

    df = pd.DataFrame(
        records,
        columns=OUTPUT_COLUMNS,
    )

    df = finalize(
        df
    )

    if df.empty:
        return (
            df,
            "NO_DATA",
        )

    if not (
        MIN_TPEX_ROWS
        <= len(df)
        <= MAX_TPEX_ROWS
    ):
        return (
            empty_df(),
            f"BAD_ROW_COUNT:{len(df)}",
        )

    margin_rate, short_rate = (
        balance_identity_rates(
            df
        )
    )

    if (
        margin_rate
        < MIN_BALANCE_IDENTITY_RATE
    ):
        return (
            empty_df(),
            (
                "BAD_MARGIN_IDENTITY_RATE:"
                f"{margin_rate:.4f}"
            ),
        )

    if (
        short_rate
        < MIN_BALANCE_IDENTITY_RATE
    ):
        return (
            empty_df(),
            (
                "BAD_SHORT_IDENTITY_RATE:"
                f"{short_rate:.4f}"
            ),
        )

    return (
        df,
        "OK",
    )


# ======================================================================================
# Calendar 對齊
# ======================================================================================
def load_calendar_mapping(
    calendar_file: Path,
    date_start: pd.Timestamp,
    date_end: pd.Timestamp,
) -> pd.DataFrame:
    if not calendar_file.exists():
        raise FileNotFoundError(
            f"找不到 calendar：{calendar_file}"
        )

    calendar = pd.read_csv(
        calendar_file,
        dtype=str,
        encoding=ENCODING,
    )

    if (
        CALENDAR_TW_COL
        not in calendar.columns
    ):
        raise KeyError(
            f"calendar 找不到欄位："
            f"{CALENDAR_TW_COL}\n"
            f"目前欄位："
            f"{list(calendar.columns)}"
        )

    tw_dates = (
        pd.to_datetime(
            calendar[
                CALENDAR_TW_COL
            ],
            errors="coerce",
        )
        .dropna()
        .dt.normalize()
        .drop_duplicates()
        .sort_values()
        .reset_index(
            drop=True,
        )
    )

    if len(tw_dates) < 2:
        raise RuntimeError(
            "calendar 有效交易日不足 2 天"
        )

    mapping = pd.DataFrame(
        {
            "source_trade_date": (
                tw_dates
                .iloc[:-1]
                .to_numpy()
            ),
            "target_trade_date": (
                tw_dates
                .iloc[1:]
                .to_numpy()
            ),
        }
    )

    mapping = mapping[
        mapping[
            "source_trade_date"
        ].between(
            date_start,
            date_end,
        )
    ].copy()

    mapping = (
        mapping
        .drop_duplicates(
            subset=[
                "source_trade_date"
            ],
            keep="last",
        )
        .sort_values(
            "source_trade_date",
            kind="stable",
        )
        .reset_index(
            drop=True,
        )
    )

    return mapping


# ======================================================================================
# 讀取與驗證既有每日檔
# ======================================================================================
def read_existing_daily(
    path: Path,
    source_date_text: str,
    target_date_text: str,
) -> tuple[pd.DataFrame | None, str]:
    if not path.exists():
        return (
            None,
            "NOT_FOUND",
        )

    try:
        df = pd.read_csv(
            path,
            dtype={
                "source_trade_date": str,
                "target_trade_date": str,
                "stock_code": str,
                "market": str,
            },
            encoding=ENCODING,
        )

    except Exception as exc:
        return (
            None,
            (
                "READ_ERROR:"
                f"{type(exc).__name__}:"
                f"{exc}"
            ),
        )

    if df.empty:
        return (
            None,
            "EMPTY",
        )

    if (
        "source_trade_date"
        in df.columns
    ):
        df["source_trade_date"] = (
            source_date_text
        )

    if (
        "target_trade_date"
        in df.columns
    ):
        df["target_trade_date"] = (
            target_date_text
        )

    df = finalize(
        df
    )

    valid, reason = validate_daily(
        df=df,
        expected_source_date=(
            source_date_text
        ),
        expected_target_date=(
            target_date_text
        ),
    )

    if not valid:
        return (
            None,
            reason,
        )

    return (
        df,
        "OK",
    )


# ======================================================================================
# 狀態檔
# ======================================================================================
def update_status_file(
    status_file: Path,
    new_row: dict[str, Any],
) -> bool:
    new_df = pd.DataFrame(
        [new_row]
    )

    if status_file.exists():
        try:
            old_df = pd.read_csv(
                status_file,
                dtype=str,
                encoding=ENCODING,
            )
        except Exception:
            old_df = pd.DataFrame()

        status_df = pd.concat(
            [
                old_df,
                new_df,
            ],
            ignore_index=True,
        )

    else:
        status_df = new_df

    status_df = (
        status_df
        .drop_duplicates(
            subset=[
                "source_trade_date"
            ],
            keep="last",
        )
        .sort_values(
            "source_trade_date",
            kind="stable",
        )
        .reset_index(
            drop=True,
        )
    )

    try:
        write_csv(
            status_df,
            status_file,
        )
        return True

    except PermissionError as exc:
        # 狀態檔只是紀錄，不應因它被鎖住而讓主要下載流程中止。
        print(
            "  [STATUS WARNING] "
            f"無法更新 {status_file.name}：{exc}"
        )
        return False


# ======================================================================================
# 重建全部 aligned 資料
# ======================================================================================
def rebuild_all_outputs(
    aligned_dir: Path,
    all_file: Path,
    latest_preview_file: Path,
    invalid_report_file: Path,
) -> tuple[int, int, int]:
    daily_files = sorted(
        aligned_dir.glob(
            "????-??-??.csv"
        )
    )

    all_file.unlink(
        missing_ok=True,
    )

    latest_preview_file.unlink(
        missing_ok=True,
    )

    invalid_rows: list[
        dict[str, Any]
    ] = []

    valid_day_count = 0
    total_rows = 0
    header_written = False
    latest_valid_df: pd.DataFrame | None = None

    for index, daily_file in enumerate(
        daily_files,
        start=1,
    ):
        try:
            df = pd.read_csv(
                daily_file,
                dtype={
                    "source_trade_date": str,
                    "target_trade_date": str,
                    "stock_code": str,
                    "market": str,
                },
                encoding=ENCODING,
            )

        except Exception as exc:
            invalid_rows.append(
                {
                    "file": str(daily_file),
                    "reason": (
                        f"READ_ERROR:"
                        f"{type(exc).__name__}:"
                        f"{exc}"
                    ),
                }
            )

            continue

        df = finalize(
            df
        )

        valid, reason = validate_daily(
            df
        )

        if not valid:
            invalid_rows.append(
                {
                    "file": str(daily_file),
                    "reason": reason,
                }
            )

            continue

        df.to_csv(
            all_file,
            mode="a",
            header=not header_written,
            index=False,
            encoding=ENCODING,
        )

        header_written = True
        valid_day_count += 1
        total_rows += len(df)
        latest_valid_df = df

        if (
            index % 100 == 0
            or index == len(daily_files)
        ):
            print(
                "重建進度："
                f"{index:,}/"
                f"{len(daily_files):,}，"
                f"有效日={valid_day_count:,}，"
                f"累計列數={total_rows:,}"
            )

    invalid_df = pd.DataFrame(
        invalid_rows,
        columns=[
            "file",
            "reason",
        ],
    )

    write_csv(
        invalid_df,
        invalid_report_file,
    )

    if latest_valid_df is not None:
        write_csv(
            latest_valid_df,
            latest_preview_file,
        )

    return (
        valid_day_count,
        total_rows,
        len(invalid_rows),
    )


# ======================================================================================
# 命令列參數
# ======================================================================================
def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "下載上市＋上櫃融資融券，"
            "並對齊下一個台股交易日。"
        )
    )

    parser.add_argument(
        "--project-root",
        type=Path,
        default=PROJECT_ROOT,
    )

    parser.add_argument(
        "--calendar-file",
        type=Path,
        default=CALENDAR_FILE,
    )

    parser.add_argument(
        "--date-start",
        default=DATE_START,
    )

    parser.add_argument(
        "--date-end",
        default=DATE_END,
    )

    parser.add_argument(
        "--overwrite",
        action="store_true",
        help=(
            "強制重新下載指定期間，"
            "包括原本正確的每日檔。"
        ),
    )

    parser.add_argument(
        "--rebuild-only",
        action="store_true",
        help=(
            "不呼叫 API，只重建總檔。"
        ),
    )

    return parser.parse_args()


# ======================================================================================
# 主程式
# ======================================================================================
def main() -> int:
    args = parse_args()

    project_root = (
        args.project_root
        .resolve()
    )

    calendar_file = (
        args.calendar_file
        .resolve()
    )

    date_start = (
        pd.Timestamp(
            args.date_start
        )
        .normalize()
    )

    date_end = (
        pd.Timestamp(
            args.date_end
        )
        .normalize()
        if args.date_end
        else pd.Timestamp(
            date.today()
        ).normalize()
    )

    if date_start > date_end:
        raise ValueError(
            f"date_start={date_start.date()} "
            f"晚於 date_end={date_end.date()}"
        )

    mapping = load_calendar_mapping(
        calendar_file=calendar_file,
        date_start=date_start,
        date_end=date_end,
    )

    if mapping.empty:
        raise RuntimeError(
            "指定期間在 calendar 中"
            "沒有任何 source_trade_date"
        )

    raw_root = (
        project_root
        / "data_raw"
        / "16_margin_trading_v5"
    )

    processed_root = (
        project_root
        / "data_processed"
        / "16_margin_trading_v5"
    )

    raw_dir = (
        raw_root
        / "by_source_trade_date"
    )

    aligned_dir = (
        processed_root
        / "by_target_trade_date"
    )

    status_file = (
        raw_root
        / "download_status.csv"
    )

    all_file = (
        processed_root
        / "margin_trading_aligned_all.csv"
    )

    latest_preview_file = (
        processed_root
        / "latest_preview.csv"
    )

    invalid_report_file = (
        processed_root
        / "invalid_daily_files.csv"
    )

    raw_dir.mkdir(
        parents=True,
        exist_ok=True,
    )

    aligned_dir.mkdir(
        parents=True,
        exist_ok=True,
    )

    banner(
        "16 Download Margin Trading V5 "
        "- FIXED V3"
    )

    print(
        f"SCRIPT_VERSION      : "
        f"{SCRIPT_VERSION}"
    )

    print(
        f"PROJECT_ROOT        : "
        f"{project_root}"
    )

    print(
        f"CALENDAR_FILE       : "
        f"{calendar_file}"
    )

    print(
        f"CALENDAR_TW_COL     : "
        f"{CALENDAR_TW_COL}"
    )

    print(
        f"DATE_START          : "
        f"{date_start.date()}"
    )

    print(
        f"DATE_END            : "
        f"{date_end.date()}"
    )

    print(
        f"SOURCE_DATE_COUNT   : "
        f"{len(mapping):,}"
    )

    print(
        "ALIGNMENT           : "
        "source_trade_date "
        "-> next tw_trade_date"
    )

    print(
        f"TWSE_PRIMARY_URL    : "
        f"{TWSE_URLS[0]}"
    )

    print(
        f"TWSE_FALLBACK_URL   : "
        f"{TWSE_URLS[1]}"
    )

    print(
        f"TPEX_URL            : "
        f"{TPEX_URL}"
    )

    print(
        "OUTPUT_COMPRESSION  : "
        "None"
    )

    started = time.perf_counter()

    if not args.rebuild_only:
        session = make_session()

        try:
            for index, mapping_row in enumerate(
                mapping.itertuples(
                    index=False
                ),
                start=1,
            ):
                source_date = pd.Timestamp(
                    mapping_row.source_trade_date
                )

                target_date = pd.Timestamp(
                    mapping_row.target_trade_date
                )

                source_text = (
                    source_date.strftime(
                        "%Y-%m-%d"
                    )
                )

                target_text = (
                    target_date.strftime(
                        "%Y-%m-%d"
                    )
                )

                raw_file = (
                    raw_dir
                    / f"{source_text}.csv"
                )

                aligned_file = (
                    aligned_dir
                    / f"{target_text}.csv"
                )

                day_started = (
                    time.perf_counter()
                )

                existing: pd.DataFrame | None
                existing_reason: str

                if args.overwrite:
                    existing = None
                    existing_reason = (
                        "OVERWRITE"
                    )

                else:
                    (
                        existing,
                        existing_reason,
                    ) = read_existing_daily(
                        path=raw_file,
                        source_date_text=source_text,
                        target_date_text=target_text,
                    )

                api_called = False

                if existing is not None:
                    write_csv(
                        existing,
                        aligned_file,
                    )

                    twse_rows = int(
                        (
                            existing["market"]
                            == "TWSE"
                        ).sum()
                    )

                    tpex_rows = int(
                        (
                            existing["market"]
                            == "TPEX"
                        ).sum()
                    )

                    total_rows = len(
                        existing
                    )

                    twse_status = (
                        "EXISTING_VALID"
                    )

                    tpex_status = (
                        "EXISTING_VALID"
                    )

                    result = (
                        "SKIPPED_EXISTING_VALID"
                    )

                else:
                    api_called = True

                    if raw_file.exists():
                        print(
                            f"  [REPAIR] {source_text} "
                            f"舊檔無效："
                            f"{existing_reason}"
                        )

                        raw_file.unlink(
                            missing_ok=True,
                        )

                    aligned_file.unlink(
                        missing_ok=True,
                    )

                    try:
                        (
                            twse_df,
                            twse_status,
                        ) = fetch_twse(
                            session=session,
                            source_date=source_date,
                            target_date=target_date,
                        )

                    except Exception as exc:
                        twse_df = empty_df()

                        twse_status = (
                            f"ERROR:"
                            f"{type(exc).__name__}:"
                            f"{exc}"
                        )

                    time.sleep(
                        random.uniform(
                            MARKET_SLEEP_MIN,
                            MARKET_SLEEP_MAX,
                        )
                    )

                    try:
                        (
                            tpex_df,
                            tpex_status,
                        ) = fetch_tpex(
                            session=session,
                            source_date=source_date,
                            target_date=target_date,
                        )

                    except Exception as exc:
                        tpex_df = empty_df()

                        tpex_status = (
                            f"ERROR:"
                            f"{type(exc).__name__}:"
                            f"{exc}"
                        )

                    twse_rows = len(
                        twse_df
                    )

                    tpex_rows = len(
                        tpex_df
                    )

                    total_rows = 0

                    if (
                        twse_status == "OK"
                        and tpex_status == "OK"
                    ):
                        combined = pd.concat(
                            [
                                twse_df,
                                tpex_df,
                            ],
                            ignore_index=True,
                        )

                        combined = finalize(
                            combined
                        )

                        valid, reason = validate_daily(
                            df=combined,
                            expected_source_date=(
                                source_text
                            ),
                            expected_target_date=(
                                target_text
                            ),
                        )

                        if valid:
                            write_csv(
                                combined,
                                raw_file,
                            )

                            write_csv(
                                combined,
                                aligned_file,
                            )

                            total_rows = len(
                                combined
                            )

                            if (
                                existing_reason
                                not in {
                                    "NOT_FOUND",
                                    "OVERWRITE",
                                }
                            ):
                                result = (
                                    "REPAIRED_WRITTEN"
                                )
                            else:
                                result = "WRITTEN"

                        else:
                            result = (
                                "INVALID_NOT_WRITTEN:"
                                f"{reason}"
                            )

                    else:
                        result = (
                            "PARTIAL_NOT_WRITTEN"
                        )

                elapsed = round(
                    time.perf_counter()
                    - day_started,
                    3,
                )

                status_row = {
                    "source_trade_date": (
                        source_text
                    ),
                    "target_trade_date": (
                        target_text
                    ),
                    "raw_file": (
                        str(raw_file)
                        if total_rows > 0
                        else ""
                    ),
                    "aligned_file": (
                        str(aligned_file)
                        if total_rows > 0
                        else ""
                    ),
                    "twse_rows": (
                        twse_rows
                    ),
                    "tpex_rows": (
                        tpex_rows
                    ),
                    "total_rows": (
                        total_rows
                    ),
                    "twse_status": (
                        twse_status
                    ),
                    "tpex_status": (
                        tpex_status
                    ),
                    "result": result,
                    "elapsed_sec": (
                        elapsed
                    ),
                    "updated_at": (
                        datetime.now()
                        .strftime(
                            "%Y-%m-%d "
                            "%H:%M:%S"
                        )
                    ),
                }

                # 已存在且驗證正確的日期不必反覆重寫狀態檔。
                # 這可大幅降低 Windows 檔案鎖與整體 I/O。
                if result != "SKIPPED_EXISTING_VALID":
                    update_status_file(
                        status_file=status_file,
                        new_row=status_row,
                    )

                print(
                    f"[{index:4d}/"
                    f"{len(mapping):4d}] "
                    f"{source_text} "
                    f"-> {target_text} "
                    f"{result:<28} "
                    f"TWSE={twse_rows:4d} "
                    f"TPEX={tpex_rows:4d} "
                    f"TOTAL={total_rows:4d} "
                    f"{elapsed:6.2f}s"
                )

                if twse_status not in {
                    "OK",
                    "EXISTING_VALID",
                }:
                    print(
                        "  TWSE_STATUS: "
                        f"{twse_status}"
                    )

                if tpex_status not in {
                    "OK",
                    "EXISTING_VALID",
                }:
                    print(
                        "  TPEX_STATUS: "
                        f"{tpex_status}"
                    )

                if api_called:
                    time.sleep(
                        random.uniform(
                            DAY_SLEEP_MIN,
                            DAY_SLEEP_MAX,
                        )
                    )

        finally:
            session.close()

    banner(
        "Rebuild All Aligned CSV"
    )

    (
        valid_day_count,
        total_row_count,
        invalid_file_count,
    ) = rebuild_all_outputs(
        aligned_dir=aligned_dir,
        all_file=all_file,
        latest_preview_file=(
            latest_preview_file
        ),
        invalid_report_file=(
            invalid_report_file
        ),
    )

    banner("Done")

    print(
        f"VALID_DAY_COUNT     : "
        f"{valid_day_count:,}"
    )

    print(
        f"INVALID_FILE_COUNT  : "
        f"{invalid_file_count:,}"
    )

    print(
        f"TOTAL_ROW_COUNT     : "
        f"{total_row_count:,}"
    )

    print(
        f"RAW_DIR             : "
        f"{raw_dir}"
    )

    print(
        f"ALIGNED_DIR         : "
        f"{aligned_dir}"
    )

    print(
        f"ALL_FILE            : "
        f"{all_file}"
    )

    print(
        f"STATUS_FILE         : "
        f"{status_file}"
    )

    print(
        f"INVALID_REPORT      : "
        f"{invalid_report_file}"
    )

    print(
        f"ELAPSED_SEC         : "
        f"{time.perf_counter() - started:,.2f}"
    )

    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(
            main()
        )

    except KeyboardInterrupt:
        print(
            "\n使用者中止。"
        )

        raise SystemExit(130)
