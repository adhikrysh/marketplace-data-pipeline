"""Clean the raw customers.csv dataset and export clean_customers.csv.

Issues addressed:
- join_date values containing commas were split into extra columns; recombine segments.
- join_date appears in multiple formats; coerce to ISO-8601 (YYYY-MM-DD).
- String fields carry stray whitespace/null tokens; trim and normalize missing markers.
- City names show casing/spacing drift; map to canonical names.
- Email addresses may repeat or be missing; lower-case, validate shape, drop duplicate non-null emails.
"""

from __future__ import annotations

import csv
import re
from pathlib import Path
from typing import Dict, List, Optional

import pandas as pd

RAW_FILE = Path(__file__).with_name("customers.csv")
OUTPUT_FILE = Path(__file__).with_name("clean_customers.csv")

EXPECTED_COLUMNS = ["customer_id", "first_name", "last_name", "email", "join_date", "city"]
NULL_TOKENS = {"", "nan", "none", "null", "n/a"}
KNOWN_CITIES = {
    "bangkok": "Bangkok",
    "ho chi minh city": "Ho Chi Minh City",
    "jakarta": "Jakarta",
    "kuala lumpur": "Kuala Lumpur",
    "manila": "Manila",
    "singapore": "Singapore",
}
EMAIL_PATTERN = re.compile(r"^[^@\s]+@[^@\s]+\.[^@\s]+$")
YEAR_TOKENS = ("2023", "2024")


def _reshape_row(raw: Dict[Optional[str], str]) -> List[str] | None:
    """Ensure every record has exactly the expected columns and repair split dates."""
    core_values = {key: (value or "") for key, value in raw.items() if key in EXPECTED_COLUMNS}
    extras = raw.get(None) or []
    extras = [extra.strip() for extra in extras if extra and extra.strip()]

    if not any(value.strip() for value in core_values.values()) and not extras:
        return None

    join_date = core_values.get("join_date", "").strip()
    city_value = core_values.get("city", "").strip()

    if any(token in join_date for token in YEAR_TOKENS):
        if extras:
            city_value = extras[-1]
    else:
        if any(token in city_value for token in YEAR_TOKENS):
            join_date = ", ".join(filter(None, [join_date, city_value]))
            if extras:
                city_value = extras[-1]
            else:
                city_value = ""
        elif extras:
            # Assume final extra is city, the rest belong to the date string.
            if len(extras) > 1:
                join_date = ", ".join(filter(None, [join_date] + extras[:-1]))
            elif join_date:
                join_date = ", ".join([join_date, extras[0]])
            else:
                join_date = extras[0]
            city_value = extras[-1]

    normalized = [
        core_values.get("customer_id", "").strip(),
        core_values.get("first_name", "").strip(),
        core_values.get("last_name", "").strip(),
        core_values.get("email", "").strip(),
        join_date,
        city_value,
    ]
    return normalized


def _load_raw_customers(path: Path) -> pd.DataFrame:
    with path.open(newline="", encoding="utf-8") as handle:
        reader = csv.DictReader(handle)
        header = [name for name in reader.fieldnames if name is not None] if reader.fieldnames else []
        if header[: len(EXPECTED_COLUMNS)] != EXPECTED_COLUMNS:
            raise ValueError(f"Unexpected header {reader.fieldnames}; expected {EXPECTED_COLUMNS}")

        records = []
        for row in reader:
            normalized = _reshape_row(row)
            if normalized:
                records.append(normalized)

    return pd.DataFrame(records, columns=EXPECTED_COLUMNS, dtype="string")


def _normalize_strings(df: pd.DataFrame) -> pd.DataFrame:
    for column in EXPECTED_COLUMNS:
        series = df[column].astype("string")
        series = series.str.strip()
        lower = series.str.lower()
        null_mask = lower.isin(NULL_TOKENS).fillna(False)
        df[column] = series.mask(null_mask)
    return df


def _normalize_city(value: pd.Series) -> pd.Series:
    def _clean(single: object) -> object:
        if pd.isna(single):
            return pd.NA
        text = str(single).strip()
        if not text:
            return pd.NA
        key = " ".join(text.split()).lower()
        if key in NULL_TOKENS:
            return pd.NA
        return KNOWN_CITIES.get(key, " ".join(part.capitalize() for part in key.split()))

    return value.map(_clean)


def _sanitize_emails(series: pd.Series) -> pd.Series:
    series = series.str.lower()
    invalid_mask = series.notna() & ~series.str.match(EMAIL_PATTERN)
    return series.mask(invalid_mask)


def clean_customers() -> pd.DataFrame:
    df = _load_raw_customers(RAW_FILE)
    df = _normalize_strings(df)
    df["email"] = _sanitize_emails(df["email"])
    df["city"] = _normalize_city(df["city"])

    join_dates = pd.to_datetime(df["join_date"], errors="coerce")
    df = df.assign(join_date_parsed=join_dates)
    df = df[df["join_date_parsed"].notna()].copy()

    df = df.sort_values(["join_date_parsed", "customer_id"])
    email_dupes = df["email"].notna() & df["email"].duplicated(keep="first")
    df = df.loc[~email_dupes]
    df = df.loc[~df["customer_id"].duplicated(keep="first")]

    df["join_date"] = df["join_date_parsed"].dt.strftime("%Y-%m-%d")
    df = df.drop(columns="join_date_parsed")
    df = df.sort_values("customer_id").reset_index(drop=True)
    return df.astype("string")


def main() -> None:
    cleaned = clean_customers()
    cleaned.to_csv(OUTPUT_FILE, index=False)


if __name__ == "__main__":
    main()
