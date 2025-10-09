import pandas as pd
import numpy as np
from datetime import datetime

NAN_PAT = r'(?i)^\s*(nan|na|n/a|null|none)?\s*$'  # case-insensitive “empty-ish” tokens

#customers.csv
_DATE_FORMATS = [
    "%Y-%m-%d",      # 2023-01-15
    "%m/%d/%Y",      # 02/20/2023  (explicitly month-first)
    "%b %d %Y",      # Mar 05 2023
    "%B %d %Y",      # March 05 2023
    "%b %d, %Y",     # Mar 05, 2023 (if any commas slipped through)
    "%B %d, %Y",     # March 05, 2023
]

def _parse_mixed_date(value):
    if pd.isna(value):
        return pd.NaT
    s = str(value).strip().replace("  ", " ")
    # Fast path: if it looks like MM/DD/YYYY, force that rule
    if "/" in s and s.count("/") == 2:
        try:
            return datetime.strptime(s, "%m/%d/%Y").date()
        except ValueError:
            pass
    # Try known formats
    for fmt in _DATE_FORMATS:
        try:
            return datetime.strptime(s, fmt).date()
        except ValueError:
            continue
    # Last-resort flexible parser (still month-first)
    return pd.to_datetime(s, errors="coerce", dayfirst=False).date() if pd.to_datetime(s, errors="coerce", dayfirst=False) is not pd.NaT else pd.NaT

def cleaning_customers(input_file="customers.csv", output_file="clean_customers.csv"):
    """
    Repair and clean a customers CSV:
      - Merge extra comma pieces into join_date
      - Strip whitespace
      - Normalise empty-ish tokens to NaN
      - Parse join_date -> YYYY-MM-DD
      - Uppercase city
    """
    # ---- 1) Read raw lines (avoid pandas parse errors first) ----
    with open(input_file, "r", encoding="utf-8") as f:
        lines = [line.rstrip("\n") for line in f]
    if not lines:
        raise ValueError("File is empty")

    header = [h.strip() for h in lines[0].split(",")]
    n_cols = len(header)

    # ---- 2) Repair rows with commas inside join_date ----
    fixed_rows = []
    for line in lines[1:]:
        parts = [p.strip() for p in line.split(",")]

        if len(parts) == n_cols:
            fixed_rows.append(parts)

        elif len(parts) == n_cols + 1:
            # Exactly one extra piece → assume it split the date
            # columns: 0..3 | [4 join_date] [5 overflow] | 6 city
            # merge 4 and 5 into a single date with a space
            parts[4] = parts[4] + " " + parts[5]
            parts.pop(5)  # remove overflow
            fixed_rows.append(parts)

        elif len(parts) > n_cols + 1:
            # Many extras → merge everything from index 4 up to last-1 into join_date
            merged_date = " ".join(parts[4:-1])
            fixed_rows.append(parts[:4] + [merged_date, parts[-1]])

        else:
            # Too few columns → pad
            parts = parts + [""] * (n_cols - len(parts))
            fixed_rows.append(parts)

    df = pd.DataFrame(fixed_rows, columns=header)

    # ---- 3) Strip whitespace in all string cells ----
    df = df.apply(lambda s: s.str.strip() if s.dtype == "object" else s)
    # Sometimes quotes sneak in
    df = df.apply(lambda s: s.str.strip('"') if s.dtype == "object" else s)

    # ---- 4) Normalise empties and “nan-like” tokens to real NaN ----
    df = df.replace(NAN_PAT, np.nan, regex=True)

    # ---- 5) Standardise join_date → YYYY-MM-DD ----
    # Standardise join_date → YYYY-MM-DD with explicit rules above
    parsed = df["join_date"].map(_parse_mixed_date)
    df["join_date"] = pd.to_datetime(parsed, errors="coerce").dt.strftime("%Y-%m-%d")

    # ---- 6) Uppercase city ----
    if "city" in df.columns:
        df["city"] = df["city"].str.upper()

    # ---- 7) Done ----
    #pd.set_option("display.max_rows", None)
    #print(df)        
    df.to_csv(output_file, index=False)

#orders.csv
# Accepted datetime formats (order matters; fastest/most common first)
_DT_FORMATS = [
    "%Y-%m-%d %H:%M:%S",     # 2024-01-12 06:01:07
    "%m/%d/%Y %H:%M:%S",     # 01/12/2024 07:01:07  (US month-first)
    "%I:%M %p %b %d, %Y",    # 7:59 AM Jan 14, 2024
    "%I:%M %p %B %d, %Y",    # 7:59 AM January 14, 2024
]

def _parse_mixed_dt(val: str):
    """Parse heterogeneous datetimes to pandas.Timestamp (NaT if unparseable)."""
    if pd.isna(val):
        return pd.NaT
    s = str(val).strip().replace("  ", " ")
    # Fast-path for slash datetimes → force MM/DD/YYYY rule
    if "/" in s and s.count("/") == 2 and ":" in s:
        try:
            return pd.Timestamp(datetime.strptime(s, "%m/%d/%Y %H:%M:%S"))
        except ValueError:
            pass
    # Try explicit formats
    for fmt in _DT_FORMATS:
        try:
            return pd.Timestamp(datetime.strptime(s, fmt))
        except ValueError:
            continue
    # Fallback to pandas' parser (still month-first by default)
    return pd.to_datetime(s, errors="coerce")

import re

def _normalize_status(x: str) -> str:
    """Map messy statuses to PLACED/SHIPPED/DELIVERED/CANCELLED/UNKNOWN."""
    if pd.isna(x):
        return "UNKNOWN"
    t = str(x).strip().lower()

    # CANCELLED: cancel, canceled, cancelled, canceld, canceledd, canceled/cancelled variants
    if re.search(r'cancel', t):
        return "CANCELLED"

    # SHIPPED: shiped, shipped, shippd, ship, etc.
    if re.search(r'ship+p*e*d*', t):  # matches 'shiped', 'shipped', 'shippd', 'shippedd'
        return "SHIPPED"

    # DELIVERED: delivered, deliverd, delivred, delivrd, delvd, etc.
    if re.search(r'deliv\w*', t) or re.search(r'deliver\w*', t):
        return "DELIVERED"

    # PLACED: placed, placedd, place, etc.
    if re.search(r'plac\w*', t):
        return "PLACED"

    return "UNKNOWN"

def cleaning_orders(input_file="orders.csv", output_file="clean_orders.csv"):
    """
    Clean orders.csv:
      - Trim whitespace & normalize empties to NaN
      - Standardize order_status to PLACED/SHIPPED/DELIVERED/CANCELLED/UNKNOWN
      - Parse event_timestamp to ISO 'YYYY-MM-DD HH:MM:SS'
      - Cast quantity to Int64, price_at_purchase to float
      - Sort by order_id, listing_id, event_timestamp
      - Write clean_orders.csv
    """
    # Read safely; skip truly malformed lines but keep quoted AM/PM rows intact
    df = pd.read_csv(input_file, dtype=str, engine="python", on_bad_lines="skip")

    # Strip whitespace in all string cells
    df = df.apply(lambda s: s.str.strip() if s.dtype == "object" else s)

    # Normalize empty-ish tokens to NaN
    df = df.replace(NAN_PAT, np.nan, regex=True)

    # Normalize status
    if "order_status" in df.columns:
        df["order_status"] = df["order_status"].map(_normalize_status)

    # Parse event_timestamp → pandas.Timestamp, then format ISO
    if "event_timestamp" in df.columns:
        dt = df["event_timestamp"].map(_parse_mixed_dt)
        df["event_timestamp"] = dt.dt.strftime("%Y-%m-%d %H:%M:%S")

    # Numeric types
    if "quantity" in df.columns:
        # Allow missing values with nullable Int64
        df["quantity"] = pd.to_numeric(df["quantity"], errors="coerce").astype("Int64")
    if "price_at_purchase" in df.columns:
        df["price_at_purchase"] = pd.to_numeric(df["price_at_purchase"], errors="coerce")

    # Sort (optional but handy for sanity checks)
    sort_cols = [c for c in ["order_id", "listing_id", "event_timestamp"] if c in df.columns]
    if sort_cols:
        df = df.sort_values(sort_cols, kind="stable")

    # Write output
    df.to_csv(output_file, index=False)
    #print(f"Wrote {len(df):,} rows → {output_file}")
    #return df

#lisiting.csv
def cleaning_listings(input_file="listings.csv", output_file="clean_listings.csv"):
    """
    Clean listings.csv:
      - Repair rows where listing_date was split by a comma (e.g., 'Mar 01, 2023')
      - Trim whitespace, strip stray quotes
      - Normalise empty-ish tokens to NaN
      - Uppercase category
      - Parse listing_date -> YYYY-MM-DD
      - Clean price ('$12.34' -> 12.34 float)
      - Cast stock_quantity -> Int64
      - Write clean_listings.csv
    """
    # 1) Read raw lines first (avoid parser errors from commas in dates)
    with open(input_file, "r", encoding="utf-8") as f:
        lines = [line.rstrip("\n") for line in f]
    if not lines:
        raise ValueError("File is empty")

    header = [h.strip() for h in lines[0].split(",")]
    n_cols = len(header)  # should be 7

    fixed_rows = []
    for line in lines[1:]:
        parts = [p.strip() for p in line.split(",")]

        if len(parts) == n_cols:
            fixed_rows.append(parts)

        elif len(parts) > n_cols:
            # Columns: 0:listing_id 1:product_name 2:category 3:seller_id 4:price 5:stock_quantity 6:listing_date
            # Anything beyond index 6 belongs to the date → merge them with spaces
            merged_date = " ".join(parts[6:])
            parts = parts[:6] + [merged_date]
            fixed_rows.append(parts)

        else:
            # Too few columns → pad
            parts += [""] * (n_cols - len(parts))
            fixed_rows.append(parts)

    df = pd.DataFrame(fixed_rows, columns=header)

    # 2) Strip whitespace in all string cells & stray quotes
    df = df.apply(lambda s: s.str.strip() if s.dtype == "object" else s)
    df = df.apply(lambda s: s.str.strip('"') if s.dtype == "object" else s)

    # 3) Normalise empties and “nan-like” tokens to real NaN
    df = df.replace(NAN_PAT, np.nan, regex=True)

    # 4) Fix/standardise fields
    # 4a) CATEGORY → uppercase (also remove stray inner spaces like " Fashion ")
    if "category" in df.columns:
        df["category"] = df["category"].astype("string").str.strip().str.upper()

    # 4b) PRICE → strip $ and spaces/commas → float
    if "price" in df.columns:
        df["price"] = (
            df["price"]
            .astype("string")
            .str.replace(r"[\$,]", "", regex=True)
            .str.strip()
        )
        df["price"] = pd.to_numeric(df["price"], errors="coerce")

    # 4c) STOCK_QUANTITY → Int64 (nullable)
    if "stock_quantity" in df.columns:
        df["stock_quantity"] = pd.to_numeric(df["stock_quantity"], errors="coerce").astype("Int64")

    # 4d) LISTING_DATE → YYYY-MM-DD using your mixed-date parser
    if "listing_date" in df.columns:
        parsed = df["listing_date"].map(_parse_mixed_date)
        df["listing_date"] = pd.to_datetime(parsed, errors="coerce").dt.strftime("%Y-%m-%d")

    # 5) Tidy other text fields
    if "product_name" in df.columns:
        df["product_name"] = df["product_name"].astype("string").str.strip()
    if "seller_id" in df.columns:
        df["seller_id"] = df["seller_id"].astype("string").str.strip()

    # 6) Write output
    df.to_csv(output_file, index=False)
    #print(f"Wrote {len(df):,} rows → {output_file}")
    #return df

if __name__ == "__main__":
    cleaning_customers()
    cleaning_orders()
    cleaning_listings()