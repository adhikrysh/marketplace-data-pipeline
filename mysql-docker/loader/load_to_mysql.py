#!/usr/bin/env python3
import os, sys, time
import mysql.connector
import pandas as pd

DB_HOST = os.getenv("DB_HOST", "db")
DB_PORT = int(os.getenv("DB_PORT", "3306"))
DB_USER = os.getenv("DB_USER", "user")
DB_PASS = os.getenv("DB_PASS", "password")
DB_NAME = os.getenv("DB_NAME", "marketplace")
DATA_DIR = os.getenv("DATA_DIR", "/data")

PATH_CUSTOMERS = os.path.join(DATA_DIR, "clean_customers.csv")
PATH_LISTINGS  = os.path.join(DATA_DIR, "clean_listings.csv")
PATH_ORDERS    = os.path.join(DATA_DIR, "clean_orders.csv")

BATCH = 1000
RETRY = 60

def connect():
    last = None
    for _ in range(RETRY):
        try:
            return mysql.connector.connect(
                host=DB_HOST, port=DB_PORT,
                user=DB_USER, password=DB_PASS,
                database=DB_NAME, autocommit=False,
            )
        except mysql.connector.Error as e:
            last = e
            time.sleep(2)
    raise last

def chunked(rows, n=BATCH):
    buf = []
    for r in rows:
        buf.append(r)
        if len(buf) >= n:
            yield buf
            buf = []
    if buf:
        yield buf

def load_customers(cur, df):
    sql = """
    INSERT INTO customers (customer_id, first_name, last_name, email, join_date, city)
    VALUES (%(customer_id)s, %(first_name)s, %(last_name)s, %(email)s, %(join_date)s, %(city)s)
    ON DUPLICATE KEY UPDATE
      first_name=VALUES(first_name),
      last_name=VALUES(last_name),
      email=VALUES(email),
      join_date=VALUES(join_date),
      city=VALUES(city)
    """
    rows = df.to_dict(orient="records")
    for batch in chunked(rows):
        cur.executemany(sql, batch)

def load_listings(cur, df):
    # Cast numerics
    df = df.copy()
    df["price"] = pd.to_numeric(df["price"], errors="coerce")
    df["stock_quantity"] = pd.to_numeric(df["stock_quantity"], errors="coerce").astype("Int64")

    sql = """
    INSERT INTO listings (listing_id, product_name, category, seller_id, price, stock_quantity, listing_date)
    VALUES (%(listing_id)s, %(product_name)s, %(category)s, %(seller_id)s, %(price)s, %(stock_quantity)s, %(listing_date)s)
    ON DUPLICATE KEY UPDATE
      product_name=VALUES(product_name),
      category=VALUES(category),
      seller_id=VALUES(seller_id),
      price=VALUES(price),
      stock_quantity=VALUES(stock_quantity),
      listing_date=VALUES(listing_date)
    """
    rows = df.to_dict(orient="records")
    for batch in chunked(rows):
        cur.executemany(sql, batch)

def transform_orders(df_orders):
    df = df_orders.copy()

    # Ensure proper types
    df["quantity"] = pd.to_numeric(df["quantity"], errors="coerce").fillna(0).astype(int)
    df["price_at_purchase"] = pd.to_numeric(df["price_at_purchase"], errors="coerce")
    df["event_timestamp"] = pd.to_datetime(df["event_timestamp"], errors="coerce")

    # ORDERS: one row per (order_id, customer_id)
    df_orders_tbl = df[["order_id", "customer_id"]].drop_duplicates().sort_values(["order_id"])

    # ORDER_ITEMS: latest event per (order_id, listing_id)
    df_sorted = df.sort_values(["order_id", "listing_id", "event_timestamp"])
    df_items_tbl = (
        df_sorted.drop_duplicates(["order_id", "listing_id"], keep="last")
                [["order_id", "listing_id", "quantity", "price_at_purchase"]]
                .copy()
    )   

    # STATUS HISTORY: every event
    df_hist_tbl = df.rename(columns={"event_timestamp": "status_timestamp"})[
        ["order_id", "listing_id", "status_timestamp", "order_status"]
    ].copy()

    return df_orders_tbl, df_items_tbl, df_hist_tbl

def load_orders(cur, df):
    sql = """
    INSERT INTO orders (order_id, customer_id)
    VALUES (%(order_id)s, %(customer_id)s)
    ON DUPLICATE KEY UPDATE
      customer_id=VALUES(customer_id)
    """
    rows = df.to_dict(orient="records")
    for batch in chunked(rows):
        cur.executemany(sql, batch)

def load_order_items(cur, df):
    sql = """
    INSERT INTO order_items (order_id, listing_id, quantity, price_at_purchase)
    VALUES (%(order_id)s, %(listing_id)s, %(quantity)s, %(price_at_purchase)s)
    ON DUPLICATE KEY UPDATE
      quantity=VALUES(quantity),
      price_at_purchase=VALUES(price_at_purchase)
    """
    rows = df.to_dict(orient="records")
    for batch in chunked(rows):
        cur.executemany(sql, batch)

def load_status_history(cur, df):
    sql = """
    INSERT INTO order_item_status_history (order_id, listing_id, status_timestamp, order_status)
    VALUES (%(order_id)s, %(listing_id)s, %(status_timestamp)s, %(order_status)s)
    ON DUPLICATE KEY UPDATE
      order_status=VALUES(order_status)  -- if re-run with same ts, update status text
    """
    rows = df.to_dict(orient="records")
    for batch in chunked(rows):
        cur.executemany(sql, batch)

def main():
    print(f"Connecting to mysql://{DB_USER}@{DB_HOST}:{DB_PORT}/{DB_NAME} …")
    cnx = connect()
    try:
        cur = cnx.cursor()

        # 1) Customers
        if os.path.exists(PATH_CUSTOMERS):
            df_c = pd.read_csv(PATH_CUSTOMERS, dtype=str, keep_default_na=False)
            load_customers(cur, df_c)
            print(f"[OK] customers: upserted {len(df_c)}")

        # 2) Listings
        if os.path.exists(PATH_LISTINGS):
            df_l = pd.read_csv(PATH_LISTINGS, dtype=str, keep_default_na=False)
            load_listings(cur, df_l)
            print(f"[OK] listings: upserted {len(df_l)}")

        # 3) Orders + Items + History (derived)
        if os.path.exists(PATH_ORDERS):
            df_o = pd.read_csv(PATH_ORDERS, dtype=str)
            df_orders_tbl, df_items_tbl, df_hist_tbl = transform_orders(df_o)

            load_orders(cur, df_orders_tbl)
            print(f"[OK] orders: upserted {len(df_orders_tbl)}")

            load_order_items(cur, df_items_tbl)
            print(f"[OK] order_items: upserted {len(df_items_tbl)}")

            load_status_history(cur, df_hist_tbl)
            print(f"[OK] order_item_status_history: upserted {len(df_hist_tbl)}")

        cnx.commit()
        print("[DONE] All committed.")
    except Exception as e:
        cnx.rollback()
        print(f"[ERROR] Rolled back: {e}", file=sys.stderr)
        sys.exit(1)
    finally:
        try:
            cur.close()
        except Exception:
            pass
        cnx.close()

if __name__ == "__main__":
    main()
