import os
import pandas as pd
import mysql.connector
from pymongo import MongoClient, ReplaceOne

# --- Config from env ---
MYSQL_HOST = os.getenv("MYSQL_HOST", "db")
MYSQL_PORT = int(os.getenv("MYSQL_PORT", "3306"))
MYSQL_USER = os.getenv("MYSQL_USER", "user")
MYSQL_PASS = os.getenv("MYSQL_PASS", "password")
MYSQL_DB   = os.getenv("MYSQL_DB", "marketplace")

MONGO_URI  = os.getenv("MONGO_URI", "mongodb://mongo:27017")
MONGO_DB   = os.getenv("MONGO_DB", "ssg")
MONGO_COLL = os.getenv("MONGO_COLL", "orders")

def get_mysql():
    return mysql.connector.connect(
        host=MYSQL_HOST, port=MYSQL_PORT, user=MYSQL_USER, password=MYSQL_PASS,
        database=MYSQL_DB, autocommit=False
    )

def get_mongo_collection():
    return MongoClient(MONGO_URI)[MONGO_DB][MONGO_COLL]

def fetch_data():
    cn = get_mysql()
    try:
        orders = pd.read_sql("SELECT order_id, customer_id FROM orders", cn)
        items = pd.read_sql("""
            SELECT order_id, listing_id, quantity, price_at_purchase
            FROM order_items
        """, cn)
        listings = pd.read_sql("""
            SELECT listing_id, product_name, category, seller_id
            FROM listings
        """, cn)
        history = pd.read_sql("""
            SELECT order_id, listing_id, status_timestamp, order_status
            FROM order_item_status_history
        """, cn)
        history["status_timestamp"] = pd.to_datetime(history["status_timestamp"], errors="coerce", utc=True)
        return orders, items, listings, history
    finally:
        cn.close()

def build_order_doc(order_id, df_orders, df_items, df_listings, df_hist):
    orow = df_orders.loc[df_orders["order_id"] == order_id].iloc[0]

    its = df_items[df_items["order_id"] == order_id].merge(df_listings, on="listing_id", how="left")
    items_arr = []
    for _, r in its.iterrows():
        qty  = int(r["quantity"] or 0)
        price = float(r["price_at_purchase"] or 0.0)
        items_arr.append({
            "listing_id": str(r["listing_id"]),
            "quantity": qty,
            "extended_price": qty * price,
            "product": {
                "name": r.get("product_name"),
                "category": r.get("category"),
                "seller_id": r.get("seller_id"),
                "price_at_purchase": price
            }
        })

    h = df_hist[df_hist["order_id"] == order_id]

    def first_at(status):
        x = h.loc[h["order_status"] == status, "status_timestamp"]
        if x.empty:
            return None
        t = x.min()
        return t.isoformat() if pd.notna(t) else None

    placed_at    = first_at("PLACED")
    shipped_at   = first_at("SHIPPED")
    delivered_at = first_at("DELIVERED")

    current_status = None
    if not h.empty:
        hx = h.sort_values("status_timestamp")
        current_status = hx.iloc[-1]["order_status"]

    subtotal = sum(i["extended_price"] for i in items_arr)

    def hours(a, b):
        if not a or not b: return None
        ta, tb = pd.to_datetime(a), pd.to_datetime(b)
        return float((tb - ta).total_seconds() / 3600.0)

    status_timeline = []
    if placed_at:    status_timeline.append({"status": "PLACED", "at": placed_at})
    if shipped_at:   status_timeline.append({"status": "SHIPPED", "at": shipped_at})
    if delivered_at: status_timeline.append({"status": "DELIVERED", "at": delivered_at})

    return {
        "_id": str(order_id),
        "order_id": str(order_id),
        "customer": { "customer_id": str(orow["customer_id"]) },
        "items": items_arr,
        "status_timeline": status_timeline,
        "current_status": current_status,
        "placed_at": placed_at,
        "shipped_at": shipped_at,
        "delivered_at": delivered_at,
        "payment": {
            "currency": "SGD",
            "subtotal": float(subtotal),
            "total": float(subtotal)
        },
        "metrics": {
            "hours_to_ship": hours(placed_at, shipped_at),
            "hours_to_deliver": hours(placed_at, delivered_at)
        },
        "audit": {
            "source": "mysql-snapshot",
            "snapshot_ts": pd.Timestamp.utcnow().isoformat()
        }
    }

def main():
    orders, items, listings, history = fetch_data()
    coll = get_mongo_collection()
    ops = []
    for oid in orders["order_id"].unique():
        doc = build_order_doc(oid, orders, items, listings, history)
        ops.append(ReplaceOne({"_id": doc["_id"]}, doc, upsert=True))
    if ops:
        res = coll.bulk_write(ops, ordered=False)
        print(f"[Mongo] upserted: {res.upserted_count}, modified: {res.modified_count}, matched: {res.matched_count}")
    else:
        print("[Mongo] no orders to write")

if __name__ == "__main__":
    main()
