# MySQL + Loader + Mongo Snapshot (Docker)

This repository spins up:

* **MySQL 8.4** with your schema and a clean data load from CSVs.
* A one-shot **loader** container that upserts CSVs into MySQL tables.
* **MongoDB 7** plus an optional **mongo-express** UI.
* A one-shot **ETL** that snapshots orders from MySQL into Mongo (one document per order).

---

## Quick start (fresh install)

**Prereqs:** Docker Desktop (or Docker Engine) with Compose.

1. **Place the cleaned CSVs**

   ```
   mysql-docker/
     data/
       clean_customers.csv
       clean_listings.csv
       clean_orders.csv
   ```

2. **Start everything** (build images, initialize databases):

   ```bash
   docker compose up --build
   ```

   What happens:

   * MySQL starts and runs `initdb/schema.sql` (first boot only).
   * Healthcheck waits until MySQL is ready.
   * Loader runs once, reads CSVs from `./data`, and upserts into:
     `customers`, `listings`, `orders`, `order_items`, `order_item_status_history`.
   * Mongo starts (and `mongo-express` if enabled on port `8081`).

3. **Snapshot to Mongo (run on demand):**

   ```bash
   docker compose run --rm etl_mongo
   ```

   This reads from MySQL and creates one document per `order_id` in Mongo collection `ssg.orders`.

4. **Verify data (MySQL):**

   ```bash
   docker exec -it mysql-db mysql -u user -ppassword marketplace -e "
     SHOW TABLES;
     SELECT COUNT(*) customers FROM customers;
     SELECT COUNT(*) listings  FROM listings;
     SELECT COUNT(*) orders    FROM orders;
     SELECT COUNT(*) items     FROM order_items;
     SELECT COUNT(*) history   FROM order_item_status_history;"
   ```

5. **Verify documents (Mongo):**

   ```bash
   docker exec -it mongo mongosh --eval '
     use ssg;
     db.orders.countDocuments();
     db.orders.findOne();'
   ```

---

## Common commands

```bash
# Start + stream logs
docker compose up --build

# Start in background
docker compose up -d --build

# Tail logs for a service
docker compose logs -f loader
docker compose logs -f etl_mongo

# Re-run loader (idempotent)
docker compose run --rm loader

# Re-run ETL to Mongo (idempotent)
docker compose run --rm etl_mongo

# Reset to a clean state (wipe MySQL + Mongo data)
docker compose down -v
docker compose up --build
```

---

## File structure

```
mysql-docker/
├─ docker-compose.yml
├─ initdb/
│  └─ schema.sql
├─ data/
│  ├─ clean_customers.csv
│  ├─ clean_listings.csv
│  └─ clean_orders.csv
├─ loader/
│  ├─ Dockerfile
│  └─ load_to_mysql.py
└─ etl_mongo/
   ├─ Dockerfile
   ├─ requirements.txt
   └─ etl_mongo.py
```

**What each part does**

* `initdb/schema.sql` — Creates the `marketplace` schema and tables; strict SQL mode; runs only on first boot of a fresh `db_data` volume.
* `data/*.csv` — Cleaned input files. Headers must match the loader’s expected column names.
* `loader/*` — Python image that connects to MySQL and upserts CSV data in FK-safe order.
* `etl_mongo/*` — Python image that reads MySQL tables and writes one MongoDB document per order (denormalized read model).
* `docker-compose.yml` — Orchestrates services, ports, volumes, healthcheck, and dependencies.

---

## How it works (end-to-end)

### 1) MySQL service (`db`)

* **Image:** `mysql:8.4`, host port **3306**.
* **Initialization:** On first boot (empty `db_data` volume), MySQL’s entrypoint executes `initdb/schema.sql` to create:

  * `customers`
  * `listings`
  * `orders`
  * `order_items`
  * `order_item_status_history`
* **Healthcheck:** `mysqladmin ping` gate—dependent services wait until MySQL is healthy.
* **Persistence:** Data stored in named volume `db_data`.

### 2) Loader service (`loader`)

* **Build:** `loader/Dockerfile` (Python 3.12 + mysql-connector + pandas).
* **Run:** Starts after DB is healthy; runs `load_to_mysql.py` once and exits.
* **Data source:** Mounts `./data` read-only at `/data`.
* **Behavior:** Upserts with `INSERT ... ON DUPLICATE KEY UPDATE`. Idempotent.
* **Order of inserts:** `customers` → `listings` → `orders` → `order_items` → `order_item_status_history` (FK-safe).

### 3) MongoDB + UI

* **Mongo (`mongo`)**: `mongo:7`, host port **27017**, data in `mongo_data`.
* **mongo-express (`mongo-express`)**: optional admin UI at **[http://localhost:8081](http://localhost:8081)**.

### 4) ETL to Mongo (`etl_mongo`)

* **Build:** `etl_mongo/Dockerfile` with deps in `etl_mongo/requirements.txt`.
* **Run:** `docker compose run --rm etl_mongo`.
* **Logic:** Reads MySQL tables and assembles a **single document per order** with:

  * `order_id`, `customer`
  * `items[]` (embedded product snapshot + `price_at_purchase`, `quantity`, `extended_price`)
  * `status_timeline[]` (PLACED/SHIPPED/DELIVERED timestamps)
  * Denormalized fields (`current_status`, `placed_at`, `shipped_at`, `delivered_at`)
  * Precomputed metrics (`hours_to_ship`, `hours_to_deliver`)
* **Upsert:** Uses `_id = order_id` and `ReplaceOne(..., upsert=True)`. Safe to re-run.

---

## Data model (relational)

* `customers(customer_id PK, ...)`
* `listings(listing_id PK, product_name, category, seller_id, price, stock_quantity, listing_date, ...)`
* `orders(order_id PK, customer_id FK→customers)`
* `order_items((order_id, listing_id) PK, quantity, price_at_purchase, FK→orders, FK→listings)`
* `order_item_status_history((order_id, listing_id, status_timestamp) PK, order_status, FK→order_items)`
* Strict SQL mode prevents silent coercions/truncations.

---

## Data model (Mongo read model)

* **DB:** `ssg`
* **Collection:** `orders`
* **Doc per order:** everything the “Order Status” page needs in one fetch.

  ```js
  db.orders.findOne({ _id: "<order_id>" })
  ```
* **Suggested indexes (dev):**

  ```js
  db.orders.createIndex({ _id: 1 }, { unique: true })
  db.orders.createIndex({ "customer.customer_id": 1, delivered_at: -1 })
  db.orders.createIndex({ current_status: 1, placed_at: -1 })
  ```

---

## Configuration & ports

* **MySQL:** `user/password`, DB `marketplace`, port **3306** on host.
* **Mongo:** no auth in dev, port **27017** on host.
* **mongo-express:** **[http://localhost:8081](http://localhost:8081)** (if enabled).

---

## Typical workflows

**Fresh run (recreate databases):**

```bash
docker compose down -v
docker compose up --build
docker compose run --rm etl_mongo
```

**Reload CSVs after editing:**

```bash
docker compose run --rm loader
```

**Refresh Mongo snapshot:**

```bash
docker compose run --rm etl_mongo
```

---

## Troubleshooting

* **Loader runs too early:** ensure the DB healthcheck is present (it is by default).
* **Schema didn’t apply:** old `db_data` volume. Reset with `docker compose down -v`.
* **Port in use (3306/27017):** stop local MySQL/Mongo or change host port mappings.
* **mongo-express not loading:** check `docker compose logs -f mongo-express` and ensure `mongo` is running.

---

## Notes

* All loaders/ETL jobs are **idempotent**—safe to re-run.
* This setup is for **local development**. For production, enable Mongo auth, store secrets outside the repo, and add backups/monitoring.
