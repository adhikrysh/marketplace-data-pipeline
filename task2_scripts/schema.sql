-- schema.sql
-- Minimal 3NF marketplace schema (customers, listings, orders, order_items, order_item_status_history)

CREATE DATABASE IF NOT EXISTS marketplace
  DEFAULT CHARACTER SET utf8mb4
  COLLATE utf8mb4_0900_ai_ci;
USE marketplace;

-- Keep MySQL strict so bad data doesn't sneak in
SET sql_mode = 'STRICT_ALL_TABLES';

-- 1) Customers
DROP TABLE IF EXISTS order_item_status_history;
DROP TABLE IF EXISTS order_items;
DROP TABLE IF EXISTS orders;
DROP TABLE IF EXISTS listings;
DROP TABLE IF EXISTS customers;

CREATE TABLE customers (
  customer_id  VARCHAR(16)  NOT NULL,
  first_name   VARCHAR(80)  NOT NULL,
  last_name    VARCHAR(80)  NOT NULL,
  email        VARCHAR(255) NULL,
  join_date    DATE         NOT NULL,
  city         VARCHAR(64)  NULL,
  CONSTRAINT pk_customers PRIMARY KEY (customer_id),
  KEY idx_customers_email (email)
) ENGINE=InnoDB;

-- 2) Listings (seller/category kept inline by requirement)
CREATE TABLE listings (
  listing_id      VARCHAR(16)    NOT NULL,
  product_name    VARCHAR(255)   NOT NULL,
  category        VARCHAR(64)    NOT NULL,
  seller_id       VARCHAR(64)    NOT NULL,
  price           DECIMAL(10,2)  NOT NULL,
  stock_quantity  INT            NOT NULL,
  listing_date    DATE           NOT NULL,
  CONSTRAINT pk_listings PRIMARY KEY (listing_id),
  KEY idx_listings_category (category),
  KEY idx_listings_seller (seller_id)
) ENGINE=InnoDB;

-- 3) Orders (header)
CREATE TABLE orders (
  order_id     INT          NOT NULL,
  customer_id  VARCHAR(16)  NOT NULL,
  CONSTRAINT pk_orders PRIMARY KEY (order_id),
  CONSTRAINT fk_orders_customer
    FOREIGN KEY (customer_id) REFERENCES customers(customer_id)
      ON UPDATE RESTRICT ON DELETE RESTRICT,
  KEY idx_orders_customer (customer_id)
) ENGINE=InnoDB;

-- 4) Order items (M:N join for Orders ↔ Listings) + relationship attributes
CREATE TABLE order_items (
  order_id          INT          NOT NULL,
  listing_id        VARCHAR(16)  NOT NULL,
  quantity          INT          NOT NULL,
  price_at_purchase DECIMAL(10,2) NOT NULL,
  CONSTRAINT pk_order_items PRIMARY KEY (order_id, listing_id),
  CONSTRAINT fk_items_order
    FOREIGN KEY (order_id) REFERENCES orders(order_id)
      ON UPDATE RESTRICT ON DELETE CASCADE,
  CONSTRAINT fk_items_listing
    FOREIGN KEY (listing_id) REFERENCES listings(listing_id)
      ON UPDATE RESTRICT ON DELETE RESTRICT,
  KEY idx_items_listing (listing_id)
) ENGINE=InnoDB;

-- 5) Item-level status history (event log)
CREATE TABLE order_item_status_history (
  order_id         INT          NOT NULL,
  listing_id       VARCHAR(16)  NOT NULL,
  status_timestamp DATETIME     NOT NULL,
  order_status     VARCHAR(32)  NOT NULL,
  CONSTRAINT pk_item_status PRIMARY KEY (order_id, listing_id, status_timestamp),
  CONSTRAINT fk_hist_item
    FOREIGN KEY (order_id, listing_id) REFERENCES order_items(order_id, listing_id)
      ON UPDATE RESTRICT ON DELETE CASCADE,
  KEY idx_hist_status (order_status),
  KEY idx_hist_time (status_timestamp)
) ENGINE=InnoDB;
