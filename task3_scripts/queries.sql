-- Query 1 — Top X sellers by revenue per category (DELIVERED items only)

-- set how many per category
SET @X := 1;

WITH delivered_items AS (
  SELECT DISTINCT ois.order_id, ois.listing_id
  FROM order_item_status_history AS ois
  WHERE ois.order_status = 'DELIVERED'
),
seller_category_revenue AS (
  SELECT
      l.category,
      l.seller_id,                -- "influencer" (seller)
      SUM(oi.quantity * oi.price_at_purchase) AS revenue
  FROM order_items AS oi
  JOIN delivered_items d
    ON d.order_id = oi.order_id AND d.listing_id = oi.listing_id
  JOIN listings AS l
    ON l.listing_id = oi.listing_id
  GROUP BY l.category, l.seller_id
),
ranked AS (
  SELECT
      category,
      seller_id,
      revenue,
      ROW_NUMBER() OVER (PARTITION BY category ORDER BY revenue DESC) AS rn
  FROM seller_category_revenue
)
SELECT category, seller_id, revenue
FROM ranked
WHERE rn <= @X
ORDER BY category, revenue DESC;

-- Query 2 — Avg time from PLACED → SHIPPED (per item, then averaged) // item level
WITH placed AS (
  SELECT
      order_id,
      listing_id,
      MIN(status_timestamp) AS t_placed
  FROM order_item_status_history
  WHERE order_status = 'PLACED'
  GROUP BY order_id, listing_id
),
shipped AS (
  SELECT
      order_id,
      listing_id,
      MIN(status_timestamp) AS t_shipped
  FROM order_item_status_history
  WHERE order_status = 'SHIPPED'
  GROUP BY order_id, listing_id
),
paired AS (
  SELECT
      p.order_id,
      p.listing_id,
      p.t_placed,
      s.t_shipped
  FROM placed p
  JOIN shipped s
    ON s.order_id = p.order_id
   AND s.listing_id = p.listing_id
  WHERE s.t_shipped >= p.t_placed   -- guard against out-of-order data
)
SELECT
  AVG(TIMESTAMPDIFF(HOUR, t_placed, t_shipped))    AS avg_hours,
  AVG(TIMESTAMPDIFF(MINUTE, t_placed, t_shipped))  AS avg_minutes,
  COUNT(*)                                         AS items_count
FROM paired;

-- Query 3 Month-over-Month (MoM) growth of DELIVERED-only revenue
WITH delivered_at AS (
  SELECT
      ois.order_id,
      ois.listing_id,
      MIN(ois.status_timestamp) AS delivered_ts
  FROM order_item_status_history ois
  WHERE ois.order_status = 'DELIVERED'
  GROUP BY ois.order_id, ois.listing_id
),
recognized AS (
  SELECT
      DATE_FORMAT(d.delivered_ts, '%Y-%m-01') AS month_start,
      oi.quantity * oi.price_at_purchase      AS revenue
  FROM delivered_at d
  JOIN order_items oi
    ON oi.order_id  = d.order_id
   AND oi.listing_id = d.listing_id
),
monthly AS (
  SELECT
      month_start,
      SUM(revenue) AS monthly_revenue
  FROM recognized
  GROUP BY month_start
)
SELECT
  month_start,
  monthly_revenue,
  LAG(monthly_revenue) OVER (ORDER BY month_start) AS prev_revenue,
  CASE
    WHEN LAG(monthly_revenue) OVER (ORDER BY month_start) IS NULL
      THEN NULL
    WHEN LAG(monthly_revenue) OVER (ORDER BY month_start) = 0
      THEN NULL
    ELSE (monthly_revenue - LAG(monthly_revenue) OVER (ORDER BY month_start))
         / LAG(monthly_revenue) OVER (ORDER BY month_start)
  END AS mom_growth_ratio
FROM monthly
ORDER BY month_start;


-- Query 4 -> performance analysis
-- Accelerate filtering by status and grouping by order/item; speeds MIN(timestamp)
CREATE INDEX idx_oish_status_order_listing_ts
  ON order_item_status_history (order_status, order_id, listing_id, status_timestamp);

-- Alternate access path when joining by order/listing first, then filtering by status
CREATE INDEX idx_oish_order_listing_status_ts
  ON order_item_status_history (order_id, listing_id, order_status, status_timestamp);

-- Ensure fast joins from items to history (if not already PK/unique)
CREATE INDEX idx_oi_order_listing
  ON order_items (order_id, listing_id);

-- For category/seller aggregations
CREATE INDEX idx_listings_category_seller
  ON listings (category, seller_id);

-- Optional: if you often filter by delivered month ranges
CREATE INDEX idx_oish_status_ts
  ON order_item_status_history (order_status, status_timestamp);


/* How to verify (and avoid full table scans)

Use EXPLAIN ANALYZE (MySQL 8.0.18+) to see actual rows read and chosen indexes: */

EXPLAIN ANALYZE
WITH placed AS ( ... )  -- paste the query for #2 or #3 here
SELECT ...;

/*Look for rows and filtered estimates; ensure the plan shows usage of your new idx_* indexes.
If you still see large “rows examined,” consider:
tightening predicates (e.g., limit to a date range),
adding or reordering composite indexes to put the most selective column first,
ensuring datatypes and collations match across joined columns.*/

-- natural language to SQL python function
-- it's in nl2sql.py!

