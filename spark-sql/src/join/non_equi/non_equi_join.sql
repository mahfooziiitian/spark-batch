-- Non-equi join examples: range / theta joins using inequality conditions.
-- Use cases: pricing tier lookup, transaction-to-date-interval assignment,
-- BETWEEN join condition, and overlapping date-range detection.
-- Tables: transactions(txn_id, customer_id, amount, txn_date),
--         pricing_tiers(tier, min_amount, max_amount, discount_pct),
--         promotions(promo_id, start_date, end_date, promo_label),
--         bookings(booking_id, room_id, check_in, check_out)

-- -----------------------------------------------------------------------
-- Test data
-- -----------------------------------------------------------------------

CREATE OR REPLACE TEMP VIEW txns AS
SELECT
    txn_id,
    customer_id,
    amount,
    txn_date
FROM
    VALUES
    (1, 101, 45.00, DATE '2024-01-15'),
    (2, 102, 150.00, DATE '2024-02-10'),
    (3, 103, 320.00, DATE '2024-03-05'),
    (4, 104, 750.00, DATE '2024-04-20'),
    (5, 105, 1200.00, DATE '2024-05-01'),
    (6, 101, 99.00, DATE '2024-06-18')
        AS txns (txn_id, customer_id, amount, txn_date);

CREATE OR REPLACE TEMP VIEW pricing_tiers AS
SELECT
    tier,
    min_amount,
    max_amount,
    discount_pct
FROM
    VALUES
    ('Bronze', 0.00, 99.99, 0.0),
    ('Silver', 100.00, 499.99, 5.0),
    ('Gold', 500.00, 999.99, 10.0),
    ('Platinum', 1000.00, 9999.99, 15.0)
        AS pricing_tiers (tier, min_amount, max_amount, discount_pct);

CREATE OR REPLACE TEMP VIEW promotions AS
SELECT
    promo_id,
    start_date,
    end_date,
    promo_label
FROM
    VALUES
    (1, DATE '2024-01-01', DATE '2024-02-28', 'Winter Sale'),
    (2, DATE '2024-03-01', DATE '2024-04-30', 'Spring Deals'),
    (3, DATE '2024-05-01', DATE '2024-06-30', 'Summer Kickoff')
        AS promotions (promo_id, start_date, end_date, promo_label);

CREATE OR REPLACE TEMP VIEW bookings AS
SELECT
    booking_id,
    room_id,
    check_in,
    check_out
FROM
    VALUES
    (1, 'R01', DATE '2024-03-01', DATE '2024-03-05'),
    (2, 'R01', DATE '2024-03-04', DATE '2024-03-08'),  -- overlaps booking 1
    (3, 'R01', DATE '2024-03-10', DATE '2024-03-12'),
    (4, 'R02', DATE '2024-03-01', DATE '2024-03-03'),
    (5, 'R02', DATE '2024-03-05', DATE '2024-03-09')
        AS bookings (booking_id, room_id, check_in, check_out);

-- -----------------------------------------------------------------------
-- 1. Pricing tier lookup — assign a tier based on transaction amount
-- -----------------------------------------------------------------------

-- The join condition uses BETWEEN (a >= min_amount AND a <= max_amount).
-- Each transaction matches exactly one tier because the ranges are exclusive.
SELECT
    t.txn_id,
    t.customer_id,
    t.amount,
    pt.tier,
    pt.discount_pct,
    ROUND(t.amount * (1 - pt.discount_pct / 100), 2) AS discounted_amount
FROM txns AS t
INNER JOIN pricing_tiers AS pt
    ON t.amount BETWEEN pt.min_amount AND pt.max_amount
ORDER BY t.txn_id;
-- Result:
--   txn 1 (45.00)   → Bronze  / 0%  discount
--   txn 2 (150.00)  → Silver  / 5%  discount
--   txn 3 (320.00)  → Silver  / 5%  discount
--   txn 4 (750.00)  → Gold    / 10% discount
--   txn 5 (1200.00) → Platinum/ 15% discount
--   txn 6 (99.00)   → Bronze  / 0%  discount

-- -----------------------------------------------------------------------
-- 2. Assign each transaction to the active promotion on its date
-- -----------------------------------------------------------------------

-- Range condition: txn_date falls within the promotion's
-- [start_date, end_date].
-- LEFT JOIN preserves transactions outside all promotion windows.
SELECT
    t.txn_id,
    t.txn_date,
    t.amount,
    COALESCE(p.promo_label, 'No Promotion') AS promo_label
FROM txns AS t
LEFT JOIN promotions AS p
    ON t.txn_date BETWEEN p.start_date AND p.end_date
ORDER BY t.txn_id;
-- Result: every transaction shows the promotion label active on its date

-- -----------------------------------------------------------------------
-- 3. Explicit inequality condition — transactions above a threshold per tier
-- -----------------------------------------------------------------------

-- Show every (transaction, tier) pair where the transaction amount is strictly
-- greater than the tier's minimum — demonstrating a theta join with >.
SELECT
    t.txn_id,
    t.amount,
    pt.tier,
    pt.min_amount
FROM txns AS t
INNER JOIN pricing_tiers AS pt
    ON t.amount > pt.min_amount
ORDER BY
    t.txn_id,
    pt.min_amount;
-- Result: multiple rows per transaction (one per tier whose floor is exceeded)

-- -----------------------------------------------------------------------
-- 4. Self non-equi join — detect overlapping bookings for the same room
-- -----------------------------------------------------------------------

-- Two bookings on the same room overlap when one check-in falls before the
-- other's check-out AND one check-out falls after the other's check-in.
-- b1.booking_id < b2.booking_id avoids duplicate pairs.
SELECT
    b1.room_id,
    b1.booking_id AS booking_a,
    b1.check_in AS a_check_in,
    b1.check_out AS a_check_out,
    b2.booking_id AS booking_b,
    b2.check_in AS b_check_in,
    b2.check_out AS b_check_out
FROM bookings AS b1
INNER JOIN bookings AS b2
    ON
        b1.room_id = b2.room_id           -- same room (equi condition)
        -- avoid self-match and duplicates
        AND b1.booking_id < b2.booking_id
        AND b1.check_in < b2.check_out         -- overlap condition
        AND b1.check_out > b2.check_in;         -- overlap condition
-- Result: room R01 — bookings 1 and 2 overlap (Mar 4–5 is shared)

-- -----------------------------------------------------------------------
-- 5. Range join with upper-bound exclusion (half-open interval)
-- -----------------------------------------------------------------------

-- Use > and < instead of BETWEEN when the interval is half-open
-- (start inclusive, end exclusive) — common for partitioned time windows.
SELECT
    t.txn_id,
    t.txn_date,
    p.promo_id,
    p.promo_label
FROM txns AS t
INNER JOIN promotions AS p
    ON
        t.txn_date >= p.start_date
        AND t.txn_date < p.end_date   -- end_date is exclusive
ORDER BY t.txn_id;
