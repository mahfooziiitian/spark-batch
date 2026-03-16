-- Pattern matching examples in Spark SQL (Databricks).
-- Covers LIKE, NOT LIKE, ILIKE (Spark 3.3+), RLIKE/REGEXP, escape characters,
-- and combining patterns efficiently.

-- ----------------------------------------------------------------------------
-- Setup: contacts table with names, emails, and phone numbers
-- ----------------------------------------------------------------------------
CREATE OR REPLACE TEMP VIEW contacts AS
SELECT
    contact_id,
    full_name,
    email,
    phone
FROM
    VALUES
    (1, 'Alice Smith', 'alice.smith@example.com', '555-1234'),
    (2, 'Bob Johnson', 'BOB.JOHNSON@EXAMPLE.COM', '555-5678'),
    (3, 'Carol White', 'carol@mycompany.org', '444-0001'),
    (4, 'Dave Brown', 'dave_b@test.net', '123-0002'),
    (5, 'Eve Davis', 'eve.davis@example.com', '555-9999'),
    (6, 'Frank 100%', 'frank@example.com', '000-0000'),
    (7, 'Grace_Lee', 'grace@demo.io', '999-8888')
        AS t (contact_id, full_name, email, phone);

-- ----------------------------------------------------------------------------
-- 1. LIKE: basic wildcard patterns
-- ----------------------------------------------------------------------------
-- % matches zero or more characters; _ matches exactly one character.

-- Starts with 'Alice':
SELECT full_name FROM contacts
WHERE full_name LIKE 'Alice%';
-- Result: Alice Smith

-- Ends with 'son':
SELECT full_name FROM contacts
WHERE full_name LIKE '%son';
-- Result: Bob Johnson

-- Contains 'ith':
SELECT full_name FROM contacts
WHERE full_name LIKE '%ith%';
-- Result: Alice Smith

-- Exactly 8 characters (using _ placeholders):
SELECT full_name FROM contacts
WHERE full_name LIKE '________';
-- Result: Grace_Lee (9 chars — no match here; illustrative)

-- Email at example.com domain:
SELECT
    full_name,
    email
FROM contacts
WHERE email LIKE '%@example.com';
-- Result: Alice Smith, Eve Davis, Frank 100%
-- Note: LIKE is case-sensitive (Bob's uppercase email is excluded).

-- ----------------------------------------------------------------------------
-- 2. NOT LIKE
-- ----------------------------------------------------------------------------
SELECT
    full_name,
    email
FROM contacts
WHERE email NOT LIKE '%@example.com';
-- Result: Bob (uppercase domain), Carol, Dave, Grace

-- ----------------------------------------------------------------------------
-- 3. ILIKE: case-insensitive LIKE (Spark 3.3+)
-- ----------------------------------------------------------------------------
-- Matches regardless of letter case.
SELECT
    full_name,
    email
FROM contacts
WHERE email ILIKE '%@example.com';
-- Result: Alice, Bob (BOB.JOHNSON@EXAMPLE.COM matches), Eve, Frank

SELECT full_name
FROM contacts
WHERE full_name ILIKE 'alice%';
-- Result: Alice Smith

-- ----------------------------------------------------------------------------
-- 4. RLIKE / REGEXP: regular expression matching
-- ----------------------------------------------------------------------------
-- Digits only in phone prefix (3 digits before the hyphen):
SELECT
    full_name,
    phone
FROM contacts
WHERE phone RLIKE '^[0-9]{3}-[0-9]{4}$';
-- Result: all rows (all match NNN-NNNN format)

-- Email format validation (simplified): local@domain.tld
SELECT
    full_name,
    email
FROM contacts
WHERE email RLIKE '^[a-zA-Z0-9._%+\\-]+@[a-zA-Z0-9.\\-]+\\.[a-zA-Z]{2,}$';
-- Result: all rows with valid-looking emails

-- Starts with a capital letter:
SELECT full_name
FROM contacts
WHERE full_name RLIKE '^[A-Z]';
-- Result: Alice, Bob, Carol, Dave, Eve, Frank, Grace (all start uppercase)

-- Contains a digit anywhere in the name:
SELECT full_name
FROM contacts
WHERE full_name RLIKE '[0-9]';
-- Result: Frank 100%

-- Phone numbers starting with 555:
SELECT
    full_name,
    phone
FROM contacts
WHERE phone RLIKE '^555';
-- Result: Alice, Bob, Eve

-- ----------------------------------------------------------------------------
-- 5. LIKE with escape character
-- ----------------------------------------------------------------------------
-- To match a literal % or _, use ESCAPE to define an escape character.
SELECT full_name
FROM contacts
-- matches names containing literal '100%'
WHERE full_name LIKE '%100!%%' ESCAPE '!';
-- Result: Frank 100%

SELECT full_name
FROM contacts
-- matches names containing literal '_'
WHERE full_name LIKE '%!_%' ESCAPE '!';
-- Result: Grace_Lee

-- ----------------------------------------------------------------------------
-- 6. Multiple LIKE with OR vs single RLIKE alternative
-- ----------------------------------------------------------------------------
-- Multiple LIKE (verbose but straightforward):
SELECT
    full_name,
    email
FROM contacts
WHERE
    email LIKE '%.com'
    OR email LIKE '%.org'
    OR email LIKE '%.net';
-- Result: contacts with .com, .org, or .net emails

-- Equivalent single RLIKE (concise):
SELECT
    full_name,
    email
FROM contacts
WHERE email RLIKE '\\.(com|org|net)$';
-- Result: same set — Alice, Bob, Carol, Dave, Eve, Frank, Grace
