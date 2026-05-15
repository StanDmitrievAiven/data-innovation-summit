-- =============================================================================
-- Source database: ecommerce (created by POSTGRES_DB env var)
-- =============================================================================

ALTER SYSTEM SET wal_level = 'logical';

CREATE TABLE IF NOT EXISTS users (
    user_id       SERIAL PRIMARY KEY,
    first_name    VARCHAR(50) NOT NULL,
    last_name     VARCHAR(50) NOT NULL,
    email         VARCHAR(150) NOT NULL UNIQUE,
    phone         VARCHAR(20),
    country       VARCHAR(60) NOT NULL,
    is_active     BOOLEAN DEFAULT true,
    created_at    TIMESTAMP NOT NULL DEFAULT NOW(),
    updated_at    TIMESTAMP NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS products (
    product_id    SERIAL PRIMARY KEY,
    sku           VARCHAR(30) NOT NULL UNIQUE,
    name          VARCHAR(200) NOT NULL,
    category      VARCHAR(80) NOT NULL,
    brand         VARCHAR(80),
    unit_price    DECIMAL(12,2) NOT NULL,
    currency      VARCHAR(3) NOT NULL DEFAULT 'EUR',
    in_stock      BOOLEAN DEFAULT true,
    created_at    TIMESTAMP NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS orders (
    order_id          SERIAL PRIMARY KEY,
    user_id           INT NOT NULL REFERENCES users(user_id),
    total_amount      DECIMAL(12,2) NOT NULL,
    currency          VARCHAR(3) NOT NULL DEFAULT 'EUR',
    status            VARCHAR(20) NOT NULL DEFAULT 'pending',
    shipping_country  VARCHAR(60),
    created_at        TIMESTAMP NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS order_items (
    item_id       SERIAL PRIMARY KEY,
    order_id      INT NOT NULL REFERENCES orders(order_id),
    product_id    INT NOT NULL REFERENCES products(product_id),
    quantity      INT NOT NULL,
    unit_price    DECIMAL(12,2) NOT NULL,
    discount      DECIMAL(5,2) DEFAULT 0.00
);

CREATE TABLE IF NOT EXISTS shipments (
    shipment_id     SERIAL PRIMARY KEY,
    order_id        INT NOT NULL REFERENCES orders(order_id),
    carrier         VARCHAR(50) NOT NULL,
    tracking_number VARCHAR(80),
    status          VARCHAR(20) NOT NULL DEFAULT 'processing',
    shipped_at      TIMESTAMP,
    delivered_at    TIMESTAMP
);

-- Seed data
INSERT INTO users (user_id, first_name, last_name, email, phone, country, is_active, created_at) VALUES
    (1,  'Emma',    'Virtanen',   'emma.virtanen@mail.fi',    '+358401234567',  'Finland',        true,  '2024-01-15 09:30:00'),
    (2,  'Lukas',   'Mueller',    'lukas.mueller@mail.de',    '+4915112345678', 'Germany',        true,  '2024-02-20 14:15:00'),
    (3,  'Sofia',   'Andersson',  'sofia.andersson@mail.se',  '+46701234567',   'Sweden',         true,  '2024-03-10 11:00:00'),
    (4,  'James',   'Wilson',     'james.wilson@mail.co.uk',  '+447911123456',  'United Kingdom', true,  '2024-04-05 16:45:00'),
    (5,  'Maria',   'Garcia',     'maria.garcia@mail.es',     '+34612345678',   'Spain',          true,  '2024-05-12 10:20:00'),
    (6,  'Antoine', 'Dupont',     'antoine.dupont@mail.fr',   '+33612345678',   'France',         true,  '2024-06-01 08:00:00'),
    (7,  'Aino',    'Korhonen',   'aino.korhonen@mail.fi',    '+358501234567',  'Finland',        true,  '2024-07-22 13:30:00'),
    (8,  'Max',     'Schmidt',    'max.schmidt@mail.de',      '+4917612345678', 'Germany',        false, '2024-08-15 09:00:00'),
    (9,  'Elena',   'Rossi',      'elena.rossi@mail.it',      '+393331234567',  'Italy',          true,  '2024-09-03 17:15:00'),
    (10, 'Oliver',  'Johansson',  'oliver.johansson@mail.se', '+46761234567',   'Sweden',         true,  '2024-10-18 12:00:00');

INSERT INTO products (product_id, sku, name, category, brand, unit_price, currency, in_stock) VALUES
    (1, 'ELEC-LAPTOP-001', 'ProBook 450 G10',        'Electronics', 'HP',         899.99, 'EUR', true),
    (2, 'ELEC-PHONE-001',  'Galaxy S24 Ultra',        'Electronics', 'Samsung',   1299.99, 'EUR', true),
    (3, 'ELEC-TABLET-001', 'iPad Air M2',             'Electronics', 'Apple',      749.00, 'EUR', true),
    (4, 'WEAR-JACKET-001', 'Arctic Expedition Parka', 'Outerwear',   'Fjallraven', 459.95, 'EUR', true),
    (5, 'WEAR-SHOES-001',  'Ultraboost Light',        'Footwear',    'Adidas',     189.99, 'EUR', true),
    (6, 'HOME-CHAIR-001',  'Markus Office Chair',     'Furniture',   'IKEA',       229.00, 'EUR', true),
    (7, 'ELEC-WATCH-001',  'Fenix 7 Pro',             'Electronics', 'Garmin',     799.99, 'EUR', false),
    (8, 'HOME-LAMP-001',   'Hue Gradient Lightstrip', 'Lighting',    'Philips',     84.99, 'EUR', true);

INSERT INTO orders (order_id, user_id, total_amount, currency, status, shipping_country, created_at) VALUES
    (1,  1,  899.99, 'EUR', 'delivered', 'Finland',        '2024-11-01 10:00:00'),
    (2,  2, 1489.98, 'EUR', 'delivered', 'Germany',        '2024-11-05 14:30:00'),
    (3,  3,  749.00, 'EUR', 'shipped',   'Sweden',         '2024-11-10 09:15:00'),
    (4,  4,  649.94, 'EUR', 'shipped',   'United Kingdom', '2024-11-15 16:00:00'),
    (5,  5,  189.99, 'EUR', 'confirmed', 'Spain',          '2024-11-20 11:45:00'),
    (6,  1,  229.00, 'EUR', 'pending',   'Finland',        '2024-11-25 08:30:00'),
    (7,  6, 1299.99, 'EUR', 'delivered', 'France',         '2024-11-28 13:00:00'),
    (8,  7,  459.95, 'EUR', 'confirmed', 'Finland',        '2024-12-01 10:00:00'),
    (9,  9,   84.99, 'EUR', 'pending',   'Italy',          '2024-12-05 15:30:00'),
    (10, 3,  899.99, 'EUR', 'pending',   'Sweden',         '2024-12-08 09:00:00');

INSERT INTO order_items (item_id, order_id, product_id, quantity, unit_price, discount) VALUES
    (1,  1,  1, 1,  899.99, 0.00),
    (2,  2,  2, 1, 1299.99, 0.00),
    (3,  2,  5, 1,  189.99, 0.00),
    (4,  3,  3, 1,  749.00, 0.00),
    (5,  4,  4, 1,  459.95, 0.00),
    (6,  4,  5, 1,  189.99, 0.00),
    (7,  5,  5, 1,  189.99, 0.00),
    (8,  6,  6, 1,  229.00, 0.00),
    (9,  7,  2, 1, 1299.99, 0.00),
    (10, 8,  4, 1,  459.95, 0.00),
    (11, 9,  8, 1,   84.99, 0.00),
    (12, 10, 1, 1,  899.99, 0.00);

INSERT INTO shipments (shipment_id, order_id, carrier, tracking_number, status, shipped_at, delivered_at) VALUES
    (1, 1, 'Posti',      'FI202411010001', 'delivered',  '2024-11-02 08:00:00', '2024-11-04 14:00:00'),
    (2, 2, 'DHL',        'DE202411050001', 'delivered',  '2024-11-06 09:00:00', '2024-11-08 16:00:00'),
    (3, 3, 'PostNord',   'SE202411100001', 'in_transit', '2024-11-11 07:00:00', NULL),
    (4, 4, 'Royal Mail', 'GB202411150001', 'in_transit', '2024-11-16 10:00:00', NULL),
    (5, 7, 'Colissimo',  'FR202411280001', 'delivered',  '2024-11-29 08:00:00', '2024-12-01 11:00:00');

-- Reset sequences
SELECT setval('users_user_id_seq',         (SELECT MAX(user_id) FROM users));
SELECT setval('products_product_id_seq',   (SELECT MAX(product_id) FROM products));
SELECT setval('orders_order_id_seq',       (SELECT MAX(order_id) FROM orders));
SELECT setval('order_items_item_id_seq',   (SELECT MAX(item_id) FROM order_items));
SELECT setval('shipments_shipment_id_seq', (SELECT MAX(shipment_id) FROM shipments));

-- =============================================================================
-- Downstream databases (created empty, populated by sink connectors)
-- =============================================================================
CREATE DATABASE analytics_staging;
CREATE DATABASE search_index;
CREATE DATABASE warehouse;
