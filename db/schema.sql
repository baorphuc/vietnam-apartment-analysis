-- ============================================
-- Vietnam Apartment Price Analysis
-- Schema Migration Script
-- ============================================

-- Drop if re-running
DROP TABLE IF EXISTS price_history;
DROP TABLE IF EXISTS apartments;
DROP TABLE IF EXISTS district_mapping;
DROP TABLE IF EXISTS scrape_logs;

-- ============================================
-- 1. District name normalization table
-- Maps raw scraped names → canonical name
-- ============================================
CREATE TABLE district_mapping (
    id              SERIAL PRIMARY KEY,
    raw_name        VARCHAR(100) NOT NULL,   -- as scraped: "Q. Bình Thạnh", "Bình Thạnh"
    canonical_name  VARCHAR(100) NOT NULL,   -- normalized: "Binh Thanh"
    city            VARCHAR(20)  NOT NULL    -- 'HCMC' | 'HN'
);

CREATE UNIQUE INDEX idx_district_raw ON district_mapping(raw_name, city);

-- Seed data — HCMC
INSERT INTO district_mapping (raw_name, canonical_name, city) VALUES
('Quận 1',          'District 1',       'HCMC'),
('Q. 1',            'District 1',       'HCMC'),
('Q1',              'District 1',       'HCMC'),
('Quận 2',          'District 2',       'HCMC'),
('Q. 2',            'District 2',       'HCMC'),
('Quận 3',          'District 3',       'HCMC'),
('Quận 4',          'District 4',       'HCMC'),
('Quận 5',          'District 5',       'HCMC'),
('Quận 6',          'District 6',       'HCMC'),
('Quận 7',          'District 7',       'HCMC'),
('Quận 8',          'District 8',       'HCMC'),
('Quận 9',          'District 9',       'HCMC'),
('Quận 10',         'District 10',      'HCMC'),
('Quận 11',         'District 11',      'HCMC'),
('Quận 12',         'District 12',      'HCMC'),
('Bình Thạnh',      'Binh Thanh',       'HCMC'),
('Q. Bình Thạnh',   'Binh Thanh',       'HCMC'),
('Phú Nhuận',       'Phu Nhuan',        'HCMC'),
('Gò Vấp',          'Go Vap',           'HCMC'),
('Tân Bình',        'Tan Binh',         'HCMC'),
('Tân Phú',         'Tan Phu',          'HCMC'),
('Bình Tân',        'Binh Tan',         'HCMC'),
('Thủ Đức',         'Thu Duc',          'HCMC'),
('TP. Thủ Đức',     'Thu Duc',          'HCMC'),
('Nhà Bè',          'Nha Be',           'HCMC'),
('Bình Chánh',      'Binh Chanh',       'HCMC'),
('Hóc Môn',         'Hoc Mon',          'HCMC'),
('Củ Chi',          'Cu Chi',           'HCMC'),
('Cần Giờ',         'Can Gio',          'HCMC'),
-- Seed data — HN
('Hoàn Kiếm',       'Hoan Kiem',        'HN'),
('Ba Đình',         'Ba Dinh',          'HN'),
('Đống Đa',         'Dong Da',          'HN'),
('Hai Bà Trưng',    'Hai Ba Trung',     'HN'),
('Hoàng Mai',       'Hoang Mai',        'HN'),
('Thanh Xuân',      'Thanh Xuan',       'HN'),
('Cầu Giấy',        'Cau Giay',         'HN'),
('Tây Hồ',          'Tay Ho',           'HN'),
('Long Biên',       'Long Bien',        'HN'),
('Bắc Từ Liêm',     'Bac Tu Liem',      'HN'),
('Nam Từ Liêm',     'Nam Tu Liem',      'HN'),
('Hà Đông',         'Ha Dong',          'HN'),
('Gia Lâm',         'Gia Lam',          'HN'),
('Đông Anh',        'Dong Anh',         'HN'),
('Sóc Sơn',         'Soc Son',          'HN');

-- ============================================
-- 2. Main apartments table
-- ============================================
CREATE TABLE apartments (
    id              SERIAL PRIMARY KEY,
    listing_id      VARCHAR(50)     NOT NULL,       -- from site (e.g. BDS-12345)
    source_url      TEXT            NOT NULL,
    title           TEXT,
    city            VARCHAR(20)     NOT NULL,        -- 'HCMC' | 'HN'
    district_raw    VARCHAR(100),                   -- as scraped, before normalization
    district        VARCHAR(100),                   -- canonical from district_mapping
    area_m2         NUMERIC(8, 2),
    price_vnd       BIGINT,                         -- total price in VND
    price_per_m2    BIGINT,                         -- computed: price_vnd / area_m2
    bedrooms        SMALLINT,
    posted_date     DATE,
    scraped_at      TIMESTAMPTZ     DEFAULT NOW(),
    is_valid        BOOLEAN         DEFAULT TRUE,   -- FALSE if flagged by data quality
    invalid_reason  TEXT                            -- e.g. 'area < 15m2', 'price > P99'
);

CREATE UNIQUE INDEX idx_apartments_url    ON apartments(source_url);
CREATE UNIQUE INDEX idx_apartments_lid    ON apartments(listing_id, city);
CREATE INDEX        idx_apartments_city   ON apartments(city);
CREATE INDEX        idx_apartments_dist   ON apartments(district);
CREATE INDEX        idx_apartments_valid  ON apartments(is_valid);

-- ============================================
-- 3. Price history (for time series analysis)
-- Captures price changes if a listing is re-scraped
-- ============================================
CREATE TABLE price_history (
    id              SERIAL PRIMARY KEY,
    apartment_id    INT             NOT NULL REFERENCES apartments(id),
    price_vnd       BIGINT          NOT NULL,
    price_per_m2    BIGINT,
    recorded_at     TIMESTAMPTZ     DEFAULT NOW()
);

CREATE INDEX idx_price_history_apt ON price_history(apartment_id);

-- ============================================
-- 4. Scrape logs (track each scrape run)
-- ============================================
CREATE TABLE scrape_logs (
    id              SERIAL PRIMARY KEY,
    city            VARCHAR(20),
    started_at      TIMESTAMPTZ     DEFAULT NOW(),
    finished_at     TIMESTAMPTZ,
    total_scraped   INT             DEFAULT 0,
    total_inserted  INT             DEFAULT 0,
    total_skipped   INT             DEFAULT 0,   -- duplicates
    total_invalid   INT             DEFAULT 0,
    status          VARCHAR(20)     DEFAULT 'running'  -- 'running' | 'done' | 'error'
);

-- ============================================
-- 5. Useful views
-- ============================================

-- Valid listings only, with computed fields
CREATE VIEW v_apartments_clean AS
SELECT
    id,
    listing_id,
    city,
    district,
    area_m2,
    price_vnd,
    price_per_m2,
    bedrooms,
    posted_date,
    scraped_at,
    CASE
        WHEN area_m2 < 50                          THEN 'small'
        WHEN area_m2 BETWEEN 50 AND 80             THEN 'medium'
        ELSE 'large'
    END AS size_bucket,
    CASE
        WHEN price_per_m2 < 40000000               THEN 'affordable'
        WHEN price_per_m2 BETWEEN 40000000 AND 80000000 THEN 'mid-range'
        ELSE 'luxury'
    END AS segment
FROM apartments
WHERE is_valid = TRUE;

-- District summary for dashboard
CREATE VIEW v_district_summary AS
SELECT
    city,
    district,
    COUNT(*)                        AS total_listings,
    ROUND(AVG(price_per_m2))        AS avg_price_per_m2,
    ROUND(PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY price_per_m2)) AS median_price_per_m2,
    ROUND(STDDEV(price_per_m2))     AS stddev_price,
    MIN(price_per_m2)               AS min_price_per_m2,
    MAX(price_per_m2)               AS max_price_per_m2
FROM v_apartments_clean
GROUP BY city, district
ORDER BY city, avg_price_per_m2 DESC;
