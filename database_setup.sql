-- Clean, focused table with only the 16 columns you need
CREATE TABLE IF NOT EXISTS yelp_businesses (
    id BIGSERIAL PRIMARY KEY,
    run_id VARCHAR(100) NOT NULL,

    -- Your 16 core columns
    biz_id VARCHAR(100) UNIQUE NOT NULL,
    name VARCHAR(500),
    year_established INTEGER,
    aggregated_rating DECIMAL(2,1),
    address_line1 TEXT,
    address_line2 TEXT,
    city VARCHAR(200),
    country VARCHAR(100),
    postal_code VARCHAR(20),
    region_code VARCHAR(50),
    category_1 VARCHAR(200),
    category_2 VARCHAR(200),
    category_3 VARCHAR(200),
    health_score VARCHAR(50),
    cuisine TEXT,
    direct_url TEXT,

    -- Metadata
    created_at TIMESTAMP DEFAULT NOW()
);

-- Indexes for fast queries
CREATE INDEX IF NOT EXISTS idx_run_id ON yelp_businesses(run_id);
CREATE INDEX IF NOT EXISTS idx_biz_id ON yelp_businesses(biz_id);
CREATE INDEX IF NOT EXISTS idx_city ON yelp_businesses(city);
CREATE INDEX IF NOT EXISTS idx_region_code ON yelp_businesses(region_code);
CREATE INDEX IF NOT EXISTS idx_category_1 ON yelp_businesses(category_1);
CREATE INDEX IF NOT EXISTS idx_created_at ON yelp_businesses(created_at);

-- Comment
COMMENT ON TABLE yelp_businesses IS 'Yelp fast food restaurant data - 16 core columns only';
