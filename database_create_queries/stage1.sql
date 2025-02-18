-- Create tables
CREATE TABLE s1_review (
    r_reviewer_source_key    VARCHAR(21)  NOT NULL DEFAULT '*Unknown',
    r_product_key            VARCHAR(10)  NOT NULL,
    r_reviewer_name          TEXT NOT NULL DEFAULT '*Unknown username',
    r_helpfulness_rating     FLOAT,
    r_review_text            TEXT NOT NULL DEFAULT '*Unknown review text',
    r_review_score           FLOAT,
    r_review_title           TEXT NOT NULL DEFAULT '*Unknown review title',
    r_review_datetime        TIMESTAMP    NOT NULL DEFAULT '1900-01-01 00:00:00'
);

CREATE TABLE s1_product (
    p_product_metadata_id    VARCHAR(7)   NOT NULL DEFAULT '*None',
    p_product_source_key     VARCHAR(10)  NOT NULL DEFAULT '*None',
    p_sales_rank_category    VARCHAR(50)  NOT NULL DEFAULT '*Unknown category',
    p_sales_rank            INT           NOT NULL DEFAULT -1,
    p_image_url             VARCHAR(225)  NOT NULL DEFAULT '*Unknown URL',
    p_title                 TEXT  NOT NULL DEFAULT '*Unknown title',
    p_description           TEXT  NOT NULL DEFAULT '*Unknown description',
    p_price                 FLOAT NOT NULL DEFAULT -1.00,
    p_brand                 VARCHAR(150)  NOT NULL DEFAULT '*Unknown brand'
);

CREATE TABLE s1_product_category (
    pc_product_source_key VARCHAR(10)  NOT NULL,
    pc_category           VARCHAR(150) NOT NULL DEFAULT '*No Category'
);

CREATE TABLE s1_related_product (
    rl_product_source_key         VARCHAR(10) NOT NULL,
    rl_related_product_source_key VARCHAR(10) NOT NULL,
    rl_relation                   VARCHAR(20) NOT NULL
);

-- Create Indexes
CREATE INDEX idx_s1_review ON s1_review (
    r_reviewer_source_key, r_product_key, r_review_datetime
);

CREATE INDEX idx_s1_product ON s1_product (
    p_product_metadata_id, p_product_source_key
);

CREATE INDEX idx_s1_product_category ON s1_product_category (
    pc_product_source_key, pc_category
);

CREATE INDEX idx_s1_related_product ON s1_related_product (
    LEAST(rl_product_source_key, rl_related_product_source_key),
    GREATEST(rl_product_source_key, rl_related_product_source_key),
    rl_relation
);

-- Create views
CREATE VIEW v_s1_review AS
SELECT DISTINCT *
FROM s1_review;

CREATE VIEW v_s1_product AS
SELECT DISTINCT *
FROM s1_product;

CREATE VIEW v_s1_product_category AS
SELECT DISTINCT *
FROM s1_product_category;

CREATE VIEW v_s1_related_product AS
SELECT DISTINCT
    rl_product_source_key,
    rl_related_product_source_key,
    rl_relation
FROM s1_related_product AS sp1
WHERE NOT EXISTS (
    SELECT 1
    FROM s1_related_product AS sp2
    WHERE 
        sp1.rl_product_source_key = sp2.rl_related_product_source_key
        AND sp1.rl_related_product_source_key = sp2.rl_product_source_key
);
