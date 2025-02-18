-- Create tables
CREATE TABLE s2_product (
    p_product_metadata_id    VARCHAR(7)   NOT NULL,
    p_product_source_key     VARCHAR(10)  NOT NULL,
    p_sales_rank_category    VARCHAR(50)  NOT NULL,
    p_sales_rank            INT           NOT NULL,
    p_image_url             VARCHAR(225)  NOT NULL,
    p_title                 TEXT  NOT NULL,
    p_description           TEXT  NOT NULL,
    p_price                 FLOAT NOT NULL,
    p_brand                 VARCHAR(150)  NOT NULL,
    PRIMARY KEY (p_product_source_key)
);

CREATE TABLE s2_review (
    r_reviewer_source_key    VARCHAR(21)  NOT NULL,
    r_product_key            VARCHAR(10)  NOT NULL,
    r_reviewer_name          TEXT NOT NULL,
    r_helpfulness_rating     FLOAT,
    r_review_text            TEXT NOT NULL,
    r_review_score           FLOAT,
    r_review_title           TEXT NOT NULL,
    r_review_datetime        TIMESTAMP    NOT NULL,
    PRIMARY KEY (r_reviewer_source_key, r_product_key),
    FOREIGN KEY (r_product_key) 
        REFERENCES s2_product (p_product_source_key) 
        ON DELETE CASCADE
);

CREATE TABLE s2_product_category (
    pc_product_source_key VARCHAR(10)  NOT NULL,
    pc_category           VARCHAR(150) NOT NULL,
    PRIMARY KEY (pc_product_source_key, pc_category),
    FOREIGN KEY (pc_product_source_key) REFERENCES s2_product (p_product_source_key) ON DELETE CASCADE
);

CREATE TABLE s2_related_product (
    rl_product_source_key         VARCHAR(10) NOT NULL,
    rl_related_product_source_key VARCHAR(10) NOT NULL,
    rl_relation                   VARCHAR(20) NOT NULL,
    PRIMARY KEY (rl_product_source_key, rl_related_product_source_key, rl_relation),
    FOREIGN KEY (rl_product_source_key) REFERENCES s2_product (p_product_source_key) ON DELETE CASCADE,
    FOREIGN KEY (rl_related_product_source_key) REFERENCES s2_product (p_product_source_key) ON DELETE CASCADE
);

-- Create indexes
CREATE INDEX idx_s2_review_datetime ON s2_review (r_review_datetime);

CREATE INDEX idx_s2_product_category_category ON s2_product_category (pc_category);

CREATE INDEX idx_s2_related_product_related_product_source_key ON s2_related_product (rl_related_product_source_key);

-- Create views
CREATE VIEW v_s2_product AS
SELECT
    p_product_metadata_id,
    p_product_source_key,
    p_sales_rank_category,
    p_sales_rank,
    p_image_url,
    p_title,
    p_description,
    p_price,
    p_brand
FROM s2_product;

CREATE VIEW v_s2_review AS
SELECT
    r_reviewer_source_key,
    r_product_key,
    r_reviewer_name,
    r_helpfulness_rating,
    r_review_text,
    r_review_score,
    r_review_title,
    CAST(TO_CHAR(r_review_datetime, 'YYYYMMDD') AS INT) AS r_review_date_key
FROM s2_review;

CREATE VIEW v_s2_reviewer AS
SELECT DISTINCT
    r_reviewer_source_key,
    r_reviewer_name
FROM s2_review;

CREATE VIEW v_s2_product_category AS
SELECT
    pc_product_source_key,
    pc_category
FROM s2_product_category;

CREATE VIEW v_s2_related_product AS
SELECT
    rl_product_source_key,
    rl_related_product_source_key,
    rl_relation
FROM s2_related_product;

CREATE VIEW v_s2_product_categories_only AS
SELECT DISTINCT product_category
FROM s2_product_category;



