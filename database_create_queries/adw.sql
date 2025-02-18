-- adw tables
CREATE TABLE date (
    date_key INT PRIMARY KEY,                 
    date DATE NOT NULL,                          
    day_of_week VARCHAR(10),                      
    month_of_year VARCHAR(10),               
    month_of_year_number INT,                          
    day_of_month INT,                                  
    year INT NOT NULL,                          
    quarter INT,                                      
    date_description VARCHAR(100)                     
);

DO $$ 
DECLARE
    StartDate DATE := '2000-01-01';
    NumberOfYears INT := 30;
    CutoffDate DATE;
BEGIN
    -- Calculate CutoffDate based on NumberOfYears
    CutoffDate := StartDate + INTERVAL '1 year' * NumberOfYears;

    -- Create temporary table
    CREATE TEMPORARY TABLE dimdate (
        date DATE,
        day SMALLINT,
        month SMALLINT,
        quarter SMALLINT,
        year SMALLINT,
        MonthName VARCHAR(12),
        week SMALLINT
    );

    -- Insert date range into the temporary table
    INSERT INTO dimdate(date)
    SELECT generate_series(StartDate, CutoffDate, INTERVAL '1 day')::DATE;

    -- Update the dimdate table with other date details
    UPDATE dimdate
    SET
        day = EXTRACT(DAY FROM date),
        month = EXTRACT(MONTH FROM date),
        MonthName = TO_CHAR(date, 'FMMonth'),
        week = EXTRACT(WEEK FROM date),
        quarter = EXTRACT(QUARTER FROM date),
        year = EXTRACT(YEAR FROM date);

    -- Insert into the final Date table
    INSERT INTO "date" (
        date_key,
        date,
        day_of_week,
        month_of_year,
        month_of_year_number,
        day_of_month,
        year,
        quarter,
        date_description
    )
    SELECT
        -- Date in YYYYMMDD format as integer
        TO_NUMBER(TO_CHAR(date, 'YYYYMMDD'), '99999999') AS date_key,
        date,
        TO_CHAR(date, 'FMDay') AS day_of_week, -- Day of the week
        MonthName AS month_of_year,  -- Month name
        month AS month_of_year_number, -- Month number
        day AS day_of_month, -- Day of the month
        year,
        quarter,
        -- Full description
        CONCAT(TO_CHAR(date, 'FMDay'), ', ', day, ' ', TO_CHAR(date, 'FMMonth'), ' ', year) AS date_description
    FROM dimdate;

    -- Drop temporary table
    DROP TABLE dimdate;
END $$;


CREATE TABLE reviewer (
    reviewer_key        SERIAL PRIMARY KEY,
    reviewer_source_key VARCHAR(21) NOT NULL,
    reviewer_name       TEXT NOT NULL
);

CREATE TABLE product (
    product_key         SERIAL PRIMARY KEY,
    product_source_key  VARCHAR(10) NOT NULL ,
    product_metadata_id VARCHAR(7) NOT NULL ,
    sales_rank_category VARCHAR(50) NOT NULL,
    sales_rank          INT NOT NULL,
    product_image_url   VARCHAR(225) NOT NULL,
    product_title       TEXT NOT NULL,
    product_description TEXT NOT NULL,
    price               DOUBLE PRECISION NOT NULL,
    brand               VARCHAR(150) NOT NULL,
    effective_date      TIMESTAMP NOT NULL,
    expiration_date     TIMESTAMP,
    is_current          BOOLEAN NOT NULL
);


CREATE TABLE related_product (
    primary_product_key   INT NOT NULL,
    secondary_product_key INT NOT NULL,
    relation              VARCHAR(20) NOT NULL,
    PRIMARY KEY (primary_product_key, secondary_product_key),
    FOREIGN KEY (primary_product_key) REFERENCES product (product_key) ON DELETE CASCADE,
    FOREIGN KEY (secondary_product_key) REFERENCES product (product_key) ON DELETE CASCADE
);

CREATE TABLE category (
    category_key     SERIAL PRIMARY KEY,
    product_category VARCHAR(150) NOT NULL
);

CREATE TABLE product_category_bridge (
    product_key  INT NOT NULL,
    category_key INT NOT NULL,
    PRIMARY KEY (product_key, category_key),
    FOREIGN KEY (product_key) REFERENCES product (product_key) ON DELETE CASCADE,
    FOREIGN KEY (category_key) REFERENCES category (category_key) ON DELETE CASCADE
);

CREATE TABLE review_descriptors (
    review_descriptors_key SERIAL PRIMARY KEY,
    review_text            TEXT NOT NULL,
    review_title           TEXT NOT NULL
);

CREATE TABLE review_fact (
    date_reviewed_key      INT NOT NULL,
    reviewer_key           INT NOT NULL,
    product_key            INT NOT NULL,
    review_descriptors_key INT NOT NULL,
    helpfulness_rating      FLOAT,
    review_rating          FLOAT,
    PRIMARY KEY (date_reviewed_key, reviewer_key, product_key, review_descriptors_key),
    FOREIGN KEY (date_reviewed_key) REFERENCES date (date_key) ON DELETE CASCADE,
    FOREIGN KEY (reviewer_key) REFERENCES reviewer (reviewer_key) ON DELETE CASCADE,
    FOREIGN KEY (product_key) REFERENCES product (product_key) ON DELETE CASCADE,
    FOREIGN KEY (review_descriptors_key) REFERENCES review_descriptors (review_descriptors_key) ON DELETE CASCADE
);

-- indexing
-- date table
CREATE INDEX idx_date_year_month ON date (year, month_of_year_number);
CREATE INDEX idx_date_quarter ON date (quarter);

-- reviewer table
CREATE INDEX idx_reviewer_source_key ON reviewer (reviewer_source_key);
CREATE INDEX idx_reviewer_name ON reviewer (reviewer_name);

-- product table
CREATE INDEX idx_product_source_key ON product (product_source_key);
CREATE INDEX idx_sales_rank_category ON product (sales_rank_category);
CREATE INDEX idx_product_price ON product (price);
CREATE INDEX idx_product_brand ON product (brand);

-- category table
CREATE INDEX idx_product_category ON category (product_category);

-- review_descriptors table
CREATE INDEX idx_review_title_hash ON review_descriptors (md5(review_text));
CREATE INDEX idx_review_text_hash ON review_descriptors (md5(review_text));


-- Metadata tables
CREATE TABLE import_batch (
    ib_id SERIAL PRIMARY KEY,
    ib_description VARCHAR(50) NOT NULL,
    ib_year INT NOT NULL,
    ib_month INT NOT NULL,
    ib_start TIMESTAMP NOT NULL,
    ib_end TIMESTAMP NULL,
    ib_status VARCHAR(10) NOT NULL
);

CREATE TABLE import_batch_process (
    ibp_id SERIAL PRIMARY KEY,
    ib_id INT NOT NULL,
    ib_description VARCHAR(50) NOT NULL,
    ib_start TIMESTAMP NOT NULL,
    ib_end TIMESTAMP NULL,
    ib_status VARCHAR(10) NOT NULL,
    FOREIGN KEY (ib_id) REFERENCES import_batch (ib_id) ON DELETE CASCADE
);

CREATE TABLE import_batch_process_task (
    ibpt_id SERIAL PRIMARY KEY,
    ibp_id INT NOT NULL,
    ib_description VARCHAR(50) NOT NULL,
    ib_start TIMESTAMP NOT NULL,
    ib_end TIMESTAMP NULL,
    ib_status VARCHAR(10) NOT NULL,
    ibpt_records_in INT NULL,
    ibpt_records_failed INT NULL,
    ibpt_records_out INT NULL,
    ibpt_records_type_2 INT NULL,
    ibpt_records_type_1 INT NULL,
    ibpt_records_dim_new INT NULL,
    FOREIGN KEY (ibp_id) REFERENCES import_batch_process (ibp_id) ON DELETE CASCADE
);
