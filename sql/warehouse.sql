CREATE TABLE IF NOT EXISTS dim_account (
    account_id SERIAL PRIMARY KEY,
    account_name VARCHAR(50),
    sector VARCHAR(50),
    year_established INT,
    employees INT,
    office_location VARCHAR(50),
    subsidiary_of VARCHAR(50)
);

CREATE TABLE IF NOT EXISTS dim_product (
    product_id SERIAL PRIMARY KEY,
    product_name VARCHAR(50),
    series VARCHAR(50),
    sale_price FLOAT
);

CREATE TABLE IF NOT EXISTS dim_sales_agent (
    sales_agent_id SERIAL PRIMARY KEY,
    sale_agent VARCHAR(50),
    manager VARCHAR(50),
    regional_office VARCHAR(50)
);

CREATE TABLE IF NOT EXISTS fact_accounts (
    id SERIAL PRIMARY KEY,
    account_id INT REFERENCES dim_account(account_id),
    product_id INT REFERENCES dim_product(product_id),
    sales_agent_id INT REFERENCES dim_sales_agent(sales_agent_id),
    deal_stage VARCHAR(30),
    engage_date DATE,
    close_date DATE,
    close_value FLOAT,
    deal_duration_days INT,
    conversion_rate FLOAT
);
