CREATE TABLE IF NOT EXISTS accounts (
    account VARCHAR(50),
    sector VARCHAR(50),
    year_established INT,
    revenue FLOAT,
    employees INT,
    office_location VARCHAR(30),
    subsidiary_of VARCHAR(50)
);

CREATE TABLE IF NOT EXISTS products (
    product VARCHAR(50),
    series VARCHAR(50),
    sales_price FLOAT
);

CREATE TABLE IF NOT EXISTS sales_teams (
    sales_agent VARCHAR(50),
    manager VARCHAR(50),
    regional_office VARCHAR(50)
);

CREATE TABLE IF NOT EXISTS sales_pipeline (
    opportunity_id VARCHAR(50),
    sales_agent VARCHAR(50),
    product VARCHAR(50),
    account VARCHAR(50),
    deal_stage VARCHAR(30),
    engage_date DATE,
    close_date DATE,
    close_value FLOAT
);
