-- =============================================================================
-- Seed Redshift with Sample Data
-- Run via AWS Redshift Data API or any SQL client connected to Redshift
-- =============================================================================

-- Create customers table with watermark column
CREATE TABLE IF NOT EXISTS sales.customers (
    customer_id INTEGER PRIMARY KEY,
    company_name VARCHAR(200),
    industry VARCHAR(100),
    annual_revenue NUMERIC(15,2),
    employee_count INTEGER,
    region VARCHAR(50),
    created_at TIMESTAMP DEFAULT GETDATE(),
    updated_at TIMESTAMP DEFAULT GETDATE(),
    is_deleted BOOLEAN DEFAULT FALSE
);

-- Create orders table
CREATE TABLE IF NOT EXISTS sales.orders (
    order_id INTEGER PRIMARY KEY,
    customer_id INTEGER,
    product_name VARCHAR(200),
    quantity INTEGER,
    unit_price NUMERIC(15,2),
    total_amount NUMERIC(15,2),
    order_status VARCHAR(50),
    order_date TIMESTAMP DEFAULT GETDATE(),
    updated_at TIMESTAMP DEFAULT GETDATE()
);

-- Create products table
CREATE TABLE IF NOT EXISTS sales.products (
    product_id INTEGER PRIMARY KEY,
    product_name VARCHAR(200),
    category VARCHAR(100),
    list_price NUMERIC(15,2),
    cost_price NUMERIC(15,2),
    supplier VARCHAR(200),
    in_stock BOOLEAN DEFAULT TRUE,
    updated_at TIMESTAMP DEFAULT GETDATE()
);

-- Seed customers
INSERT INTO sales.customers (customer_id, company_name, industry, annual_revenue, employee_count, region)
VALUES
    (1, 'Acme Corp', 'Technology', 5000000.00, 250, 'West'),
    (2, 'GlobalTech Inc', 'Technology', 12000000.00, 800, 'East'),
    (3, 'DataDriven LLC', 'Analytics', 3000000.00, 120, 'Central'),
    (4, 'CloudFirst Solutions', 'Cloud Services', 8000000.00, 450, 'West'),
    (5, 'RetailMax', 'Retail', 25000000.00, 2000, 'East'),
    (6, 'FinServe Partners', 'Financial Services', 15000000.00, 600, 'Central'),
    (7, 'HealthTech Systems', 'Healthcare', 7000000.00, 350, 'West'),
    (8, 'EduLearn Platform', 'Education', 2000000.00, 80, 'East');

-- Seed orders
INSERT INTO sales.orders (order_id, customer_id, product_name, quantity, unit_price, total_amount, order_status)
VALUES
    (1, 1, 'Cloud Analytics Pro', 5, 2500.00, 12500.00, 'completed'),
    (2, 1, 'Data Warehouse Starter', 2, 5000.00, 10000.00, 'completed'),
    (3, 2, 'ML Platform Enterprise', 1, 50000.00, 50000.00, 'completed'),
    (4, 3, 'BI Dashboard Suite', 10, 1200.00, 12000.00, 'pending'),
    (5, 4, 'Cloud Migration Tool', 3, 8000.00, 24000.00, 'completed'),
    (6, 5, 'POS Analytics', 20, 500.00, 10000.00, 'completed'),
    (7, 5, 'Inventory Optimizer', 1, 15000.00, 15000.00, 'pending'),
    (8, 6, 'Risk Assessment Engine', 2, 25000.00, 50000.00, 'completed'),
    (9, 7, 'Patient Data Manager', 5, 3000.00, 15000.00, 'completed'),
    (10, 8, 'LMS Enterprise', 1, 10000.00, 10000.00, 'pending');

-- Seed products
INSERT INTO sales.products (product_id, product_name, category, list_price, cost_price, supplier)
VALUES
    (1, 'Cloud Analytics Pro', 'Analytics', 2500.00, 1200.00, 'SnowTech'),
    (2, 'Data Warehouse Starter', 'Storage', 5000.00, 2500.00, 'SnowTech'),
    (3, 'ML Platform Enterprise', 'AI/ML', 50000.00, 25000.00, 'Cortex Systems'),
    (4, 'BI Dashboard Suite', 'Visualization', 1200.00, 600.00, 'ChartWorks'),
    (5, 'Cloud Migration Tool', 'Migration', 8000.00, 4000.00, 'MigrateNow'),
    (6, 'POS Analytics', 'Retail', 500.00, 250.00, 'RetailAI');
