-- =============================================================================
-- ETS Impact Advisor - Lakehouse Setup
-- =============================================================================
-- This script creates the Delta tables for the ETS Impact Advisor demo
-- Run in Microsoft Fabric Notebook (Spark SQL)
-- Prerequisites: Fabric workspace with Lakehouse created
-- =============================================================================

-- Set lakehouse context (replace with your lakehouse name)
USE CATALOG lakehouse;
USE SCHEMA ets_advisor_lakehouse;

-- =============================================================================
-- 1. TripFacts Table - Core trip telemetry data
-- =============================================================================
-- Stores individual trip records from fleet telematics
-- Grain: One row per trip
-- Volume: ~108,000 rows (90 days × 40 trucks × 30 trips/day)

DROP TABLE IF EXISTS TripFacts;

CREATE TABLE TripFacts (
    TripID STRING NOT NULL,                    -- Primary key: unique trip identifier
    VIN STRING NOT NULL,                       -- Vehicle identification number
    StartTimeUTC TIMESTAMP NOT NULL,           -- Trip start timestamp (UTC)
    EndTimeUTC TIMESTAMP NOT NULL,             -- Trip end timestamp (UTC)
    Distance_km DOUBLE NOT NULL,               -- Trip distance in kilometers
    Fuel_l DOUBLE NOT NULL,                    -- Fuel consumed in liters
    StartLat DOUBLE,                           -- Start GPS latitude
    StartLon DOUBLE,                           -- Start GPS longitude
    EndLat DOUBLE,                             -- End GPS latitude
    EndLon DOUBLE,                             -- End GPS longitude
    TripDate DATE GENERATED ALWAYS AS (CAST(StartTimeUTC AS DATE)) -- Partition key
)
USING DELTA
PARTITIONED BY (TripDate)
TBLPROPERTIES (
    'delta.columnMapping.mode' = 'name',
    'delta.feature.allowColumnDefaults' = 'supported'
);

-- Add table constraints
ALTER TABLE TripFacts ADD CONSTRAINT chk_trip_duration 
    CHECK (EndTimeUTC > StartTimeUTC);

ALTER TABLE TripFacts ADD CONSTRAINT chk_positive_distance 
    CHECK (Distance_km > 0);

ALTER TABLE TripFacts ADD CONSTRAINT chk_positive_fuel 
    CHECK (Fuel_l > 0);

-- =============================================================================
-- 2. FuelInvoices Table - Monthly fuel aggregations
-- =============================================================================
-- Stores monthly fuel invoice data aggregated by vehicle
-- Grain: One row per truck per month
-- Volume: ~360 rows (40 trucks × 9 months)

DROP TABLE IF EXISTS FuelInvoices;

CREATE TABLE FuelInvoices (
    VIN STRING NOT NULL,                       -- Vehicle identification number
    InvoiceMonth DATE NOT NULL,                -- Invoice month (first day of month)
    Litres DOUBLE NOT NULL,                    -- Total litres invoiced
    EUR_Amount DOUBLE NOT NULL,                -- Total EUR amount
    EUR_per_Litre DOUBLE GENERATED ALWAYS AS (EUR_Amount / Litres), -- Unit price
    InvoiceNumber STRING,                      -- Invoice reference number
    SupplierName STRING,                       -- Fuel supplier name
    CreatedDate TIMESTAMP DEFAULT CURRENT_TIMESTAMP() -- Record creation timestamp
)
USING DELTA
TBLPROPERTIES (
    'delta.columnMapping.mode' = 'name',
    'delta.feature.allowColumnDefaults' = 'supported'
);

-- Add table constraints
ALTER TABLE FuelInvoices ADD CONSTRAINT chk_positive_litres 
    CHECK (Litres > 0);

ALTER TABLE FuelInvoices ADD CONSTRAINT chk_positive_amount 
    CHECK (EUR_Amount > 0);

-- =============================================================================
-- 3. ETSPriceCurve Table - ETS price projections
-- =============================================================================
-- Stores monthly ETS price curves for cost modeling
-- Grain: One row per month
-- Volume: 72 rows (6 years × 12 months)

DROP TABLE IF EXISTS ETSPriceCurve;

CREATE TABLE ETSPriceCurve (
    PriceMonth DATE NOT NULL,                  -- Price month (first day of month)
    EUR_per_t DOUBLE NOT NULL,                 -- ETS price in EUR per tonne CO2
    PriceSource STRING DEFAULT 'MARKET',       -- Price source (MARKET, FORECAST, SCENARIO)
    Volatility DOUBLE DEFAULT 0.15,           -- Price volatility factor
    LastUpdated TIMESTAMP DEFAULT CURRENT_TIMESTAMP() -- Last update timestamp
)
USING DELTA
TBLPROPERTIES (
    'delta.columnMapping.mode' = 'name',
    'delta.feature.allowColumnDefaults' = 'supported'
);

-- Add table constraints
ALTER TABLE ETSPriceCurve ADD CONSTRAINT chk_positive_price 
    CHECK (EUR_per_t > 0);

ALTER TABLE ETSPriceCurve ADD CONSTRAINT pk_price_month 
    PRIMARY KEY (PriceMonth);

-- =============================================================================
-- 4. ScenarioRun Table - Fleet electrification scenarios
-- =============================================================================
-- Stores scenario analysis results for fleet replacement strategies
-- Grain: One row per scenario per time period
-- Volume: ~300 rows (3 scenarios × 100 time periods)

DROP TABLE IF EXISTS ScenarioRun;

CREATE TABLE ScenarioRun (
    ScenarioID STRING NOT NULL,                -- Scenario identifier (A, B, C)
    ScenarioName STRING NOT NULL,              -- Human-readable scenario name
    TimeHorizon_months INT NOT NULL,           -- Time horizon in months
    VehiclesReplaced INT NOT NULL,             -- Number of vehicles replaced
    CapEx_EUR DOUBLE NOT NULL,                 -- Capital expenditure
    OpEx_EUR DOUBLE NOT NULL,                  -- Operating expenditure
    CO2_Reduction_t DOUBLE NOT NULL,           -- CO2 reduction in tonnes
    ETS_Savings_EUR DOUBLE NOT NULL,           -- ETS cost savings
    NPV_EUR DOUBLE NOT NULL,                   -- Net present value
    ROI_Percent DOUBLE NOT NULL,               -- Return on investment %
    Payback_Years DOUBLE NOT NULL,             -- Payback period in years
    CreatedDate TIMESTAMP DEFAULT CURRENT_TIMESTAMP() -- Record creation timestamp
)
USING DELTA
TBLPROPERTIES (
    'delta.columnMapping.mode' = 'name',
    'delta.feature.allowColumnDefaults' = 'supported'
);

-- Add table constraints
ALTER TABLE ScenarioRun ADD CONSTRAINT chk_positive_timeHorizon 
    CHECK (TimeHorizon_months > 0);

ALTER TABLE ScenarioRun ADD CONSTRAINT chk_positive_vehicles 
    CHECK (VehiclesReplaced >= 0);

-- =============================================================================
-- 5. Data Loading Section
-- =============================================================================
-- Instructions for loading sample data from CSV files
-- Run after uploading CSV files to Lakehouse Files section

-- Load TripFacts data
COPY INTO TripFacts
FROM 'Files/tripfacts_sample.csv'
FILEFORMAT = CSV
FORMAT_OPTIONS (
    'header' = 'true',
    'inferSchema' = 'true',
    'timestampFormat' = 'yyyy-MM-dd HH:mm:ss'
);

-- Load FuelInvoices data
COPY INTO FuelInvoices (VIN, InvoiceMonth, Litres, EUR_Amount, InvoiceNumber, SupplierName)
FROM 'Files/fuel_invoices_sample.csv'
FILEFORMAT = CSV
FORMAT_OPTIONS (
    'header' = 'true',
    'inferSchema' = 'true',
    'dateFormat' = 'yyyy-MM-dd'
);

-- Load ETSPriceCurve data
COPY INTO ETSPriceCurve (PriceMonth, EUR_per_t, PriceSource)
FROM 'Files/price_curve_sample.csv'
FILEFORMAT = CSV
FORMAT_OPTIONS (
    'header' = 'true',
    'inferSchema' = 'true',
    'dateFormat' = 'yyyy-MM-dd'
);

-- =============================================================================
-- 6. Data Quality Validation
-- =============================================================================
-- Run these queries to validate data loading

-- Check row counts
SELECT 'TripFacts' as TableName, COUNT(*) as RowCount FROM TripFacts
UNION ALL
SELECT 'FuelInvoices' as TableName, COUNT(*) as RowCount FROM FuelInvoices
UNION ALL
SELECT 'ETSPriceCurve' as TableName, COUNT(*) as RowCount FROM ETSPriceCurve
UNION ALL
SELECT 'ScenarioRun' as TableName, COUNT(*) as RowCount FROM ScenarioRun;

-- Check data ranges
SELECT 
    'TripFacts' as TableName,
    MIN(StartTimeUTC) as MinDate,
    MAX(EndTimeUTC) as MaxDate,
    COUNT(DISTINCT VIN) as UniqueVehicles
FROM TripFacts;

-- Check fuel data completeness
SELECT 
    COUNT(DISTINCT VIN) as VehiclesWithFuelData,
    COUNT(DISTINCT InvoiceMonth) as MonthsWithData,
    SUM(Litres) as TotalLitres,
    SUM(EUR_Amount) as TotalEUR
FROM FuelInvoices;

-- Check ETS price curve
SELECT 
    MIN(PriceMonth) as FirstMonth,
    MAX(PriceMonth) as LastMonth,
    AVG(EUR_per_t) as AvgPrice,
    MIN(EUR_per_t) as MinPrice,
    MAX(EUR_per_t) as MaxPrice
FROM ETSPriceCurve;

-- =============================================================================
-- 7. Performance Optimization
-- =============================================================================
-- Optimize tables for Direct Lake performance

-- Analyze table statistics
ANALYZE TABLE TripFacts COMPUTE STATISTICS;
ANALYZE TABLE FuelInvoices COMPUTE STATISTICS;
ANALYZE TABLE ETSPriceCurve COMPUTE STATISTICS;

-- Optimize Delta tables
OPTIMIZE TripFacts ZORDER BY (VIN, StartTimeUTC);
OPTIMIZE FuelInvoices ZORDER BY (VIN, InvoiceMonth);
OPTIMIZE ETSPriceCurve ZORDER BY (PriceMonth);

-- =============================================================================
-- 8. Security & Permissions
-- =============================================================================
-- Grant permissions for Power BI service account
-- Replace 'powerbi-service@yourdomain.com' with actual service account

-- GRANT SELECT ON TripFacts TO 'powerbi-service@yourdomain.com';
-- GRANT SELECT ON FuelInvoices TO 'powerbi-service@yourdomain.com';
-- GRANT SELECT ON ETSPriceCurve TO 'powerbi-service@yourdomain.com';
-- GRANT SELECT ON ScenarioRun TO 'powerbi-service@yourdomain.com';

-- =============================================================================
-- Setup Complete! 🎉
-- =============================================================================
-- Next steps:
-- 1. Verify all tables have data: Run validation queries above
-- 2. Connect Power BI Desktop to this lakehouse
-- 3. Create semantic model with Direct Lake mode
-- 4. Build report using the DAX measures provided
-- ============================================================================= 