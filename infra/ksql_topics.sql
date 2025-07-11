-- =============================================================================
-- ETS Impact Advisor - KSQL Topics & Stream Processing
-- =============================================================================
-- KSQL commands for creating Event Hub topics and stream processing
-- Compatible with Azure Event Hub with Kafka protocol
-- 
-- Execute these commands in KSQL CLI or Event Hub with Kafka support
-- =============================================================================

-- =============================================================================
-- 1. TELEMATICS RAW DATA STREAM
-- =============================================================================
-- Creates a stream for raw telematics data from fleet vehicles

CREATE STREAM telematics_raw (
    messageId VARCHAR,
    vehicleId VARCHAR,
    vin VARCHAR,
    tripId VARCHAR,
    startTime VARCHAR,
    endTime VARCHAR,
    startLatitude DOUBLE,
    startLongitude DOUBLE,
    endLatitude DOUBLE,
    endLongitude DOUBLE,
    totalDistance DOUBLE,
    fuelConsumed DOUBLE,
    engineHours DOUBLE,
    idleTime DOUBLE,
    maxSpeed DOUBLE,
    avgSpeed DOUBLE,
    driverId VARCHAR,
    routeId VARCHAR,
    timestamp VARCHAR
) WITH (
    KAFKA_TOPIC = 'telematics-trip-end',
    VALUE_FORMAT = 'JSON',
    TIMESTAMP = 'timestamp',
    TIMESTAMP_FORMAT = 'yyyy-MM-dd''T''HH:mm:ss.SSSZ'
);

-- =============================================================================
-- 2. CLEAN TELEMATICS STREAM 
-- =============================================================================
-- Cleaned and validated telematics data with calculated fields

CREATE STREAM telematics_clean WITH (
    KAFKA_TOPIC = 'telematics-clean',
    VALUE_FORMAT = 'JSON',
    PARTITIONS = 4
) AS
SELECT 
    messageId,
    vin,
    tripId,
    PARSE_TIMESTAMP(startTime, 'yyyy-MM-dd''T''HH:mm:ss.SSSZ') AS startTimeUTC,
    PARSE_TIMESTAMP(endTime, 'yyyy-MM-dd''T''HH:mm:ss.SSSZ') AS endTimeUTC,
    startLatitude,
    startLongitude,
    endLatitude,
    endLongitude,
    totalDistance AS distance_km,
    fuelConsumed AS fuel_l,
    engineHours,
    idleTime,
    maxSpeed,
    avgSpeed,
    driverId,
    routeId,
    -- Calculated fields
    (fuelConsumed / totalDistance * 100) AS fuel_efficiency_l_100km,
    (fuelConsumed * 2.68) AS co2_kg,
    (fuelConsumed * 2.68 * 85 / 1000) AS ets_cost_eur,
    (totalDistance / ((UNIX_TIMESTAMP(PARSE_TIMESTAMP(endTime, 'yyyy-MM-dd''T''HH:mm:ss.SSSZ')) - 
                       UNIX_TIMESTAMP(PARSE_TIMESTAMP(startTime, 'yyyy-MM-dd''T''HH:mm:ss.SSSZ'))) / 3600)) AS avg_speed_calculated,
    ROWTIME AS processedTime
FROM telematics_raw
WHERE 
    vin IS NOT NULL 
    AND tripId IS NOT NULL 
    AND totalDistance > 0 
    AND fuelConsumed > 0
    AND startTime IS NOT NULL 
    AND endTime IS NOT NULL;

-- =============================================================================
-- 3. VEHICLE PERFORMANCE AGGREGATIONS
-- =============================================================================
-- Real-time aggregations by vehicle for performance monitoring

CREATE TABLE vehicle_daily_stats WITH (
    KAFKA_TOPIC = 'vehicle-daily-stats',
    VALUE_FORMAT = 'JSON',
    PARTITIONS = 4
) AS
SELECT 
    vin,
    FORMAT_DATE(startTimeUTC, 'yyyy-MM-dd') AS date_str,
    COUNT(*) AS trip_count,
    SUM(distance_km) AS total_distance_km,
    SUM(fuel_l) AS total_fuel_l,
    AVG(fuel_efficiency_l_100km) AS avg_fuel_efficiency,
    SUM(co2_kg) AS total_co2_kg,
    SUM(ets_cost_eur) AS total_ets_cost_eur,
    AVG(maxSpeed) AS avg_max_speed,
    SUM(engineHours) AS total_engine_hours,
    SUM(idleTime) AS total_idle_time,
    LATEST_BY_OFFSET(processedTime) AS last_update
FROM telematics_clean 
WINDOW TUMBLING (SIZE 1 DAY)
GROUP BY vin, FORMAT_DATE(startTimeUTC, 'yyyy-MM-dd');

-- =============================================================================
-- 4. FLEET PERFORMANCE AGGREGATIONS  
-- =============================================================================
-- Fleet-level performance metrics for dashboard

CREATE TABLE fleet_hourly_stats WITH (
    KAFKA_TOPIC = 'fleet-hourly-stats',
    VALUE_FORMAT = 'JSON',
    PARTITIONS = 2
) AS
SELECT 
    'FLEET_TOTAL' AS fleet_id,
    FORMAT_TIMESTAMP(WINDOWSTART, 'yyyy-MM-dd HH:00:00') AS hour_timestamp,
    COUNT_DISTINCT(vin) AS active_vehicles,
    COUNT(*) AS total_trips,
    SUM(distance_km) AS total_distance_km,
    SUM(fuel_l) AS total_fuel_l,
    AVG(fuel_efficiency_l_100km) AS avg_fuel_efficiency,
    SUM(co2_kg) AS total_co2_kg,
    SUM(ets_cost_eur) AS total_ets_cost_eur,
    AVG(avgSpeed) AS fleet_avg_speed,
    SUM(engineHours) AS total_engine_hours,
    SUM(idleTime) AS total_idle_time
FROM telematics_clean 
WINDOW TUMBLING (SIZE 1 HOUR)
GROUP BY 'FLEET_TOTAL';

-- =============================================================================
-- 5. FUEL EFFICIENCY ALERTS
-- =============================================================================
-- Stream for detecting fuel efficiency anomalies

CREATE STREAM fuel_efficiency_alerts WITH (
    KAFKA_TOPIC = 'fuel-efficiency-alerts',
    VALUE_FORMAT = 'JSON',
    PARTITIONS = 2
) AS
SELECT 
    vin,
    tripId,
    fuel_efficiency_l_100km,
    distance_km,
    fuel_l,
    startTimeUTC,
    endTimeUTC,
    'HIGH_FUEL_CONSUMPTION' AS alert_type,
    CASE 
        WHEN fuel_efficiency_l_100km > 35 THEN 'CRITICAL'
        WHEN fuel_efficiency_l_100km > 32 THEN 'WARNING'
        ELSE 'INFO'
    END AS severity,
    processedTime AS alert_timestamp
FROM telematics_clean
WHERE fuel_efficiency_l_100km > 30;  -- Alert threshold: > 30 L/100km

-- =============================================================================
-- 6. ETS COST MONITORING
-- =============================================================================
-- Track ETS costs in real-time for budget monitoring

CREATE TABLE ets_cost_daily WITH (
    KAFKA_TOPIC = 'ets-cost-daily',
    VALUE_FORMAT = 'JSON',
    PARTITIONS = 2
) AS
SELECT 
    FORMAT_DATE(startTimeUTC, 'yyyy-MM-dd') AS date_str,
    FORMAT_DATE(startTimeUTC, 'yyyy-MM') AS month_str,
    COUNT_DISTINCT(vin) AS active_vehicles,
    COUNT(*) AS total_trips,
    SUM(co2_kg) AS daily_co2_kg,
    SUM(ets_cost_eur) AS daily_ets_cost_eur,
    AVG(ets_cost_eur) AS avg_ets_cost_per_trip,
    LATEST_BY_OFFSET(processedTime) AS last_update
FROM telematics_clean 
WINDOW TUMBLING (SIZE 1 DAY)
GROUP BY FORMAT_DATE(startTimeUTC, 'yyyy-MM-dd'), FORMAT_DATE(startTimeUTC, 'yyyy-MM');

-- =============================================================================
-- 7. GEOSPATIAL ANALYSIS STREAM
-- =============================================================================
-- Stream for route analysis and optimization

CREATE STREAM route_analysis WITH (
    KAFKA_TOPIC = 'route-analysis',
    VALUE_FORMAT = 'JSON',
    PARTITIONS = 4
) AS
SELECT 
    vin,
    tripId,
    startLatitude,
    startLongitude,
    endLatitude,
    endLongitude,
    distance_km,
    fuel_efficiency_l_100km,
    startTimeUTC,
    endTimeUTC,
    routeId,
    -- Calculate approximate distance using Haversine formula (simplified)
    (6371 * ACOS(
        COS(RADIANS(startLatitude)) * 
        COS(RADIANS(endLatitude)) * 
        COS(RADIANS(endLongitude) - RADIANS(startLongitude)) + 
        SIN(RADIANS(startLatitude)) * 
        SIN(RADIANS(endLatitude))
    )) AS straight_line_distance_km,
    
    -- Route efficiency (actual vs straight line distance)
    (distance_km / (6371 * ACOS(
        COS(RADIANS(startLatitude)) * 
        COS(RADIANS(endLatitude)) * 
        COS(RADIANS(endLongitude) - RADIANS(startLongitude)) + 
        SIN(RADIANS(startLatitude)) * 
        SIN(RADIANS(endLatitude))
    ))) AS route_efficiency_ratio,
    
    processedTime
FROM telematics_clean
WHERE 
    startLatitude IS NOT NULL 
    AND startLongitude IS NOT NULL 
    AND endLatitude IS NOT NULL 
    AND endLongitude IS NOT NULL
    AND startLatitude BETWEEN -90 AND 90
    AND startLongitude BETWEEN -180 AND 180
    AND endLatitude BETWEEN -90 AND 90
    AND endLongitude BETWEEN -180 AND 180;

-- =============================================================================
-- 8. DRIVER PERFORMANCE STREAM
-- =============================================================================
-- Analyze driver performance patterns

CREATE TABLE driver_daily_performance WITH (
    KAFKA_TOPIC = 'driver-performance',
    VALUE_FORMAT = 'JSON',
    PARTITIONS = 4
) AS
SELECT 
    driverId,
    FORMAT_DATE(startTimeUTC, 'yyyy-MM-dd') AS date_str,
    COUNT(*) AS trips_count,
    COUNT_DISTINCT(vin) AS vehicles_driven,
    SUM(distance_km) AS total_distance_km,
    SUM(fuel_l) AS total_fuel_l,
    AVG(fuel_efficiency_l_100km) AS avg_fuel_efficiency,
    AVG(maxSpeed) AS avg_max_speed,
    AVG(avgSpeed) AS avg_driving_speed,
    SUM(idleTime) AS total_idle_time,
    (SUM(idleTime) / SUM(engineHours) * 100) AS idle_time_percentage,
    LATEST_BY_OFFSET(processedTime) AS last_update
FROM telematics_clean 
WINDOW TUMBLING (SIZE 1 DAY)
WHERE driverId IS NOT NULL
GROUP BY driverId, FORMAT_DATE(startTimeUTC, 'yyyy-MM-dd');

-- =============================================================================
-- 9. VEHICLE MAINTENANCE INSIGHTS
-- =============================================================================
-- Predictive maintenance indicators

CREATE STREAM maintenance_indicators WITH (
    KAFKA_TOPIC = 'maintenance-indicators',
    VALUE_FORMAT = 'JSON',
    PARTITIONS = 2
) AS
SELECT 
    vin,
    tripId,
    fuel_efficiency_l_100km,
    engineHours,
    idleTime,
    maxSpeed,
    distance_km,
    startTimeUTC,
    
    -- Maintenance indicators
    CASE 
        WHEN fuel_efficiency_l_100km > 35 THEN 'ENGINE_CHECK_REQUIRED'
        WHEN (idleTime / engineHours * 100) > 25 THEN 'EXCESSIVE_IDLING'
        WHEN maxSpeed > 90 THEN 'SPEED_VIOLATION'
        ELSE 'NORMAL'
    END AS maintenance_flag,
    
    -- Severity scoring
    CASE 
        WHEN fuel_efficiency_l_100km > 40 OR maxSpeed > 100 THEN 'HIGH'
        WHEN fuel_efficiency_l_100km > 35 OR (idleTime / engineHours * 100) > 30 THEN 'MEDIUM'
        ELSE 'LOW'
    END AS severity,
    
    processedTime
FROM telematics_clean
WHERE 
    fuel_efficiency_l_100km > 30 
    OR (idleTime / engineHours * 100) > 20 
    OR maxSpeed > 85;

-- =============================================================================
-- 10. SCENARIO ANALYSIS DATA STREAM
-- =============================================================================
-- Stream for feeding scenario analysis and optimization

CREATE STREAM scenario_analysis_feed WITH (
    KAFKA_TOPIC = 'scenario-analysis',
    VALUE_FORMAT = 'JSON',
    PARTITIONS = 2
) AS
SELECT 
    vin,
    FORMAT_DATE(startTimeUTC, 'yyyy-MM') AS analysis_month,
    AVG(fuel_efficiency_l_100km) AS monthly_avg_efficiency,
    SUM(distance_km) AS monthly_distance_km,
    SUM(fuel_l) AS monthly_fuel_l,
    SUM(co2_kg) AS monthly_co2_kg,
    SUM(ets_cost_eur) AS monthly_ets_cost_eur,
    COUNT(*) AS monthly_trips,
    AVG(engineHours) AS avg_engine_hours,
    
    -- Vehicle utilization metrics
    (COUNT(*) / 30.0) AS avg_trips_per_day,
    (SUM(distance_km) / 30.0) AS avg_km_per_day,
    (SUM(engineHours) / COUNT(*)) AS avg_engine_hours_per_trip,
    
    -- Cost per km calculations
    (SUM(ets_cost_eur) / SUM(distance_km)) AS ets_cost_per_km,
    (SUM(fuel_l) * 1.45 / SUM(distance_km)) AS fuel_cost_per_km,
    
    LATEST_BY_OFFSET(processedTime) AS last_update
FROM telematics_clean 
WINDOW TUMBLING (SIZE 30 DAYS)
GROUP BY vin, FORMAT_DATE(startTimeUTC, 'yyyy-MM');

-- =============================================================================
-- 11. CONNECTOR CONFIGURATIONS
-- =============================================================================
-- Kafka Connect configurations for Azure Event Hub

/*
Event Hub Kafka Connect Source Configuration:
{
  "name": "eventhub-telematics-source",
  "config": {
    "connector.class": "org.apache.kafka.connect.eventhub.EventHubSourceConnector",
    "tasks.max": "1",
    "event.hub.connection.string": "Endpoint=sb://YOUR_NAMESPACE.servicebus.windows.net/;SharedAccessKeyName=RootManageSharedAccessKey;SharedAccessKey=YOUR_KEY",
    "event.hub.name": "telematics-trip-end",
    "consumer.group": "ets-advisor-consumer",
    "key.converter": "org.apache.kafka.connect.storage.StringConverter",
    "value.converter": "org.apache.kafka.connect.json.JsonConverter",
    "value.converter.schemas.enable": "false"
  }
}

Event Hub Kafka Connect Sink Configuration:
{
  "name": "eventhub-processed-sink",
  "config": {
    "connector.class": "org.apache.kafka.connect.eventhub.EventHubSinkConnector",
    "tasks.max": "1",
    "topics": "telematics-clean,vehicle-daily-stats,fleet-hourly-stats",
    "event.hub.connection.string": "Endpoint=sb://YOUR_NAMESPACE.servicebus.windows.net/;SharedAccessKeyName=RootManageSharedAccessKey;SharedAccessKey=YOUR_KEY",
    "key.converter": "org.apache.kafka.connect.storage.StringConverter",
    "value.converter": "org.apache.kafka.connect.json.JsonConverter",
    "value.converter.schemas.enable": "false"
  }
}
*/

-- =============================================================================
-- 12. MONITORING QUERIES
-- =============================================================================
-- Useful queries for monitoring stream processing health

-- Check stream processing lag
SELECT 
    'telematics_clean' AS stream_name,
    COUNT(*) AS message_count,
    MAX(processedTime) AS latest_processed_time,
    (UNIX_TIMESTAMP() * 1000 - MAX(ROWTIME)) / 1000 AS lag_seconds
FROM telematics_clean
WHERE ROWTIME > (UNIX_TIMESTAMP() - 3600) * 1000  -- Last hour
GROUP BY 'telematics_clean';

-- Monitor data quality
SELECT 
    'data_quality_check' AS check_type,
    COUNT(*) AS total_messages,
    COUNT(CASE WHEN fuel_efficiency_l_100km > 50 THEN 1 END) AS invalid_efficiency_count,
    COUNT(CASE WHEN distance_km <= 0 THEN 1 END) AS invalid_distance_count,
    COUNT(CASE WHEN fuel_l <= 0 THEN 1 END) AS invalid_fuel_count,
    (COUNT(CASE WHEN fuel_efficiency_l_100km > 50 THEN 1 END) * 100.0 / COUNT(*)) AS invalid_efficiency_percentage
FROM telematics_clean
WHERE ROWTIME > (UNIX_TIMESTAMP() - 3600) * 1000;  -- Last hour

-- =============================================================================
-- DEPLOYMENT NOTES
-- =============================================================================
/*
1. Event Hub Setup:
   - Create Event Hub namespace in Azure
   - Enable Kafka protocol
   - Create topics with appropriate partition counts
   - Configure retention policies

2. KSQL Deployment:
   - Deploy KSQL server (can use Confluent Cloud or self-hosted)
   - Configure connection to Event Hub with Kafka protocol
   - Execute these DDL statements in order

3. Monitoring:
   - Set up monitoring for stream processing lag
   - Configure alerts for data quality issues
   - Monitor resource utilization

4. Security:
   - Use proper authentication for Event Hub
   - Configure network security groups
   - Enable audit logging

5. Scaling:
   - Adjust partition counts based on throughput requirements
   - Scale KSQL server instances for high availability
   - Monitor and tune memory allocation

6. Integration:
   - Connect processed streams to Power BI via Event Hub
   - Configure real-time dashboard refresh
   - Set up data archival to Delta Lake tables
*/ 