#!/usr/bin/env python3
"""
ETS Impact Advisor - Telematics Data Ingestion
===============================================

Ingests real-time telematics data from Kafka topic into Delta Lake tables.
Processes trip completion events and enriches with calculated fields.

Usage:
    python ingest_telematics.py --lakehouse-path "abfss://workspace@onelake.dfs.fabric.microsoft.com/lakehouse"
    
Requirements:
    pip install pyspark delta-spark confluent-kafka pandas
"""

import argparse
import logging
import json
import time
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Tuple
from pathlib import Path
import os
import sys

# PySpark and Delta imports
try:
    from pyspark.sql import SparkSession
    from pyspark.sql.functions import (
        col, lit, when, isnan, isnull, regexp_replace, 
        to_timestamp, datediff, round as spark_round,
        struct, from_json, explode, current_timestamp
    )
    from pyspark.sql.types import (
        StructType, StructField, StringType, DoubleType, 
        TimestampType, IntegerType, DateType
    )
    from delta.tables import DeltaTable
    from delta import configure_spark_with_delta_pip
except ImportError as e:
    print(f"❌ Missing PySpark/Delta dependencies: {e}")
    print("Install with: pip install pyspark delta-spark")
    sys.exit(1)

# Kafka imports
try:
    from confluent_kafka import Consumer, KafkaError, KafkaException
except ImportError as e:
    print(f"❌ Missing Kafka dependencies: {e}")
    print("Install with: pip install confluent-kafka")
    sys.exit(1)

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('telematics_ingestion.log'),
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger(__name__)

# Constants
KAFKA_TOPIC = "telematics_trip_end"
KAFKA_CONSUMER_GROUP = "ets-advisor-consumer"
BATCH_SIZE = 1000
CHECKPOINT_INTERVAL = 300  # 5 minutes
CO2_CONVERSION_FACTOR = 2.68  # kg CO2 per liter diesel

# Telematics message schema
TELEMATICS_SCHEMA = StructType([
    StructField("messageId", StringType(), True),
    StructField("vehicleId", StringType(), True),
    StructField("vin", StringType(), True),
    StructField("tripId", StringType(), True),
    StructField("startTime", StringType(), True),
    StructField("endTime", StringType(), True),
    StructField("startLatitude", DoubleType(), True),
    StructField("startLongitude", DoubleType(), True),
    StructField("endLatitude", DoubleType(), True),
    StructField("endLongitude", DoubleType(), True),
    StructField("totalDistance", DoubleType(), True),
    StructField("fuelConsumed", DoubleType(), True),
    StructField("engineHours", DoubleType(), True),
    StructField("idleTime", DoubleType(), True),
    StructField("maxSpeed", DoubleType(), True),
    StructField("avgSpeed", DoubleType(), True),
    StructField("driverId", StringType(), True),
    StructField("routeId", StringType(), True),
    StructField("timestamp", StringType(), True)
])

class TelematicsIngestionError(Exception):
    """Custom exception for telematics ingestion errors"""
    pass

class TelematicsProcessor:
    """
    Processes telematics data from Kafka and writes to Delta Lake
    """
    
    def __init__(self, lakehouse_path: str, kafka_config: Dict[str, str]):
        """
        Initialize the telematics processor
        
        Args:
            lakehouse_path: Path to the Fabric Lakehouse
            kafka_config: Kafka configuration dictionary
        """
        self.lakehouse_path = lakehouse_path
        self.kafka_config = kafka_config
        self.spark = self._create_spark_session()
        self.consumer = None
        self.batch_buffer = []
        self.last_checkpoint = time.time()
        
    def _create_spark_session(self) -> SparkSession:
        """Create Spark session optimized for Delta Lake and Fabric"""
        logger.info("🔧 Creating Spark session for Delta Lake processing...")
        
        # Configure Spark for Delta Lake
        builder = SparkSession.builder \
            .appName("ETS-Advisor-Telematics-Ingestion") \
            .config("spark.sql.adaptive.enabled", "true") \
            .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
            .config("spark.sql.adaptive.coalescePartitions.minPartitionNum", "1") \
            .config("spark.sql.adaptive.coalescePartitions.initialPartitionNum", "10") \
            .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer") \
            .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
            .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
            .config("spark.databricks.delta.autoCompact.enabled", "true") \
            .config("spark.databricks.delta.optimizeWrite.enabled", "true")
        
        # Configure for Fabric Lakehouse
        if "onelake.dfs.fabric.microsoft.com" in self.lakehouse_path:
            logger.info("📊 Configuring for Microsoft Fabric Lakehouse...")
            builder = builder.config("spark.sql.warehouse.dir", self.lakehouse_path)
        
        spark = configure_spark_with_delta_pip(builder).getOrCreate()
        
        # Set log level to reduce noise
        spark.sparkContext.setLogLevel("WARN")
        
        logger.info("✅ Spark session created successfully")
        return spark
        
    def _create_kafka_consumer(self) -> Consumer:
        """Create Kafka consumer with proper configuration"""
        logger.info("🔗 Creating Kafka consumer...")
        
        consumer_config = {
            'bootstrap.servers': self.kafka_config.get('bootstrap_servers', 'localhost:9092'),
            'group.id': KAFKA_CONSUMER_GROUP,
            'auto.offset.reset': 'latest',
            'enable.auto.commit': False,
            'max.poll.interval.ms': 300000,  # 5 minutes
            'session.timeout.ms': 30000,     # 30 seconds
            'heartbeat.interval.ms': 3000,   # 3 seconds
            'fetch.wait.max.ms': 500,        # 500ms
            **self.kafka_config  # Override with user config
        }
        
        consumer = Consumer(consumer_config)
        consumer.subscribe([KAFKA_TOPIC])
        
        logger.info(f"✅ Kafka consumer created for topic: {KAFKA_TOPIC}")
        return consumer
        
    def _validate_telematics_data(self, data: Dict) -> Dict:
        """
        Validate and clean telematics data
        
        Args:
            data: Raw telematics data dictionary
            
        Returns:
            Validated and cleaned data dictionary
        """
        try:
            # Required fields validation
            required_fields = ['vin', 'tripId', 'startTime', 'endTime', 'totalDistance', 'fuelConsumed']
            for field in required_fields:
                if field not in data or data[field] is None:
                    raise TelematicsIngestionError(f"Missing required field: {field}")
            
            # Data type validation and conversion
            validated_data = {}
            
            # String fields
            string_fields = ['vin', 'tripId', 'vehicleId', 'driverId', 'routeId', 'messageId']
            for field in string_fields:
                if field in data:
                    validated_data[field] = str(data[field]) if data[field] is not None else None
            
            # Numeric fields
            numeric_fields = {
                'totalDistance': float,
                'fuelConsumed': float,
                'startLatitude': float,
                'startLongitude': float,
                'endLatitude': float,
                'endLongitude': float,
                'engineHours': float,
                'idleTime': float,
                'maxSpeed': float,
                'avgSpeed': float
            }
            
            for field, dtype in numeric_fields.items():
                if field in data and data[field] is not None:
                    try:
                        validated_data[field] = dtype(data[field])
                    except (ValueError, TypeError):
                        logger.warning(f"Invalid {field} value: {data[field]}, setting to None")
                        validated_data[field] = None
                else:
                    validated_data[field] = None
            
            # Timestamp fields
            timestamp_fields = ['startTime', 'endTime', 'timestamp']
            for field in timestamp_fields:
                if field in data and data[field] is not None:
                    try:
                        # Parse ISO format timestamp
                        validated_data[field] = datetime.fromisoformat(data[field].replace('Z', '+00:00'))
                    except (ValueError, AttributeError):
                        logger.warning(f"Invalid timestamp format for {field}: {data[field]}")
                        validated_data[field] = None
                else:
                    validated_data[field] = None
            
            # Business logic validation
            if validated_data.get('totalDistance', 0) <= 0:
                raise TelematicsIngestionError("Total distance must be positive")
                
            if validated_data.get('fuelConsumed', 0) <= 0:
                raise TelematicsIngestionError("Fuel consumed must be positive")
                
            if validated_data.get('startTime') and validated_data.get('endTime'):
                if validated_data['startTime'] >= validated_data['endTime']:
                    raise TelematicsIngestionError("Start time must be before end time")
            
            return validated_data
            
        except Exception as e:
            logger.error(f"Data validation error: {e}")
            raise TelematicsIngestionError(f"Data validation failed: {e}")
    
    def _transform_telematics_data(self, raw_data: List[Dict]) -> List[Dict]:
        """
        Transform raw telematics data into TripFacts format
        
        Args:
            raw_data: List of raw telematics data dictionaries
            
        Returns:
            List of transformed TripFacts records
        """
        transformed_data = []
        
        for record in raw_data:
            try:
                # Validate data first
                validated_data = self._validate_telematics_data(record)
                
                # Transform to TripFacts schema
                trip_record = {
                    'TripID': validated_data.get('tripId'),
                    'VIN': validated_data.get('vin'),
                    'StartTimeUTC': validated_data.get('startTime'),
                    'EndTimeUTC': validated_data.get('endTime'),
                    'Distance_km': validated_data.get('totalDistance'),
                    'Fuel_l': validated_data.get('fuelConsumed'),
                    'StartLat': validated_data.get('startLatitude'),
                    'StartLon': validated_data.get('startLongitude'),
                    'EndLat': validated_data.get('endLatitude'),
                    'EndLon': validated_data.get('endLongitude'),
                    # Additional fields for enrichment
                    'ProcessedTime': datetime.utcnow(),
                    'DataSource': 'TELEMATICS_KAFKA',
                    'MessageId': validated_data.get('messageId'),
                    'DriverId': validated_data.get('driverId'),
                    'RouteId': validated_data.get('routeId'),
                    'EngineHours': validated_data.get('engineHours'),
                    'IdleTime': validated_data.get('idleTime'),
                    'MaxSpeed': validated_data.get('maxSpeed'),
                    'AvgSpeed': validated_data.get('avgSpeed')
                }
                
                transformed_data.append(trip_record)
                
            except TelematicsIngestionError as e:
                logger.error(f"Failed to transform record: {e}")
                continue
            except Exception as e:
                logger.error(f"Unexpected error transforming record: {e}")
                continue
        
        return transformed_data
    
    def _write_to_delta_lake(self, data: List[Dict]) -> None:
        """
        Write transformed data to Delta Lake table
        
        Args:
            data: List of transformed TripFacts records
        """
        if not data:
            logger.info("No data to write to Delta Lake")
            return
            
        try:
            logger.info(f"📝 Writing {len(data)} records to Delta Lake...")
            
            # Create DataFrame
            df = self.spark.createDataFrame(data)
            
            # Define Delta table path
            delta_table_path = f"{self.lakehouse_path}/Tables/TripFacts"
            
            # Check if table exists
            try:
                delta_table = DeltaTable.forPath(self.spark, delta_table_path)
                table_exists = True
            except Exception:
                table_exists = False
                
            if table_exists:
                # Merge data (upsert based on TripID)
                logger.info("🔄 Merging data into existing Delta table...")
                
                delta_table.alias("target").merge(
                    df.alias("source"),
                    "target.TripID = source.TripID"
                ).whenMatchedUpdateAll().whenNotMatchedInsertAll().execute()
                
                logger.info("✅ Data merged successfully")
                
            else:
                # Create new table
                logger.info("🆕 Creating new Delta table...")
                
                df.write \
                    .format("delta") \
                    .mode("overwrite") \
                    .option("path", delta_table_path) \
                    .saveAsTable("TripFacts")
                    
                logger.info("✅ Delta table created successfully")
            
            # Log statistics
            total_records = self.spark.read.format("delta").load(delta_table_path).count()
            logger.info(f"📊 Total records in TripFacts table: {total_records:,}")
            
        except Exception as e:
            logger.error(f"Failed to write to Delta Lake: {e}")
            raise TelematicsIngestionError(f"Delta Lake write failed: {e}")
    
    def _process_kafka_messages(self, messages: List) -> None:
        """
        Process a batch of Kafka messages
        
        Args:
            messages: List of Kafka messages
        """
        try:
            raw_data = []
            
            for message in messages:
                try:
                    # Parse JSON message
                    data = json.loads(message.value().decode('utf-8'))
                    raw_data.append(data)
                except json.JSONDecodeError as e:
                    logger.error(f"Failed to parse JSON message: {e}")
                    continue
                except Exception as e:
                    logger.error(f"Error processing message: {e}")
                    continue
            
            if raw_data:
                # Transform data
                transformed_data = self._transform_telematics_data(raw_data)
                
                # Write to Delta Lake
                if transformed_data:
                    self._write_to_delta_lake(transformed_data)
                    
                    # Commit Kafka offsets
                    self.consumer.commit(asynchronous=False)
                    logger.info(f"✅ Processed {len(transformed_data)} telematics records")
                else:
                    logger.warning("No valid data to process")
            
        except Exception as e:
            logger.error(f"Error processing Kafka messages: {e}")
            raise TelematicsIngestionError(f"Message processing failed: {e}")
    
    def run_ingestion(self, duration_seconds: Optional[int] = None) -> None:
        """
        Run the telematics ingestion process
        
        Args:
            duration_seconds: Optional duration to run (None for continuous)
        """
        logger.info("🚀 Starting telematics data ingestion...")
        
        try:
            # Create Kafka consumer
            self.consumer = self._create_kafka_consumer()
            
            start_time = time.time()
            message_count = 0
            
            while True:
                # Check duration limit
                if duration_seconds and (time.time() - start_time) > duration_seconds:
                    logger.info(f"⏰ Reached duration limit of {duration_seconds} seconds")
                    break
                
                try:
                    # Poll for messages
                    messages = self.consumer.consume(
                        num_messages=BATCH_SIZE,
                        timeout=1.0
                    )
                    
                    if messages:
                        # Filter out None and error messages
                        valid_messages = []
                        for msg in messages:
                            if msg is None:
                                continue
                            if msg.error():
                                if msg.error().code() == KafkaError._PARTITION_EOF:
                                    logger.debug("Reached end of partition")
                                else:
                                    logger.error(f"Consumer error: {msg.error()}")
                                continue
                            valid_messages.append(msg)
                        
                        if valid_messages:
                            self._process_kafka_messages(valid_messages)
                            message_count += len(valid_messages)
                    
                    # Periodic logging
                    if time.time() - self.last_checkpoint > CHECKPOINT_INTERVAL:
                        logger.info(f"📈 Processed {message_count} messages in last {CHECKPOINT_INTERVAL} seconds")
                        message_count = 0
                        self.last_checkpoint = time.time()
                
                except KafkaException as e:
                    logger.error(f"Kafka exception: {e}")
                    time.sleep(5)  # Wait before retrying
                    continue
                except Exception as e:
                    logger.error(f"Unexpected error: {e}")
                    time.sleep(5)  # Wait before retrying
                    continue
        
        except KeyboardInterrupt:
            logger.info("🛑 Received interrupt signal, shutting down...")
        except Exception as e:
            logger.error(f"Fatal error in ingestion process: {e}")
            raise
        finally:
            # Cleanup
            if self.consumer:
                self.consumer.close()
                logger.info("🔌 Kafka consumer closed")
            
            if self.spark:
                self.spark.stop()
                logger.info("🔌 Spark session stopped")
            
            logger.info("🏁 Telematics ingestion process completed")

def main():
    """Main function to run telematics ingestion"""
    parser = argparse.ArgumentParser(description='ETS Impact Advisor - Telematics Data Ingestion')
    parser.add_argument('--lakehouse-path', required=True,
                       help='Path to Fabric Lakehouse (e.g., abfss://workspace@onelake.dfs.fabric.microsoft.com/lakehouse)')
    parser.add_argument('--kafka-bootstrap-servers', default='localhost:9092',
                       help='Kafka bootstrap servers')
    parser.add_argument('--kafka-security-protocol', default='PLAINTEXT',
                       help='Kafka security protocol')
    parser.add_argument('--kafka-sasl-mechanism', default='PLAIN',
                       help='Kafka SASL mechanism')
    parser.add_argument('--kafka-sasl-username', default='',
                       help='Kafka SASL username')
    parser.add_argument('--kafka-sasl-password', default='',
                       help='Kafka SASL password')
    parser.add_argument('--duration', type=int, default=None,
                       help='Duration to run ingestion (seconds, None for continuous)')
    parser.add_argument('--log-level', default='INFO',
                       choices=['DEBUG', 'INFO', 'WARNING', 'ERROR'],
                       help='Log level')
    
    args = parser.parse_args()
    
    # Set log level
    logging.getLogger().setLevel(getattr(logging, args.log_level))
    
    # Create Kafka configuration
    kafka_config = {
        'bootstrap_servers': args.kafka_bootstrap_servers,
        'security.protocol': args.kafka_security_protocol,
        'sasl.mechanism': args.kafka_sasl_mechanism,
        'sasl.username': args.kafka_sasl_username,
        'sasl.password': args.kafka_sasl_password
    }
    
    # Remove empty values
    kafka_config = {k: v for k, v in kafka_config.items() if v}
    
    try:
        # Create processor
        processor = TelematicsProcessor(args.lakehouse_path, kafka_config)
        
        # Run ingestion
        processor.run_ingestion(args.duration)
        
    except Exception as e:
        logger.error(f"Failed to run telematics ingestion: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main() 