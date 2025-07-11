#!/usr/bin/env python3
"""
ETS Impact Advisor - Fuel Data Ingestion
=========================================

Ingests fuel invoice data from CSV files into Delta Lake tables.
Supports daily batch processing with upsert operations.

Usage:
    python ingest_fuel.py --lakehouse-path "abfss://workspace@onelake.dfs.fabric.microsoft.com/lakehouse" --data-path "/path/to/fuel/csvs"
    
Requirements:
    pip install pyspark delta-spark pandas
"""

import argparse
import logging
import glob
import os
import sys
import time
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Tuple
from pathlib import Path
import pandas as pd

# PySpark and Delta imports
try:
    from pyspark.sql import SparkSession
    from pyspark.sql.functions import (
        col, lit, when, isnan, isnull, regexp_replace, 
        to_timestamp, date_format, round as spark_round,
        sum as spark_sum, avg as spark_avg, max as spark_max,
        current_timestamp, input_file_name, monotonically_increasing_id
    )
    from pyspark.sql.types import (
        StructType, StructField, StringType, DoubleType, 
        TimestampType, IntegerType, DateType, DecimalType
    )
    from delta.tables import DeltaTable
    from delta import configure_spark_with_delta_pip
except ImportError as e:
    print(f"❌ Missing PySpark/Delta dependencies: {e}")
    print("Install with: pip install pyspark delta-spark")
    sys.exit(1)

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('fuel_ingestion.log'),
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger(__name__)

# Constants
SUPPORTED_FILE_FORMATS = ['.csv', '.tsv', '.txt']
BATCH_SIZE = 10000
CHECKPOINT_INTERVAL = 300  # 5 minutes

# Expected fuel invoice schema
FUEL_INVOICE_SCHEMA = StructType([
    StructField("VIN", StringType(), True),
    StructField("InvoiceDate", DateType(), True),
    StructField("InvoiceMonth", DateType(), True),
    StructField("Litres", DoubleType(), True),
    StructField("EUR_Amount", DoubleType(), True),
    StructField("InvoiceNumber", StringType(), True),
    StructField("SupplierName", StringType(), True),
    StructField("FuelType", StringType(), True),
    StructField("Location", StringType(), True),
    StructField("ProcessedTime", TimestampType(), True)
])

class FuelIngestionError(Exception):
    """Custom exception for fuel ingestion errors"""
    pass

class FuelDataProcessor:
    """
    Processes fuel invoice data from CSV files and writes to Delta Lake
    """
    
    def __init__(self, lakehouse_path: str, data_path: str):
        """
        Initialize the fuel data processor
        
        Args:
            lakehouse_path: Path to the Fabric Lakehouse
            data_path: Path to directory containing fuel CSV files
        """
        self.lakehouse_path = lakehouse_path
        self.data_path = data_path
        self.spark = self._create_spark_session()
        self.processed_files = set()
        
    def _create_spark_session(self) -> SparkSession:
        """Create Spark session optimized for Delta Lake and Fabric"""
        logger.info("🔧 Creating Spark session for Delta Lake processing...")
        
        # Configure Spark for Delta Lake
        builder = SparkSession.builder \
            .appName("ETS-Advisor-Fuel-Ingestion") \
            .config("spark.sql.adaptive.enabled", "true") \
            .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
            .config("spark.sql.adaptive.coalescePartitions.minPartitionNum", "1") \
            .config("spark.sql.adaptive.coalescePartitions.initialPartitionNum", "5") \
            .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer") \
            .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
            .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
            .config("spark.databricks.delta.autoCompact.enabled", "true") \
            .config("spark.databricks.delta.optimizeWrite.enabled", "true") \
            .config("spark.sql.adaptive.skewJoin.enabled", "true")
        
        # Configure for Fabric Lakehouse
        if "onelake.dfs.fabric.microsoft.com" in self.lakehouse_path:
            logger.info("📊 Configuring for Microsoft Fabric Lakehouse...")
            builder = builder.config("spark.sql.warehouse.dir", self.lakehouse_path)
        
        spark = configure_spark_with_delta_pip(builder).getOrCreate()
        
        # Set log level to reduce noise
        spark.sparkContext.setLogLevel("WARN")
        
        logger.info("✅ Spark session created successfully")
        return spark
        
    def _detect_csv_schema(self, file_path: str) -> Dict:
        """
        Detect CSV file schema and format
        
        Args:
            file_path: Path to CSV file
            
        Returns:
            Dictionary with schema information
        """
        try:
            # Read a sample of the file to detect format
            sample_df = pd.read_csv(file_path, nrows=10)
            
            # Detect delimiter
            delimiter = ','
            if '\t' in open(file_path).read(1000):
                delimiter = '\t'
            elif ';' in open(file_path).read(1000):
                delimiter = ';'
            
            # Detect column mappings
            column_mappings = {}
            columns = sample_df.columns.str.lower().str.strip()
            
            # Map common column variations
            for col in columns:
                if 'vin' in col or 'vehicle' in col:
                    column_mappings['VIN'] = col
                elif 'invoice' in col and ('date' in col or 'month' in col):
                    column_mappings['InvoiceDate'] = col
                elif 'litre' in col or 'liter' in col or 'volume' in col:
                    column_mappings['Litres'] = col
                elif 'eur' in col or 'euro' in col or 'amount' in col or 'cost' in col:
                    column_mappings['EUR_Amount'] = col
                elif 'invoice' in col and 'number' in col:
                    column_mappings['InvoiceNumber'] = col
                elif 'supplier' in col or 'vendor' in col or 'provider' in col:
                    column_mappings['SupplierName'] = col
                elif 'fuel' in col and 'type' in col:
                    column_mappings['FuelType'] = col
                elif 'location' in col or 'station' in col or 'site' in col:
                    column_mappings['Location'] = col
            
            return {
                'delimiter': delimiter,
                'column_mappings': column_mappings,
                'has_header': True,
                'encoding': 'utf-8'
            }
            
        except Exception as e:
            logger.warning(f"Could not detect schema for {file_path}: {e}")
            return {
                'delimiter': ',',
                'column_mappings': {},
                'has_header': True,
                'encoding': 'utf-8'
            }
    
    def _validate_fuel_data(self, df) -> Tuple[bool, List[str]]:
        """
        Validate fuel data quality
        
        Args:
            df: PySpark DataFrame with fuel data
            
        Returns:
            Tuple of (is_valid, list_of_errors)
        """
        errors = []
        
        try:
            # Check for required columns
            required_cols = ['VIN', 'Litres', 'EUR_Amount']
            missing_cols = [col for col in required_cols if col not in df.columns]
            if missing_cols:
                errors.append(f"Missing required columns: {missing_cols}")
            
            # Check for empty dataset
            if df.count() == 0:
                errors.append("Dataset is empty")
                return False, errors
            
            # Check for null values in critical columns
            null_counts = {}
            for col_name in required_cols:
                if col_name in df.columns:
                    null_count = df.filter(col(col_name).isNull()).count()
                    if null_count > 0:
                        null_counts[col_name] = null_count
            
            if null_counts:
                total_rows = df.count()
                for col_name, null_count in null_counts.items():
                    percentage = (null_count / total_rows) * 100
                    if percentage > 10:  # More than 10% null values
                        errors.append(f"Column {col_name} has {percentage:.1f}% null values")
            
            # Check for negative values
            if 'Litres' in df.columns:
                negative_litres = df.filter(col('Litres') < 0).count()
                if negative_litres > 0:
                    errors.append(f"Found {negative_litres} records with negative litres")
            
            if 'EUR_Amount' in df.columns:
                negative_amounts = df.filter(col('EUR_Amount') < 0).count()
                if negative_amounts > 0:
                    errors.append(f"Found {negative_amounts} records with negative amounts")
            
            # Check for duplicate invoice numbers
            if 'InvoiceNumber' in df.columns:
                total_invoices = df.filter(col('InvoiceNumber').isNotNull()).count()
                unique_invoices = df.select('InvoiceNumber').filter(col('InvoiceNumber').isNotNull()).distinct().count()
                if total_invoices != unique_invoices:
                    duplicates = total_invoices - unique_invoices
                    errors.append(f"Found {duplicates} duplicate invoice numbers")
            
            return len(errors) == 0, errors
            
        except Exception as e:
            errors.append(f"Validation error: {str(e)}")
            return False, errors
    
    def _transform_fuel_data(self, df, schema_info: Dict) -> any:
        """
        Transform raw fuel data to target schema
        
        Args:
            df: PySpark DataFrame with raw fuel data
            schema_info: Schema information dictionary
            
        Returns:
            Transformed PySpark DataFrame
        """
        try:
            logger.info("🔄 Transforming fuel data...")
            
            # Apply column mappings
            column_mappings = schema_info.get('column_mappings', {})
            transformed_df = df
            
            # Rename columns based on mappings
            for target_col, source_col in column_mappings.items():
                if source_col in df.columns:
                    transformed_df = transformed_df.withColumnRenamed(source_col, target_col)
            
            # Ensure required columns exist
            required_columns = {
                'VIN': 'UNKNOWN',
                'Litres': 0.0,
                'EUR_Amount': 0.0,
                'InvoiceNumber': None,
                'SupplierName': 'UNKNOWN',
                'FuelType': 'DIESEL',
                'Location': 'UNKNOWN'
            }
            
            for col_name, default_value in required_columns.items():
                if col_name not in transformed_df.columns:
                    transformed_df = transformed_df.withColumn(col_name, lit(default_value))
            
            # Handle date columns
            if 'InvoiceDate' in transformed_df.columns:
                # Convert to date if not already
                transformed_df = transformed_df.withColumn(
                    'InvoiceDate', 
                    to_timestamp(col('InvoiceDate')).cast('date')
                )
            else:
                # Use current date if no date column
                transformed_df = transformed_df.withColumn(
                    'InvoiceDate', 
                    current_timestamp().cast('date')
                )
            
            # Create InvoiceMonth from InvoiceDate
            transformed_df = transformed_df.withColumn(
                'InvoiceMonth',
                date_format(col('InvoiceDate'), 'yyyy-MM-01').cast('date')
            )
            
            # Clean and validate numeric columns
            transformed_df = transformed_df.withColumn(
                'Litres',
                when(col('Litres').isNull() | (col('Litres') <= 0), 0.0)
                .otherwise(col('Litres'))
            )
            
            transformed_df = transformed_df.withColumn(
                'EUR_Amount',
                when(col('EUR_Amount').isNull() | (col('EUR_Amount') <= 0), 0.0)
                .otherwise(col('EUR_Amount'))
            )
            
            # Calculate EUR per litre
            transformed_df = transformed_df.withColumn(
                'EUR_per_Litre',
                when(col('Litres') > 0, col('EUR_Amount') / col('Litres'))
                .otherwise(0.0)
            )
            
            # Add processing metadata
            transformed_df = transformed_df.withColumn(
                'ProcessedTime',
                current_timestamp()
            ).withColumn(
                'DataSource',
                lit('CSV_IMPORT')
            ).withColumn(
                'SourceFile',
                input_file_name()
            )
            
            # Select final columns
            final_columns = [
                'VIN', 'InvoiceDate', 'InvoiceMonth', 'Litres', 'EUR_Amount',
                'EUR_per_Litre', 'InvoiceNumber', 'SupplierName', 'FuelType',
                'Location', 'ProcessedTime', 'DataSource', 'SourceFile'
            ]
            
            transformed_df = transformed_df.select(*final_columns)
            
            logger.info("✅ Fuel data transformation completed")
            return transformed_df
            
        except Exception as e:
            logger.error(f"Failed to transform fuel data: {e}")
            raise FuelIngestionError(f"Data transformation failed: {e}")
    
    def _write_to_delta_lake(self, df, table_name: str = "FuelInvoices") -> None:
        """
        Write fuel data to Delta Lake table
        
        Args:
            df: PySpark DataFrame with fuel data
            table_name: Name of the Delta table
        """
        try:
            logger.info(f"📝 Writing fuel data to Delta Lake table: {table_name}")
            
            # Define Delta table path
            delta_table_path = f"{self.lakehouse_path}/Tables/{table_name}"
            
            # Check if table exists
            try:
                delta_table = DeltaTable.forPath(self.spark, delta_table_path)
                table_exists = True
            except Exception:
                table_exists = False
            
            if table_exists:
                # Merge data (upsert based on VIN + InvoiceNumber + InvoiceDate)
                logger.info("🔄 Merging fuel data into existing Delta table...")
                
                # Create merge condition
                merge_condition = """
                    target.VIN = source.VIN 
                    AND target.InvoiceNumber = source.InvoiceNumber 
                    AND target.InvoiceDate = source.InvoiceDate
                """
                
                # Perform merge
                delta_table.alias("target").merge(
                    df.alias("source"),
                    merge_condition
                ).whenMatchedUpdateAll().whenNotMatchedInsertAll().execute()
                
                logger.info("✅ Fuel data merged successfully")
                
            else:
                # Create new table
                logger.info("🆕 Creating new Delta table for fuel data...")
                
                df.write \
                    .format("delta") \
                    .mode("overwrite") \
                    .option("path", delta_table_path) \
                    .partitionBy("InvoiceMonth") \
                    .saveAsTable(table_name)
                
                logger.info("✅ Delta table created successfully")
            
            # Log statistics
            total_records = self.spark.read.format("delta").load(delta_table_path).count()
            logger.info(f"📊 Total records in {table_name} table: {total_records:,}")
            
        except Exception as e:
            logger.error(f"Failed to write to Delta Lake: {e}")
            raise FuelIngestionError(f"Delta Lake write failed: {e}")
    
    def _get_file_list(self) -> List[str]:
        """
        Get list of fuel CSV files to process
        
        Returns:
            List of file paths
        """
        files = []
        
        for extension in SUPPORTED_FILE_FORMATS:
            pattern = os.path.join(self.data_path, f"**/*{extension}")
            files.extend(glob.glob(pattern, recursive=True))
        
        # Filter out already processed files
        new_files = [f for f in files if f not in self.processed_files]
        
        logger.info(f"Found {len(new_files)} new fuel files to process")
        return new_files
    
    def process_single_file(self, file_path: str) -> bool:
        """
        Process a single fuel CSV file
        
        Args:
            file_path: Path to the CSV file
            
        Returns:
            Boolean indicating success
        """
        try:
            logger.info(f"📄 Processing file: {os.path.basename(file_path)}")
            
            # Detect file schema
            schema_info = self._detect_csv_schema(file_path)
            
            # Read CSV file
            df = self.spark.read \
                .option("header", "true") \
                .option("inferSchema", "true") \
                .option("delimiter", schema_info['delimiter']) \
                .option("encoding", schema_info['encoding']) \
                .csv(file_path)
            
            # Validate data
            is_valid, errors = self._validate_fuel_data(df)
            if not is_valid:
                logger.error(f"Data validation failed for {file_path}: {errors}")
                return False
            
            # Transform data
            transformed_df = self._transform_fuel_data(df, schema_info)
            
            # Write to Delta Lake
            self._write_to_delta_lake(transformed_df)
            
            # Mark file as processed
            self.processed_files.add(file_path)
            
            logger.info(f"✅ Successfully processed {os.path.basename(file_path)}")
            return True
            
        except Exception as e:
            logger.error(f"Failed to process file {file_path}: {e}")
            return False
    
    def run_batch_processing(self, max_files: Optional[int] = None) -> Dict[str, int]:
        """
        Run batch processing of fuel files
        
        Args:
            max_files: Maximum number of files to process (None for all)
            
        Returns:
            Dictionary with processing statistics
        """
        logger.info("🚀 Starting fuel data batch processing...")
        
        stats = {
            'total_files': 0,
            'processed_files': 0,
            'failed_files': 0,
            'skipped_files': 0
        }
        
        try:
            # Get list of files to process
            files = self._get_file_list()
            stats['total_files'] = len(files)
            
            if max_files:
                files = files[:max_files]
            
            # Process each file
            for file_path in files:
                try:
                    success = self.process_single_file(file_path)
                    if success:
                        stats['processed_files'] += 1
                    else:
                        stats['failed_files'] += 1
                        
                except Exception as e:
                    logger.error(f"Unexpected error processing {file_path}: {e}")
                    stats['failed_files'] += 1
            
            stats['skipped_files'] = stats['total_files'] - stats['processed_files'] - stats['failed_files']
            
            logger.info("📊 Batch processing completed")
            logger.info(f"   Total files: {stats['total_files']}")
            logger.info(f"   Processed: {stats['processed_files']}")
            logger.info(f"   Failed: {stats['failed_files']}")
            logger.info(f"   Skipped: {stats['skipped_files']}")
            
            return stats
            
        except Exception as e:
            logger.error(f"Fatal error in batch processing: {e}")
            raise FuelIngestionError(f"Batch processing failed: {e}")
        
        finally:
            # Cleanup
            if self.spark:
                self.spark.stop()
                logger.info("🔌 Spark session stopped")
    
    def run_continuous_monitoring(self, check_interval: int = 300) -> None:
        """
        Run continuous monitoring for new fuel files
        
        Args:
            check_interval: Check interval in seconds
        """
        logger.info(f"🔄 Starting continuous monitoring (check every {check_interval}s)")
        
        try:
            while True:
                # Run batch processing
                stats = self.run_batch_processing()
                
                if stats['processed_files'] > 0:
                    logger.info(f"✅ Processed {stats['processed_files']} new files")
                else:
                    logger.info("😴 No new files to process")
                
                # Wait for next check
                time.sleep(check_interval)
                
        except KeyboardInterrupt:
            logger.info("🛑 Received interrupt signal, shutting down...")
        except Exception as e:
            logger.error(f"Fatal error in continuous monitoring: {e}")
            raise

def main():
    """Main function to run fuel data ingestion"""
    parser = argparse.ArgumentParser(description='ETS Impact Advisor - Fuel Data Ingestion')
    parser.add_argument('--lakehouse-path', required=True,
                       help='Path to Fabric Lakehouse')
    parser.add_argument('--data-path', required=True,
                       help='Path to directory containing fuel CSV files')
    parser.add_argument('--mode', choices=['batch', 'continuous'], default='batch',
                       help='Processing mode')
    parser.add_argument('--max-files', type=int, default=None,
                       help='Maximum number of files to process (batch mode)')
    parser.add_argument('--check-interval', type=int, default=300,
                       help='Check interval in seconds (continuous mode)')
    parser.add_argument('--log-level', default='INFO',
                       choices=['DEBUG', 'INFO', 'WARNING', 'ERROR'],
                       help='Log level')
    
    args = parser.parse_args()
    
    # Set log level
    logging.getLogger().setLevel(getattr(logging, args.log_level))
    
    # Validate paths
    if not os.path.exists(args.data_path):
        logger.error(f"Data path does not exist: {args.data_path}")
        sys.exit(1)
    
    try:
        # Create processor
        processor = FuelDataProcessor(args.lakehouse_path, args.data_path)
        
        # Run processing
        if args.mode == 'batch':
            stats = processor.run_batch_processing(args.max_files)
            if stats['failed_files'] > 0:
                sys.exit(1)
        else:
            processor.run_continuous_monitoring(args.check_interval)
            
    except Exception as e:
        logger.error(f"Failed to run fuel data ingestion: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main() 