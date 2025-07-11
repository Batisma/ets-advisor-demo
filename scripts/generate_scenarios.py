#!/usr/bin/env python3
"""
ETS Impact Advisor - Scenario Generation
========================================

Generates fleet electrification scenarios with financial modeling.
Creates multiple scenarios for comparison in Power BI.

Usage:
    python generate_scenarios.py --lakehouse-path "abfss://workspace@onelake.dfs.fabric.microsoft.com/lakehouse"
    
Requirements:
    pip install pyspark delta-spark pandas numpy
"""

import argparse
import logging
import sys
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Tuple
import math

# PySpark and Delta imports
try:
    from pyspark.sql import SparkSession
    from pyspark.sql.functions import (
        col, lit, when, current_timestamp, 
        monotonically_increasing_id, round as spark_round
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

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Constants
CO2_REDUCTION_FACTOR = 0.85  # 85% CO2 reduction with electric vehicles
DISCOUNT_RATE = 0.08  # 8% discount rate for NPV calculations
ANALYSIS_PERIOD_YEARS = 5  # 5-year analysis period

class ScenarioGenerator:
    """
    Generates fleet electrification scenarios for financial analysis
    """
    
    def __init__(self, lakehouse_path: str):
        """
        Initialize the scenario generator
        
        Args:
            lakehouse_path: Path to the Fabric Lakehouse
        """
        self.lakehouse_path = lakehouse_path
        self.spark = self._create_spark_session()
        
    def _create_spark_session(self) -> SparkSession:
        """Create Spark session for Delta Lake operations"""
        logger.info("🔧 Creating Spark session...")
        
        builder = SparkSession.builder \
            .appName("ETS-Advisor-Scenario-Generation") \
            .config("spark.sql.adaptive.enabled", "true") \
            .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
            .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        
        # Configure for Fabric Lakehouse
        if "onelake.dfs.fabric.microsoft.com" in self.lakehouse_path:
            builder = builder.config("spark.sql.warehouse.dir", self.lakehouse_path)
        
        spark = configure_spark_with_delta_pip(builder).getOrCreate()
        spark.sparkContext.setLogLevel("WARN")
        
        logger.info("✅ Spark session created")
        return spark
    
    def _get_fleet_baseline(self) -> Dict[str, float]:
        """
        Get baseline fleet metrics from TripFacts table
        
        Returns:
            Dictionary with baseline metrics
        """
        try:
            logger.info("📊 Calculating fleet baseline metrics...")
            
            # Read trip facts data
            trip_facts_path = f"{self.lakehouse_path}/Tables/TripFacts"
            trip_df = self.spark.read.format("delta").load(trip_facts_path)
            
            # Calculate baseline metrics
            baseline_metrics = trip_df.agg({
                "VIN": "approx_count_distinct",
                "Distance_km": "sum", 
                "Fuel_l": "sum"
            }).collect()[0]
            
            fleet_size = baseline_metrics[0]
            total_distance = baseline_metrics[1] or 0
            total_fuel = baseline_metrics[2] or 0
            
            # Calculate derived metrics
            annual_co2_tonnes = (total_fuel * 2.68) / 1000  # kg to tonnes
            annual_distance_per_vehicle = total_distance / fleet_size if fleet_size > 0 else 0
            annual_fuel_per_vehicle = total_fuel / fleet_size if fleet_size > 0 else 0
            fuel_efficiency = (total_fuel / total_distance * 100) if total_distance > 0 else 30.0  # L/100km
            
            baseline = {
                'fleet_size': float(fleet_size),
                'annual_distance_km': float(total_distance),
                'annual_fuel_litres': float(total_fuel),
                'annual_co2_tonnes': float(annual_co2_tonnes),
                'distance_per_vehicle': float(annual_distance_per_vehicle),
                'fuel_per_vehicle': float(annual_fuel_per_vehicle),
                'fuel_efficiency_l_100km': float(fuel_efficiency)
            }
            
            logger.info(f"✅ Baseline calculated: {fleet_size} vehicles, {annual_co2_tonnes:.1f}t CO2/year")
            return baseline
            
        except Exception as e:
            logger.warning(f"Could not calculate baseline from TripFacts: {e}")
            # Return default baseline for demo
            return {
                'fleet_size': 40.0,
                'annual_distance_km': 800000.0,
                'annual_fuel_litres': 240000.0,
                'annual_co2_tonnes': 643.2,
                'distance_per_vehicle': 20000.0,
                'fuel_per_vehicle': 6000.0,
                'fuel_efficiency_l_100km': 30.0
            }
    
    def _calculate_npv(self, cash_flows: List[float], discount_rate: float = DISCOUNT_RATE) -> float:
        """
        Calculate Net Present Value
        
        Args:
            cash_flows: List of annual cash flows (year 0 = initial investment)
            discount_rate: Discount rate for NPV calculation
            
        Returns:
            Net Present Value
        """
        npv = 0.0
        for year, cash_flow in enumerate(cash_flows):
            npv += cash_flow / ((1 + discount_rate) ** year)
        return npv
    
    def _calculate_payback_period(self, initial_investment: float, annual_savings: float) -> float:
        """
        Calculate simple payback period
        
        Args:
            initial_investment: Initial capital investment
            annual_savings: Annual cost savings
            
        Returns:
            Payback period in years
        """
        if annual_savings <= 0:
            return 999.0  # Never pays back
        return initial_investment / annual_savings
    
    def _generate_scenario_a(self, baseline: Dict[str, float]) -> Dict:
        """
        Generate Scenario A: Conservative Replacement
        
        Args:
            baseline: Baseline fleet metrics
            
        Returns:
            Scenario A data dictionary
        """
        logger.info("📋 Generating Scenario A: Conservative Replacement")
        
        # Scenario parameters
        vehicles_to_replace = min(10, int(baseline['fleet_size'] * 0.25))  # 25% of fleet, max 10
        electric_vehicle_cost = 180000  # €180k per electric truck
        infrastructure_cost = 50000  # €50k charging infrastructure
        
        # Calculate costs
        total_capex = (vehicles_to_replace * electric_vehicle_cost) + infrastructure_cost
        
        # Calculate savings
        fuel_cost_per_litre = 1.45  # €/L
        annual_fuel_savings_per_vehicle = baseline['fuel_per_vehicle'] * 0.85 * fuel_cost_per_litre  # 85% fuel reduction
        annual_maintenance_savings_per_vehicle = 2500  # €2,500/year maintenance savings
        annual_co2_reduction_per_vehicle = (baseline['fuel_per_vehicle'] * 2.68 / 1000) * 0.85  # 85% CO2 reduction
        
        total_annual_fuel_savings = vehicles_to_replace * annual_fuel_savings_per_vehicle
        total_annual_maintenance_savings = vehicles_to_replace * annual_maintenance_savings_per_vehicle
        total_annual_co2_reduction = vehicles_to_replace * annual_co2_reduction_per_vehicle
        
        # ETS cost savings (€85/tonne CO2)
        annual_ets_savings = total_annual_co2_reduction * 85
        
        total_annual_savings = total_annual_fuel_savings + total_annual_maintenance_savings + annual_ets_savings
        
        # Calculate NPV over 5 years
        cash_flows = [-float(total_capex)]  # Year 0: investment
        for year in range(1, 6):
            cash_flows.append(float(total_annual_savings))
        
        npv = self._calculate_npv(cash_flows)
        roi = (npv / total_capex * 100) if total_capex > 0 else 0
        payback_years = self._calculate_payback_period(total_capex, total_annual_savings)
        
        return {
            'ScenarioID': 'A',
            'ScenarioName': 'Conservative Replacement',
            'TimeHorizon_months': 60,
            'VehiclesReplaced': vehicles_to_replace,
            'CapEx_EUR': total_capex,
            'OpEx_EUR': total_annual_maintenance_savings * 5,  # 5-year total
            'CO2_Reduction_t': total_annual_co2_reduction * 5,  # 5-year total
            'ETS_Savings_EUR': annual_ets_savings * 5,  # 5-year total
            'FuelSavings_EUR': total_annual_fuel_savings * 5,  # 5-year total
            'TotalSavings_EUR': total_annual_savings * 5,  # 5-year total
            'NPV_EUR': npv,
            'ROI_Percent': roi,
            'Payback_Years': payback_years,
            'Risk_Level': 'LOW',
            'Implementation_Complexity': 'SIMPLE'
        }
    
    def _generate_scenario_b(self, baseline: Dict[str, float]) -> Dict:
        """
        Generate Scenario B: Aggressive Electrification
        
        Args:
            baseline: Baseline fleet metrics
            
        Returns:
            Scenario B data dictionary
        """
        logger.info("📋 Generating Scenario B: Aggressive Electrification")
        
        # Scenario parameters
        vehicles_to_replace = min(25, int(baseline['fleet_size'] * 0.62))  # 62% of fleet, max 25
        electric_vehicle_cost = 160000  # €160k per electric truck (economies of scale)
        infrastructure_cost = 120000  # €120k charging infrastructure (more extensive)
        
        # Calculate costs
        total_capex = (vehicles_to_replace * electric_vehicle_cost) + infrastructure_cost
        
        # Calculate savings (higher due to scale and better technology)
        fuel_cost_per_litre = 1.45  # €/L
        annual_fuel_savings_per_vehicle = baseline['fuel_per_vehicle'] * 0.88 * fuel_cost_per_litre  # 88% fuel reduction
        annual_maintenance_savings_per_vehicle = 3000  # €3,000/year maintenance savings (better with scale)
        annual_co2_reduction_per_vehicle = (baseline['fuel_per_vehicle'] * 2.68 / 1000) * 0.88  # 88% CO2 reduction
        
        total_annual_fuel_savings = vehicles_to_replace * annual_fuel_savings_per_vehicle
        total_annual_maintenance_savings = vehicles_to_replace * annual_maintenance_savings_per_vehicle
        total_annual_co2_reduction = vehicles_to_replace * annual_co2_reduction_per_vehicle
        
        # ETS cost savings (€85/tonne CO2)
        annual_ets_savings = total_annual_co2_reduction * 85
        
        total_annual_savings = total_annual_fuel_savings + total_annual_maintenance_savings + annual_ets_savings
        
        # Calculate NPV over 5 years
        cash_flows = [-float(total_capex)]  # Year 0: investment
        for year in range(1, 6):
            cash_flows.append(float(total_annual_savings))
        
        npv = self._calculate_npv(cash_flows)
        roi = (npv / total_capex * 100) if total_capex > 0 else 0
        payback_years = self._calculate_payback_period(total_capex, total_annual_savings)
        
        return {
            'ScenarioID': 'B',
            'ScenarioName': 'Aggressive Electrification',
            'TimeHorizon_months': 60,
            'VehiclesReplaced': vehicles_to_replace,
            'CapEx_EUR': total_capex,
            'OpEx_EUR': total_annual_maintenance_savings * 5,  # 5-year total
            'CO2_Reduction_t': total_annual_co2_reduction * 5,  # 5-year total
            'ETS_Savings_EUR': annual_ets_savings * 5,  # 5-year total
            'FuelSavings_EUR': total_annual_fuel_savings * 5,  # 5-year total
            'TotalSavings_EUR': total_annual_savings * 5,  # 5-year total
            'NPV_EUR': npv,
            'ROI_Percent': roi,
            'Payback_Years': payback_years,
            'Risk_Level': 'HIGH',
            'Implementation_Complexity': 'COMPLEX'
        }
    
    def _generate_scenario_c(self, baseline: Dict[str, float]) -> Dict:
        """
        Generate Scenario C: Hybrid Approach
        
        Args:
            baseline: Baseline fleet metrics
            
        Returns:
            Scenario C data dictionary
        """
        logger.info("📋 Generating Scenario C: Hybrid Approach")
        
        # Scenario parameters (mix of electric and hybrid vehicles)
        vehicles_to_replace = min(15, int(baseline['fleet_size'] * 0.37))  # 37% of fleet, max 15
        electric_vehicles = int(vehicles_to_replace * 0.6)  # 60% electric
        hybrid_vehicles = vehicles_to_replace - electric_vehicles  # 40% hybrid
        
        electric_vehicle_cost = 170000  # €170k per electric truck
        hybrid_vehicle_cost = 140000  # €140k per hybrid truck
        infrastructure_cost = 75000  # €75k charging infrastructure (moderate)
        
        # Calculate costs
        total_capex = (electric_vehicles * electric_vehicle_cost) + \
                     (hybrid_vehicles * hybrid_vehicle_cost) + infrastructure_cost
        
        # Calculate savings (weighted average of electric and hybrid)
        fuel_cost_per_litre = 1.45  # €/L
        
        # Electric vehicle savings
        electric_fuel_savings_per_vehicle = baseline['fuel_per_vehicle'] * 0.85 * fuel_cost_per_litre
        electric_maintenance_savings_per_vehicle = 2750
        electric_co2_reduction_per_vehicle = (baseline['fuel_per_vehicle'] * 2.68 / 1000) * 0.85
        
        # Hybrid vehicle savings
        hybrid_fuel_savings_per_vehicle = baseline['fuel_per_vehicle'] * 0.35 * fuel_cost_per_litre  # 35% fuel reduction
        hybrid_maintenance_savings_per_vehicle = 1500
        hybrid_co2_reduction_per_vehicle = (baseline['fuel_per_vehicle'] * 2.68 / 1000) * 0.35
        
        # Total savings
        total_annual_fuel_savings = (electric_vehicles * electric_fuel_savings_per_vehicle) + \
                                  (hybrid_vehicles * hybrid_fuel_savings_per_vehicle)
        total_annual_maintenance_savings = (electric_vehicles * electric_maintenance_savings_per_vehicle) + \
                                         (hybrid_vehicles * hybrid_maintenance_savings_per_vehicle)
        total_annual_co2_reduction = (electric_vehicles * electric_co2_reduction_per_vehicle) + \
                                   (hybrid_vehicles * hybrid_co2_reduction_per_vehicle)
        
        # ETS cost savings (€85/tonne CO2)
        annual_ets_savings = total_annual_co2_reduction * 85
        
        total_annual_savings = total_annual_fuel_savings + total_annual_maintenance_savings + annual_ets_savings
        
        # Calculate NPV over 5 years
        cash_flows = [-float(total_capex)]  # Year 0: investment
        for year in range(1, 6):
            cash_flows.append(float(total_annual_savings))
        
        npv = self._calculate_npv(cash_flows)
        roi = (npv / total_capex * 100) if total_capex > 0 else 0
        payback_years = self._calculate_payback_period(total_capex, total_annual_savings)
        
        return {
            'ScenarioID': 'C',
            'ScenarioName': 'Hybrid Approach',
            'TimeHorizon_months': 60,
            'VehiclesReplaced': vehicles_to_replace,
            'ElectricVehicles': electric_vehicles,
            'HybridVehicles': hybrid_vehicles,
            'CapEx_EUR': total_capex,
            'OpEx_EUR': total_annual_maintenance_savings * 5,  # 5-year total
            'CO2_Reduction_t': total_annual_co2_reduction * 5,  # 5-year total
            'ETS_Savings_EUR': annual_ets_savings * 5,  # 5-year total
            'FuelSavings_EUR': total_annual_fuel_savings * 5,  # 5-year total
            'TotalSavings_EUR': total_annual_savings * 5,  # 5-year total
            'NPV_EUR': npv,
            'ROI_Percent': roi,
            'Payback_Years': payback_years,
            'Risk_Level': 'MEDIUM',
            'Implementation_Complexity': 'MODERATE'
        }
    
    def _write_scenarios_to_delta(self, scenarios: List[Dict]) -> None:
        """
        Write scenario data to Delta Lake table
        
        Args:
            scenarios: List of scenario dictionaries
        """
        try:
            logger.info("📝 Writing scenarios to Delta Lake...")
            
            # Add metadata to scenarios
            for scenario in scenarios:
                scenario['CreatedDate'] = datetime.utcnow()
                scenario['CreatedBy'] = 'SCENARIO_GENERATOR'
                scenario['ModelVersion'] = '1.0'
            
            # Create DataFrame
            df = self.spark.createDataFrame(scenarios)
            
            # Define Delta table path
            delta_table_path = f"{self.lakehouse_path}/Tables/ScenarioRun"
            
            # Check if table exists
            try:
                delta_table = DeltaTable.forPath(self.spark, delta_table_path)
                table_exists = True
            except Exception:
                table_exists = False
            
            if table_exists:
                # Merge scenarios (upsert based on ScenarioID)
                logger.info("🔄 Merging scenarios into existing table...")
                
                delta_table.alias("target").merge(
                    df.alias("source"),
                    "target.ScenarioID = source.ScenarioID"
                ).whenMatchedUpdateAll().whenNotMatchedInsertAll().execute()
                
            else:
                # Create new table
                logger.info("🆕 Creating new ScenarioRun table...")
                
                df.write \
                    .format("delta") \
                    .mode("overwrite") \
                    .option("path", delta_table_path) \
                    .saveAsTable("ScenarioRun")
            
            # Log results
            total_scenarios = self.spark.read.format("delta").load(delta_table_path).count()
            logger.info(f"✅ Successfully wrote {len(scenarios)} scenarios (total: {total_scenarios})")
            
        except Exception as e:
            logger.error(f"Failed to write scenarios to Delta Lake: {e}")
            raise
    
    def generate_all_scenarios(self) -> List[Dict]:
        """
        Generate all fleet electrification scenarios
        
        Returns:
            List of scenario dictionaries
        """
        logger.info("🚀 Starting scenario generation...")
        
        try:
            # Get baseline fleet metrics
            baseline = self._get_fleet_baseline()
            
            # Generate scenarios
            scenarios = []
            scenarios.append(self._generate_scenario_a(baseline))
            scenarios.append(self._generate_scenario_b(baseline))
            scenarios.append(self._generate_scenario_c(baseline))
            
            # Write to Delta Lake
            self._write_scenarios_to_delta(scenarios)
            
            # Log summary
            logger.info("📊 Scenario Generation Summary:")
            for scenario in scenarios:
                logger.info(f"  {scenario['ScenarioID']}: {scenario['ScenarioName']}")
                logger.info(f"    Vehicles: {scenario['VehiclesReplaced']}")
                logger.info(f"    CapEx: €{scenario['CapEx_EUR']:,.0f}")
                logger.info(f"    NPV: €{scenario['NPV_EUR']:,.0f}")
                logger.info(f"    ROI: {scenario['ROI_Percent']:.1f}%")
                logger.info(f"    Payback: {scenario['Payback_Years']:.1f} years")
                logger.info("")
            
            logger.info("✅ Scenario generation completed successfully")
            return scenarios
            
        except Exception as e:
            logger.error(f"Failed to generate scenarios: {e}")
            raise
        finally:
            if self.spark:
                self.spark.stop()
                logger.info("🔌 Spark session stopped")

def main():
    """Main function to run scenario generation"""
    parser = argparse.ArgumentParser(description='ETS Impact Advisor - Scenario Generation')
    parser.add_argument('--lakehouse-path', required=True,
                       help='Path to Fabric Lakehouse')
    parser.add_argument('--log-level', default='INFO',
                       choices=['DEBUG', 'INFO', 'WARNING', 'ERROR'],
                       help='Log level')
    
    args = parser.parse_args()
    
    # Set log level
    logging.getLogger().setLevel(getattr(logging, args.log_level))
    
    try:
        # Create generator
        generator = ScenarioGenerator(args.lakehouse_path)
        
        # Generate scenarios
        scenarios = generator.generate_all_scenarios()
        
        print(f"✅ Successfully generated {len(scenarios)} scenarios")
        
    except Exception as e:
        logger.error(f"Failed to run scenario generation: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main() 