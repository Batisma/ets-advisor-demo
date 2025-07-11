#!/usr/bin/env python3
"""
ETS Impact Advisor - Sample Data Generator
==========================================

Generates deterministic sample data for the demo:
- TripFacts: 90 days of trips for 40 trucks
- FuelInvoices: Monthly aggregated fuel data
- ETSPriceCurve: ETS price projections 2025-2030

Usage:
    python generate_sample_data.py --output-dir ../lakehouse/
    
Requirements:
    pip install pandas numpy faker
"""

import argparse
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from faker import Faker
from faker.providers import automotive
import random
import os
from pathlib import Path

# Set random seed for reproducibility
RANDOM_SEED = 42
random.seed(RANDOM_SEED)
np.random.seed(RANDOM_SEED)

# Initialize Faker with seed
fake = Faker()
fake.add_provider(automotive)
Faker.seed(RANDOM_SEED)

# Fleet configuration
NUM_TRUCKS = 40
NUM_DAYS = 90
TRIPS_PER_DAY_RANGE = (25, 35)  # Average 30 trips per day per truck
BASE_DATE = datetime(2024, 1, 1)

# European cities for route simulation
EUROPEAN_CITIES = [
    {"name": "Amsterdam", "lat": 52.3676, "lon": 4.9041},
    {"name": "Berlin", "lat": 52.5200, "lon": 13.4050},
    {"name": "Brussels", "lat": 50.8503, "lon": 4.3517},
    {"name": "Copenhagen", "lat": 55.6761, "lon": 12.5683},
    {"name": "Frankfurt", "lat": 50.1109, "lon": 8.6821},
    {"name": "Hamburg", "lat": 53.5511, "lon": 9.9937},
    {"name": "Madrid", "lat": 40.4168, "lon": -3.7038},
    {"name": "Milan", "lat": 45.4642, "lon": 9.1900},
    {"name": "Munich", "lat": 48.1351, "lon": 11.5820},
    {"name": "Paris", "lat": 48.8566, "lon": 2.3522},
    {"name": "Rome", "lat": 41.9028, "lon": 12.4964},
    {"name": "Vienna", "lat": 48.2082, "lon": 16.3738},
    {"name": "Warsaw", "lat": 52.2297, "lon": 21.0122},
    {"name": "Zurich", "lat": 47.3769, "lon": 8.5417}
]

# Fuel suppliers
FUEL_SUPPLIERS = [
    "Shell Energy Europe",
    "TotalEnergies Fleet",
    "BP Commercial",
    "Eni Fuel Solutions",
    "Repsol Fleet Services"
]

def generate_vin_numbers(num_trucks: int) -> list:
    """Generate realistic VIN numbers for trucks"""
    vins = []
    for i in range(num_trucks):
        # Generate VIN with pattern: Manufacturer(3) + Year(1) + Serial(13)
        manufacturer = random.choice(['WDB', 'WMH', 'VSA', 'XLR', 'YV2'])
        year_code = random.choice(['M', 'N', 'P', 'R', 'S'])  # 2021-2025
        serial = ''.join([str(random.randint(0, 9)) for _ in range(13)])
        vins.append(f"{manufacturer}{year_code}{serial}")
    return vins

def calculate_distance(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    """Calculate approximate distance between two points in km"""
    # Simple approximation for European distances
    lat_diff = abs(lat1 - lat2)
    lon_diff = abs(lon1 - lon2)
    return ((lat_diff ** 2 + lon_diff ** 2) ** 0.5) * 111.32  # Rough km per degree

def generate_trip_facts(vins: list, num_days: int) -> pd.DataFrame:
    """Generate trip facts data"""
    print(f"🚛 Generating trip facts for {len(vins)} trucks over {num_days} days...")
    
    trips = []
    trip_id = 1
    
    for day in range(num_days):
        current_date = BASE_DATE + timedelta(days=day)
        
        for vin in vins:
            # Number of trips per truck per day (varies by day of week)
            if current_date.weekday() < 5:  # Weekdays
                num_trips = random.randint(TRIPS_PER_DAY_RANGE[0], TRIPS_PER_DAY_RANGE[1])
            else:  # Weekends
                num_trips = random.randint(5, 15)
            
            # Generate trips for this truck on this day
            for trip_num in range(num_trips):
                # Select random start and end cities
                start_city = random.choice(EUROPEAN_CITIES)
                end_city = random.choice([city for city in EUROPEAN_CITIES if city != start_city])
                
                # Calculate distance
                distance = calculate_distance(
                    start_city["lat"], start_city["lon"],
                    end_city["lat"], end_city["lon"]
                )
                
                # Add some randomness to distance (±20%)
                distance *= random.uniform(0.8, 1.2)
                
                # Generate trip times
                start_hour = random.randint(5, 20)  # 5 AM to 8 PM
                start_minute = random.randint(0, 59)
                trip_duration_hours = distance / random.uniform(60, 80)  # 60-80 km/h average
                
                start_time = current_date.replace(hour=start_hour, minute=start_minute)
                end_time = start_time + timedelta(hours=trip_duration_hours)
                
                # Calculate fuel consumption (realistic for trucks)
                # Base consumption: 25-35 L/100km, adjusted for distance and load
                base_consumption = random.uniform(25, 35) / 100  # L/km
                load_factor = random.uniform(0.8, 1.2)  # Empty vs loaded
                fuel_consumed = distance * base_consumption * load_factor
                
                trips.append({
                    'TripID': f'T{trip_id:08d}',
                    'VIN': vin,
                    'StartTimeUTC': start_time,
                    'EndTimeUTC': end_time,
                    'Distance_km': round(distance, 2),
                    'Fuel_l': round(fuel_consumed, 2),
                    'StartLat': start_city["lat"] + random.uniform(-0.1, 0.1),
                    'StartLon': start_city["lon"] + random.uniform(-0.1, 0.1),
                    'EndLat': end_city["lat"] + random.uniform(-0.1, 0.1),
                    'EndLon': end_city["lon"] + random.uniform(-0.1, 0.1)
                })
                trip_id += 1
    
    print(f"✅ Generated {len(trips)} trip records")
    return pd.DataFrame(trips)

def generate_fuel_invoices(vins: list, trip_df: pd.DataFrame) -> pd.DataFrame:
    """Generate monthly fuel invoice data"""
    print(f"⛽ Generating fuel invoices for {len(vins)} trucks...")
    
    invoices = []
    invoice_id = 1001
    
    # Group trips by VIN and month
    trip_df['Month'] = trip_df['StartTimeUTC'].dt.to_period('M')
    monthly_fuel = trip_df.groupby(['VIN', 'Month'])['Fuel_l'].sum().reset_index()
    
    for _, row in monthly_fuel.iterrows():
        vin = row['VIN']
        month = row['Month']
        total_fuel = row['Fuel_l']
        
        # Add some variance to invoiced fuel (±5% for measurement differences)
        invoiced_fuel = total_fuel * random.uniform(0.95, 1.05)
        
        # Generate fuel price (€1.30-1.60 per liter with seasonal variation)
        base_price = 1.45
        seasonal_factor = 1 + 0.1 * np.sin(2 * np.pi * month.month / 12)  # ±10% seasonal
        price_per_liter = base_price * seasonal_factor * random.uniform(0.95, 1.05)
        
        total_amount = invoiced_fuel * price_per_liter
        
        invoices.append({
            'VIN': vin,
            'InvoiceMonth': month.to_timestamp(),
            'Litres': round(invoiced_fuel, 2),
            'EUR_Amount': round(total_amount, 2),
            'InvoiceNumber': f'INV-{invoice_id:06d}',
            'SupplierName': random.choice(FUEL_SUPPLIERS)
        })
        invoice_id += 1
    
    print(f"✅ Generated {len(invoices)} fuel invoice records")
    return pd.DataFrame(invoices)

def generate_ets_price_curve() -> pd.DataFrame:
    """Generate ETS price curve data for 2025-2030"""
    print("💰 Generating ETS price curve...")
    
    prices = []
    current_month = datetime(2025, 1, 1)
    base_price = 85.0  # €85/t base price
    
    for month_num in range(72):  # 6 years × 12 months
        # Add trend and seasonality
        trend = 0.5 * month_num  # €0.50/t increase per month
        seasonal = 5 * np.sin(2 * np.pi * current_month.month / 12)  # ±€5/t seasonal
        volatility = random.uniform(-8, 8)  # ±€8/t random volatility
        
        price = base_price + trend + seasonal + volatility
        
        # Price source varies over time
        if month_num < 12:
            price_source = 'MARKET'
        elif month_num < 36:
            price_source = 'FORECAST'
        else:
            price_source = 'SCENARIO'
        
        prices.append({
            'PriceMonth': current_month,
            'EUR_per_t': round(price, 2),
            'PriceSource': price_source
        })
        
        # Move to next month
        if current_month.month == 12:
            current_month = current_month.replace(year=current_month.year + 1, month=1)
        else:
            current_month = current_month.replace(month=current_month.month + 1)
    
    print(f"✅ Generated {len(prices)} price curve records")
    return pd.DataFrame(prices)

def generate_scenario_data() -> pd.DataFrame:
    """Generate scenario analysis data"""
    print("📊 Generating scenario analysis data...")
    
    scenarios = []
    
    # Scenario definitions
    scenario_configs = [
        {
            'ScenarioID': 'A',
            'ScenarioName': 'Conservative Replacement',
            'vehicles_replaced': 10,
            'capex_per_vehicle': 180000,
            'opex_reduction': 0.15,
            'co2_reduction': 0.85
        },
        {
            'ScenarioID': 'B',
            'ScenarioName': 'Aggressive Electrification',
            'vehicles_replaced': 25,
            'capex_per_vehicle': 160000,
            'opex_reduction': 0.22,
            'co2_reduction': 0.88
        },
        {
            'ScenarioID': 'C',
            'ScenarioName': 'Hybrid Approach',
            'vehicles_replaced': 15,
            'capex_per_vehicle': 140000,
            'opex_reduction': 0.18,
            'co2_reduction': 0.75
        }
    ]
    
    for config in scenario_configs:
        vehicles_replaced = config['vehicles_replaced']
        capex_per_vehicle = config['capex_per_vehicle']
        
        # Calculate scenario metrics
        total_capex = vehicles_replaced * capex_per_vehicle
        annual_opex_savings = vehicles_replaced * 15000 * config['opex_reduction']  # €15k/year base OpEx
        annual_co2_reduction = vehicles_replaced * 21 * config['co2_reduction']  # 21t CO2/year per truck
        annual_ets_savings = annual_co2_reduction * 85  # €85/t CO2
        
        total_annual_savings = annual_opex_savings + annual_ets_savings
        
        # NPV calculation (5 years, 8% discount rate)
        npv = -total_capex
        for year in range(1, 6):
            npv += total_annual_savings / (1.08 ** year)
        
        roi = (npv / total_capex) * 100
        payback_years = total_capex / total_annual_savings
        
        scenarios.append({
            'ScenarioID': config['ScenarioID'],
            'ScenarioName': config['ScenarioName'],
            'TimeHorizon_months': 60,
            'VehiclesReplaced': vehicles_replaced,
            'CapEx_EUR': total_capex,
            'OpEx_EUR': annual_opex_savings * 5,  # 5-year total
            'CO2_Reduction_t': annual_co2_reduction * 5,  # 5-year total
            'ETS_Savings_EUR': annual_ets_savings * 5,  # 5-year total
            'NPV_EUR': round(npv, 2),
            'ROI_Percent': round(roi, 1),
            'Payback_Years': round(payback_years, 1)
        })
    
    print(f"✅ Generated {len(scenarios)} scenario records")
    return pd.DataFrame(scenarios)

def main():
    """Main function to generate all sample data"""
    parser = argparse.ArgumentParser(description='Generate ETS Impact Advisor sample data')
    parser.add_argument('--output-dir', default='../lakehouse/', 
                       help='Output directory for CSV files')
    parser.add_argument('--num-trucks', type=int, default=40,
                       help='Number of trucks in fleet')
    parser.add_argument('--num-days', type=int, default=90,
                       help='Number of days to generate data for')
    
    args = parser.parse_args()
    
    # Create output directory
    output_dir = Path(args.output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)
    
    print(f"🎯 Generating ETS Impact Advisor sample data...")
    print(f"📁 Output directory: {output_dir}")
    print(f"🚛 Fleet size: {args.num_trucks} trucks")
    print(f"📅 Time period: {args.num_days} days")
    print(f"🎲 Random seed: {RANDOM_SEED}")
    print("-" * 60)
    
    # Generate VIN numbers
    vins = generate_vin_numbers(args.num_trucks)
    
    # Generate trip facts
    trip_df = generate_trip_facts(vins, args.num_days)
    trip_df.to_csv(output_dir / 'tripfacts_sample.csv', index=False)
    
    # Generate fuel invoices
    fuel_df = generate_fuel_invoices(vins, trip_df)
    fuel_df.to_csv(output_dir / 'fuel_invoices_sample.csv', index=False)
    
    # Generate ETS price curve
    price_df = generate_ets_price_curve()
    price_df.to_csv(output_dir / 'price_curve_sample.csv', index=False)
    
    # Generate scenario data
    scenario_df = generate_scenario_data()
    scenario_df.to_csv(output_dir / 'scenario_sample.csv', index=False)
    
    print("-" * 60)
    print("✅ Sample data generation complete!")
    print(f"📊 Generated files:")
    print(f"   - tripfacts_sample.csv: {len(trip_df):,} records")
    print(f"   - fuel_invoices_sample.csv: {len(fuel_df):,} records")
    print(f"   - price_curve_sample.csv: {len(price_df):,} records")
    print(f"   - scenario_sample.csv: {len(scenario_df):,} records")
    
    # Calculate total data size
    total_size = sum([
        os.path.getsize(output_dir / 'tripfacts_sample.csv'),
        os.path.getsize(output_dir / 'fuel_invoices_sample.csv'),
        os.path.getsize(output_dir / 'price_curve_sample.csv'),
        os.path.getsize(output_dir / 'scenario_sample.csv')
    ])
    
    print(f"💾 Total data size: {total_size / (1024*1024):.1f} MB")
    print(f"🎉 Ready to upload to Fabric Lakehouse!")

if __name__ == "__main__":
    main() 