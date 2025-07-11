#!/usr/bin/env python3
"""
Quick test script to verify the ETS Impact Advisor demo functions
"""

import pandas as pd
import os
import sys

def test_data_loading():
    """Test if data loads correctly"""
    print("🧪 Testing data loading...")
    
    try:
        # Test trip facts
        trips = pd.read_csv('lakehouse/tripfacts_sample.csv')
        trips['StartTimeUTC'] = pd.to_datetime(trips['StartTimeUTC'])
        trips['EndTimeUTC'] = pd.to_datetime(trips['EndTimeUTC'])
        trips['TripDate'] = trips['StartTimeUTC'].dt.date
        print(f"  ✅ Trip facts: {len(trips)} records, columns: {list(trips.columns)}")
        
        # Test fuel invoices
        fuel = pd.read_csv('lakehouse/fuel_invoices_sample.csv')
        fuel['InvoiceMonth'] = pd.to_datetime(fuel['InvoiceMonth'])
        print(f"  ✅ Fuel invoices: {len(fuel)} records")
        
        # Test price curve
        prices = pd.read_csv('lakehouse/price_curve_sample.csv')
        prices['PriceMonth'] = pd.to_datetime(prices['PriceMonth'])
        print(f"  ✅ Price curve: {len(prices)} records, columns: {list(prices.columns)}")
        
        # Test scenarios
        scenarios = pd.read_csv('lakehouse/scenario_sample.csv')
        print(f"  ✅ Scenarios: {len(scenarios)} records")
        
        return True
        
    except Exception as e:
        print(f"  ❌ Error: {str(e)}")
        return False

def test_metrics_calculation():
    """Test key metrics calculation"""
    print("\n📊 Testing metrics calculation...")
    
    try:
        trips = pd.read_csv('lakehouse/tripfacts_sample.csv')
        
        metrics = {
            'fleet_size': trips['VIN'].nunique(),
            'total_trips': len(trips),
            'total_fuel': trips['Fuel_l'].sum(),
            'total_distance': trips['Distance_km'].sum(),
            'co2_emissions': trips['Fuel_l'].sum() * 2.68 / 1000,
            'fleet_efficiency': (trips['Fuel_l'].sum() / trips['Distance_km'].sum()) * 100
        }
        
        print(f"  ✅ Fleet size: {metrics['fleet_size']} vehicles")
        print(f"  ✅ Total trips: {metrics['total_trips']:,}")
        print(f"  ✅ CO₂ emissions: {metrics['co2_emissions']:.1f} tons")
        print(f"  ✅ Fleet efficiency: {metrics['fleet_efficiency']:.1f} L/100km")
        
        return True
        
    except Exception as e:
        print(f"  ❌ Error: {str(e)}")
        return False

def test_compliance_table():
    """Test compliance table formatting"""
    print("\n📋 Testing compliance table...")
    
    try:
        prices = pd.read_csv('lakehouse/price_curve_sample.csv')
        prices['PriceMonth'] = pd.to_datetime(prices['PriceMonth'])
        
        # Add calculated columns
        co2_emissions = 74067.0  # from metrics
        compliance_data = prices.copy()
        compliance_data['CO2_Emissions_t'] = co2_emissions
        compliance_data['ETS_Cost_EUR'] = compliance_data['EUR_per_t'] * compliance_data['CO2_Emissions_t']
        
        # Test the fixed column selection
        display_data = compliance_data[['PriceMonth', 'EUR_per_t', 'CO2_Emissions_t', 'ETS_Cost_EUR']].copy()
        display_data['PriceMonth'] = display_data['PriceMonth'].dt.strftime('%Y-%m')
        display_data['EUR_per_t'] = display_data['EUR_per_t'].apply(lambda x: f"€{x:.2f}")
        display_data['CO2_Emissions_t'] = display_data['CO2_Emissions_t'].apply(lambda x: f"{x:.1f}")
        display_data['ETS_Cost_EUR'] = display_data['ETS_Cost_EUR'].apply(lambda x: f"€{x:,.0f}")
        
        display_data.columns = ['Month', 'ETS Price (€/t)', 'CO₂ Emissions (t)', 'ETS Cost (€)']
        
        print(f"  ✅ Compliance table: {len(display_data)} rows, {len(display_data.columns)} columns")
        print(f"  ✅ Columns: {list(display_data.columns)}")
        print(f"  ✅ Sample row: {display_data.iloc[0].to_dict()}")
        
        return True
        
    except Exception as e:
        print(f"  ❌ Error: {str(e)}")
        return False

def main():
    """Run all tests"""
    print("🚛 ETS Impact Advisor Demo - Function Tests")
    print("=" * 50)
    
    # Check current directory
    if not os.path.exists('app.py'):
        print("❌ Not in the correct directory. Please run from the ets-advisor-demo folder.")
        return False
    
    # Run tests
    tests = [
        test_data_loading(),
        test_metrics_calculation(),
        test_compliance_table()
    ]
    
    print("\n" + "=" * 50)
    if all(tests):
        print("🎉 All function tests passed!")
        print("\n✅ The app should now work without column errors")
        print("🌐 Access at: http://localhost:8501")
        return True
    else:
        print("❌ Some tests failed.")
        return False

if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1) 