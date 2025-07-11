#!/usr/bin/env python3
"""
ETS Impact Advisor Demo Verification Script
This script verifies all components are working correctly
"""

import os
import sys
import pandas as pd
from pathlib import Path

def check_dependencies():
    """Check if all required dependencies are available"""
    print("🔍 Checking dependencies...")
    
    required_packages = [
        'pandas', 'streamlit', 'plotly', 'folium', 'streamlit_folium', 'numpy'
    ]
    
    missing_packages = []
    for package in required_packages:
        try:
            __import__(package)
            print(f"  ✅ {package}")
        except ImportError:
            print(f"  ❌ {package} - MISSING")
            missing_packages.append(package)
    
    if missing_packages:
        print(f"\n❌ Missing packages: {', '.join(missing_packages)}")
        print("Run: pip3 install --break-system-packages " + " ".join(missing_packages))
        return False
    else:
        print("✅ All dependencies are available!")
        return True

def check_data_files():
    """Check if all required data files exist"""
    print("\n📁 Checking data files...")
    
    required_files = [
        'lakehouse/tripfacts_sample.csv',
        'lakehouse/fuel_invoices_sample.csv',
        'lakehouse/price_curve_sample.csv',
        'lakehouse/scenario_sample.csv'
    ]
    
    missing_files = []
    for file_path in required_files:
        if os.path.exists(file_path):
            # Check file size
            size = os.path.getsize(file_path)
            if size > 0:
                print(f"  ✅ {file_path} ({size:,} bytes)")
            else:
                print(f"  ⚠️  {file_path} (empty file)")
                missing_files.append(file_path)
        else:
            print(f"  ❌ {file_path} - MISSING")
            missing_files.append(file_path)
    
    if missing_files:
        print(f"\n❌ Missing files: {', '.join(missing_files)}")
        print("Run: python3 scripts/generate_sample_data.py --output-dir lakehouse/")
        return False
    else:
        print("✅ All data files are available!")
        return True

def verify_data_quality():
    """Verify the quality of the generated data"""
    print("\n📊 Verifying data quality...")
    
    try:
        # Check trip facts
        trips = pd.read_csv('lakehouse/tripfacts_sample.csv')
        print(f"  ✅ Trip facts: {len(trips):,} records")
        print(f"      - Fleet size: {trips['VIN'].nunique()} vehicles")
        print(f"      - Date range: {trips['StartTimeUTC'].min()} to {trips['StartTimeUTC'].max()}")
        
        # Check fuel invoices
        fuel = pd.read_csv('lakehouse/fuel_invoices_sample.csv')
        print(f"  ✅ Fuel invoices: {len(fuel):,} records")
        
        # Check price curve
        prices = pd.read_csv('lakehouse/price_curve_sample.csv')
        print(f"  ✅ Price curve: {len(prices):,} records")
        
        # Check scenarios
        scenarios = pd.read_csv('lakehouse/scenario_sample.csv')
        print(f"  ✅ Scenarios: {len(scenarios):,} records")
        
        # Calculate key metrics
        total_fuel = trips['Fuel_l'].sum()
        total_distance = trips['Distance_km'].sum()
        co2_emissions = total_fuel * 2.68 / 1000  # tons
        
        print(f"\n📈 Key metrics:")
        print(f"  • Total fuel: {total_fuel:,.0f} L")
        print(f"  • Total distance: {total_distance:,.0f} km")
        print(f"  • CO₂ emissions: {co2_emissions:,.1f} tons")
        print(f"  • Fleet efficiency: {(total_fuel/total_distance)*100:.1f} L/100km")
        
        return True
        
    except Exception as e:
        print(f"❌ Error verifying data: {str(e)}")
        return False

def main():
    """Main verification function"""
    print("🚛 ETS Impact Advisor Demo Verification")
    print("=" * 45)
    
    # Check current directory
    current_dir = Path.cwd()
    print(f"📂 Current directory: {current_dir}")
    
    # Verify we're in the right directory
    if not os.path.exists('app.py'):
        print("❌ Not in the correct directory. Please run from the ets-advisor-demo folder.")
        return False
    
    # Run checks
    deps_ok = check_dependencies()
    files_ok = check_data_files()
    data_ok = verify_data_quality() if files_ok else False
    
    print("\n" + "=" * 45)
    if deps_ok and files_ok and data_ok:
        print("🎉 All checks passed! Demo is ready to run.")
        print("\nTo start the demo:")
        print("  ./start_demo.sh")
        print("\nOr manually:")
        print("  streamlit run app.py")
        return True
    else:
        print("❌ Some checks failed. Please fix the issues above.")
        return False

if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1) 