# ETS Impact Advisor Demo - Quick Start Guide

## 📊 Overview
This is a local Streamlit dashboard that demonstrates the ETS Impact Advisor capabilities using generated sample data from a 40-vehicle fleet over 90 days.

## 🚀 Starting the Demo

### Option 1: Quick Start (Recommended)
```bash
./start_demo.sh
```

### Option 2: Manual Start
```bash
streamlit run app.py
```

## 🌐 Accessing the Dashboard
Once started, open your web browser and navigate to:
**http://localhost:8501**

## 📋 Demo Pages

### 1. Fleet Digital Twin 🚛
- **Key Metrics**: Fleet size, total trips, CO₂ emissions, ETS costs
- **Interactive Map**: Shows fleet activity across Europe
- **Charts**: Daily trip volume trends
- **Fleet Performance**: Vehicle-by-vehicle efficiency summary

### 2. ETS Cost Simulator 💰
- **Price Slider**: Adjust ETS price from €50-150/ton CO₂
- **Real-time Calculation**: See immediate impact on fleet costs
- **Sensitivity Analysis**: Visual cost projections
- **Monthly Breakdown**: Historical and projected costs

### 3. Scenario Cockpit 🎯
- **Scenario Selection**: Choose from 3 electrification scenarios
- **Financial Metrics**: NPV, ROI, payback period
- **Comparison Charts**: Side-by-side scenario analysis
- **Detailed Analysis**: Full financial breakdown

### 4. Compliance Centre 📋
- **Compliance Status**: Current fleet efficiency and status
- **ETS Price Tracking**: Historical price evolution
- **Reporting**: Generate compliance reports
- **Action Buttons**: Generate purchase orders, send alerts

## 📊 Sample Data Summary

| Dataset | Records | Description |
|---------|---------|-------------|
| Trip Facts | 88,183 | Individual vehicle trips with fuel, distance, GPS |
| Fuel Invoices | 120 | Monthly fuel billing data |
| ETS Price Curve | 72 | Historical ETS carbon prices |
| Scenarios | 3 | Electrification investment scenarios |

## 🎯 Key Demo Points

1. **Real-time Analytics**: Show how ETS price changes impact fleet costs
2. **Scenario Planning**: Compare different electrification strategies
3. **Compliance Management**: Track regulatory requirements
4. **Interactive Visualizations**: Maps, charts, and dynamic calculations

## 📈 Generated Fleet Metrics
- **Fleet Size**: 40 vehicles
- **Total Fuel**: 27.6M liters
- **Total Distance**: 92.1M km
- **CO₂ Emissions**: 74,067 tons
- **Fleet Efficiency**: 30.0 L/100km
- **ETS Cost (€85/ton)**: €6.3M annually

## 🛠️ Troubleshooting

### If the demo won't start:
1. Verify dependencies: `python3 verify_demo.py`
2. Check data files exist in `lakehouse/` directory
3. Ensure port 8501 is available

### If data is missing:
```bash
python3 scripts/generate_sample_data.py --output-dir lakehouse/
```

## 💡 Demo Tips
- Use the sidebar to navigate between pages
- Try adjusting the ETS price slider to see real-time cost impacts
- Switch between scenarios to compare investment options
- The map shows a sample of trip locations for performance

## 🔧 Technical Details
- **Frontend**: Streamlit with custom CSS styling
- **Visualizations**: Plotly charts and Folium maps
- **Data Processing**: Pandas for analytics
- **Architecture**: Mirrors the Power BI structure for Fabric deployment

---

*This demo represents the local preview of the full Microsoft Fabric solution with Power BI Direct Lake mode and real-time data processing.* 