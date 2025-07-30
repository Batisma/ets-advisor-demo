import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import folium
from streamlit_folium import folium_static
import numpy as np
from datetime import datetime, timedelta
import os
from PIL import Image

# Page config
st.set_page_config(
    page_title="ETS Impact Advisor",
    page_icon="assets/ets_advisor_logo.png",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Custom CSS for styling
st.markdown("""
<style>
    .main-header {
        text-align: center;
        margin-bottom: 2rem;
    }
    .logo-container {
        display: flex;
        justify-content: center;
        align-items: center;
        margin-bottom: 1rem;
    }
    .page-logo {
        display: flex;
        align-items: center;
        margin-bottom: 1rem;
    }
    .page-logo img {
        margin-right: 1rem;
    }
    .metric-card {
        background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
        padding: 1rem;
        border-radius: 10px;
        color: white;
        text-align: center;
        margin: 0.5rem;
    }
    .metric-value {
        font-size: 2.5rem;
        font-weight: bold;
    }
    .metric-label {
        font-size: 1rem;
        opacity: 0.9;
    }
    .page-header {
        font-size: 2.5rem;
        font-weight: bold;
        color: #2c5aa0;
        margin-bottom: 1rem;
        display: flex;
        align-items: center;
    }
</style>
""", unsafe_allow_html=True)

# Logo display functions
def display_main_logo():
    """Display the main logo header"""
    try:
        # Try Magellan logo first
        logo = Image.open("Logo Mag.webp")
        col1, col2, col3 = st.columns([1, 2, 1])
        with col2:
            st.image(logo, width=200)
    except FileNotFoundError:
        try:
            # Fallback to ETS logo
            logo = Image.open("assets/ets_advisor_logo.png")
            col1, col2, col3 = st.columns([1, 2, 1])
            with col2:
                st.image(logo, width=400)
        except FileNotFoundError:
            st.markdown('<div class="main-header">🚛 ETS Impact Advisor</div>', unsafe_allow_html=True)

def display_page_logo(page_title):
    """Display the page logo with centered title"""
    try:
        # Try Magellan logo first
        logo = Image.open("Logo Mag.webp")
        # Center the logo
        col1, col2, col3 = st.columns([1, 1, 1])
        with col2:
            st.image(logo, width=120)
        # Center the title
        st.markdown(f'<div style="text-align: center; font-size: 2.5rem; font-weight: bold; color: #2c5aa0; margin: 1rem 0;">{page_title}</div>', unsafe_allow_html=True)
    except FileNotFoundError:
        try:
            # Fallback to ETS logo
            logo = Image.open("assets/ets_advisor_logo.png")
            # Center the logo
            col1, col2, col3 = st.columns([1, 1, 1])
            with col2:
                st.image(logo, width=150)
            # Center the title
            st.markdown(f'<div style="text-align: center; font-size: 2.5rem; font-weight: bold; color: #2c5aa0; margin: 1rem 0;">{page_title}</div>', unsafe_allow_html=True)
        except FileNotFoundError:
            st.markdown(f'<div style="text-align: center; font-size: 2.5rem; font-weight: bold; color: #2c5aa0; margin: 1rem 0;">{page_title}</div>', unsafe_allow_html=True)

# Data loading function
@st.cache_data
def load_data():
    """Load all CSV files"""
    data = {}
    
    # Load trip facts
    if os.path.exists('lakehouse/tripfacts_sample.csv'):
        data['trips'] = pd.read_csv('lakehouse/tripfacts_sample.csv')
        data['trips']['StartTimeUTC'] = pd.to_datetime(data['trips']['StartTimeUTC'])
        data['trips']['EndTimeUTC'] = pd.to_datetime(data['trips']['EndTimeUTC'])
        # Create TripDate from StartTimeUTC for analysis
        data['trips']['TripDate'] = data['trips']['StartTimeUTC'].dt.date
    
    # Load fuel invoices
    if os.path.exists('lakehouse/fuel_invoices_sample.csv'):
        data['fuel'] = pd.read_csv('lakehouse/fuel_invoices_sample.csv')
        data['fuel']['InvoiceMonth'] = pd.to_datetime(data['fuel']['InvoiceMonth'])
    
    # Load ETS price curve
    if os.path.exists('lakehouse/price_curve_sample.csv'):
        data['price_curve'] = pd.read_csv('lakehouse/price_curve_sample.csv')
        data['price_curve']['PriceMonth'] = pd.to_datetime(data['price_curve']['PriceMonth'])
    
    # Load scenarios
    if os.path.exists('lakehouse/scenario_sample.csv'):
        data['scenarios'] = pd.read_csv('lakehouse/scenario_sample.csv')
    
    return data

# Calculate key metrics
def calculate_metrics(data):
    """Calculate key fleet metrics"""
    if 'trips' not in data:
        return {}
        
    trips = data['trips']
    
    metrics = {
        'fleet_size': trips['VIN'].nunique(),
        'total_trips': len(trips),
        'total_distance': trips['Distance_km'].sum(),
        'total_fuel': trips['Fuel_l'].sum(),
        'co2_emissions': trips['Fuel_l'].sum() * 2.68 / 1000,  # tons
        'fleet_efficiency': (trips['Fuel_l'].sum() / trips['Distance_km'].sum()) * 100,
        'avg_trip_distance': trips['Distance_km'].mean(),
        'avg_trip_duration': (trips['EndTimeUTC'] - trips['StartTimeUTC']).dt.total_seconds().mean() / 3600
    }
    
    return metrics

# Main app
def main():
    # Load data
    data = load_data()
    
    if not data:
        st.error("⚠️ No data found. Please ensure CSV files are in the lakehouse/ directory.")
        return
    
    # Sidebar navigation
    st.sidebar.title("Navigation")
    page = st.sidebar.selectbox(
        "Select Page",
        ["Fleet Management Overview", "ETS Cost Simulator", "Scenario Cockpit", "Compliance Centre"]
    )
    
    # Calculate metrics
    metrics = calculate_metrics(data)
    
    # Display selected page
    if page == "Fleet Management Overview":
        show_fleet_digital_twin(data, metrics)
    elif page == "ETS Cost Simulator":
        show_ets_cost_simulator(data, metrics)
    elif page == "Scenario Cockpit":
        show_scenario_cockpit(data, metrics)
    elif page == "Compliance Centre":
        show_compliance_centre(data, metrics)

def show_fleet_digital_twin(data, metrics):
    """Fleet Management Overview page"""
    display_page_logo("Fleet Management Overview")
    
    # Key metrics row
    col1, col2, col3, col4 = st.columns(4)
    
    with col1:
        st.metric("Fleet Size", f"{metrics.get('fleet_size', 0)}", "vehicles")
    
    with col2:
        st.metric("Total Trips", f"{metrics.get('total_trips', 0):,}", "trips")
    
    with col3:
        st.metric("CO₂ Emissions", f"{metrics.get('co2_emissions', 0):.1f}", "tons")
    
    with col4:
        current_ets_price = 85.0  # Default price
        ets_cost = metrics.get('co2_emissions', 0) * current_ets_price
        st.metric("ETS Cost", f"€{ets_cost:,.0f}", "EUR")
    
    # Charts row
    col1, col2 = st.columns(2)
    
    with col1:
        st.subheader("🗺️ Fleet Activity Map")
        if 'trips' in data and len(data['trips']) > 0:
            # Create map
            trips_sample = data['trips'].sample(min(500, len(data['trips'])))  # Sample for performance
            center_lat = trips_sample['StartLat'].mean()
            center_lon = trips_sample['StartLon'].mean()
            
            m = folium.Map(location=[center_lat, center_lon], zoom_start=6)
            
            # Add markers
            for _, row in trips_sample.iterrows():
                folium.CircleMarker(
                    location=[row['StartLat'], row['StartLon']],
                    radius=3,
                    popup=f"VIN: {row['VIN'][:8]}...<br>Distance: {row['Distance_km']:.1f} km",
                    color='blue',
                    fill=True
                ).add_to(m)
            
            folium_static(m, width=500, height=400)
        else:
            st.info("No trip data available for map visualization")
    
    with col2:
        st.subheader("📈 Daily Trip Volume")
        if 'trips' in data:
            daily_trips = data['trips'].groupby(data['trips']['TripDate']).size().reset_index()
            daily_trips.columns = ['Date', 'Trips']
            
            fig = px.line(daily_trips, x='Date', y='Trips', title='Daily Trip Count')
            fig.update_layout(height=400)
            st.plotly_chart(fig, use_container_width=True)
    
    # Fleet performance table
    st.subheader("🚚 Fleet Performance Summary")
    if 'trips' in data:
        fleet_summary = data['trips'].groupby('VIN').agg({
            'TripID': 'count',
            'Distance_km': 'sum',
            'Fuel_l': 'sum'
        }).reset_index()
        
        fleet_summary.columns = ['VIN', 'Total Trips', 'Total Distance (km)', 'Total Fuel (L)']
        fleet_summary['Efficiency (L/100km)'] = (fleet_summary['Total Fuel (L)'] / fleet_summary['Total Distance (km)']) * 100
        fleet_summary = fleet_summary.round(2)
        
        st.dataframe(fleet_summary.head(10), use_container_width=True)

def show_ets_cost_simulator(data, metrics):
    """ETS Cost Simulator page"""
    display_page_logo("ETS Cost Simulator")
    
    # ETS Price Slider
    st.subheader("🎚️ ETS Price Simulation")
    ets_price = st.slider(
        "ETS Price (€/ton CO₂)",
        min_value=50,
        max_value=150,
        value=85,
        step=5,
        help="Adjust the ETS price to see impact on fleet costs"
    )
    
    # Calculate costs
    co2_emissions = metrics.get('co2_emissions', 0)
    ets_cost = co2_emissions * ets_price
    
    # Metrics row
    col1, col2, col3 = st.columns(3)
    
    with col1:
        st.metric("Current ETS Price", f"€{ets_price}", "per ton CO₂")
    
    with col2:
        st.metric("Total CO₂ Emissions", f"{co2_emissions:.1f}", "tons")
    
    with col3:
        st.metric("Total ETS Cost", f"€{ets_cost:,.0f}", "EUR")
    
    # Cost sensitivity analysis
    st.subheader("📊 Cost Sensitivity Analysis")
    
    # Generate price scenarios
    price_scenarios = list(range(50, 151, 10))
    cost_scenarios = [co2_emissions * price for price in price_scenarios]
    
    sensitivity_df = pd.DataFrame({
        'ETS Price (€/ton)': price_scenarios,
        'Total Cost (€)': cost_scenarios
    })
    
    fig = px.line(sensitivity_df, x='ETS Price (€/ton)', y='Total Cost (€)', 
                  title='ETS Cost Sensitivity Analysis')
    fig.add_vline(x=ets_price, line_dash="dash", line_color="red", 
                  annotation_text=f"Current: €{ets_price}")
    fig.update_layout(height=400)
    st.plotly_chart(fig, use_container_width=True)
    
    # Monthly cost breakdown
    if 'trips' in data:
        st.subheader("📅 Monthly ETS Cost Breakdown")
        monthly_data = data['trips'].groupby(pd.to_datetime(data['trips']['TripDate']).dt.to_period('M')).agg({
            'Fuel_l': 'sum'
        }).reset_index()
        
        monthly_data['Month'] = monthly_data['TripDate'].astype(str)
        monthly_data['CO2_tons'] = monthly_data['Fuel_l'] * 2.68 / 1000
        monthly_data['ETS_Cost'] = monthly_data['CO2_tons'] * ets_price
        
        fig = px.bar(monthly_data, x='Month', y='ETS_Cost', 
                     title=f'Monthly ETS Costs at €{ets_price}/ton')
        fig.update_layout(height=400)
        st.plotly_chart(fig, use_container_width=True)

def show_scenario_cockpit(data, metrics):
    """Enhanced Scenario Cockpit page with detailed explanations"""
    display_page_logo("Scenario Cockpit")
    
    # Introduction section
    st.markdown("""
    ### 🎯 What are Fleet Upgrade Scenarios?
    
    Fleet upgrade scenarios help you understand the **financial impact** of different strategies for modernizing your truck fleet. 
    Each scenario shows you the costs, savings, and timeline for upgrading to more efficient, lower-emission vehicles.
    
    **Why upgrade your fleet?**
    - ✅ **Reduce ETS costs** by lowering CO₂ emissions
    - ✅ **Save on fuel** with more efficient vehicles  
    - ✅ **Meet regulations** and avoid penalties
    - ✅ **Improve brand image** with greener operations
    """)
    
    # Key terms explanation
    with st.expander("📚 Key Terms Explained (Click to expand)"):
        st.markdown("""
        **💰 CapEx (Capital Expenditure)**: Money spent upfront to buy new trucks  
        **🔧 OpEx (Operational Expenditure)**: Ongoing costs like fuel, maintenance, insurance  
        **📈 NPV (Net Present Value)**: Total profit/loss over 5 years in today's money  
        **📊 ROI (Return on Investment)**: How much profit you make for every euro invested  
        **⏱️ Payback Period**: How long until the investment pays for itself  
        **🏭 ETS (Emissions Trading System)**: EU system requiring payment for CO₂ emissions  
        **🌱 CO₂ Reduction**: How much less pollution your new fleet will produce  
        """)
    
    if 'scenarios' in data and len(data['scenarios']) > 0:
        st.markdown("---")
        
        # Scenario overview cards
        st.subheader("🚛 Available Fleet Upgrade Strategies")
        
        scenarios = data['scenarios']
        scenario_descriptions = {
            "Conservative Replacement": {
                "icon": "🐢",
                "description": "**Low-risk approach**: Replace only the oldest, least efficient trucks",
                "pros": ["Lower upfront cost", "Minimal operational disruption", "Proven technology"],
                "cons": ["Slower emission reductions", "Limited cost savings", "Longer payback period"],
                "best_for": "Companies wanting to test the waters with fleet modernization"
            },
            "Aggressive Electrification": {
                "icon": "⚡",
                "description": "**High-impact approach**: Replace most trucks with electric or hybrid vehicles",
                "pros": ["Maximum emission reduction", "Highest long-term savings", "Strong environmental impact"],
                "cons": ["High upfront investment", "Requires charging infrastructure", "Technology risk"],
                "best_for": "Forward-thinking companies with available capital and environmental commitments"
            },
            "Hybrid Approach": {
                "icon": "⚖️",
                "description": "**Balanced approach**: Mix of efficient diesel and some electric vehicles",
                "pros": ["Balanced risk/reward", "Moderate investment", "Flexible implementation"],
                "cons": ["Moderate emission reductions", "Compromise on savings", "Complex fleet management"],
                "best_for": "Companies seeking a balanced modernization strategy"
            }
        }
        
        # Display scenario cards
        for _, scenario in scenarios.iterrows():
            name = scenario['ScenarioName']
            desc_info = scenario_descriptions.get(name, {"icon": "🚛", "description": "Fleet upgrade scenario"})
            
            with st.container():
                st.markdown(f"""
                <div style="border: 2px solid #f0f0f0; border-radius: 10px; padding: 20px; margin: 10px 0; background: linear-gradient(135deg, #f8f9fa 0%, #e9ecef 100%);">
                    <h4>{desc_info['icon']} {name}</h4>
                    <p style="font-size: 16px; margin-bottom: 15px;">{desc_info['description']}</p>
                    
                    <div style="display: flex; justify-content: space-between; margin-bottom: 15px;">
                        <div style="text-align: center;">
                            <strong style="color: #28a745;">€{scenario['NPV_EUR']:,.0f}</strong><br>
                            <small>5-Year Profit/Loss</small>
                        </div>
                        <div style="text-align: center;">
                            <strong style="color: #17a2b8;">{scenario['VehiclesReplaced']}</strong><br>
                            <small>Trucks Replaced</small>
                        </div>
                        <div style="text-align: center;">
                            <strong style="color: #ffc107;">{scenario['Payback_Years']:.1f} years</strong><br>
                            <small>Payback Time</small>
                        </div>
                        <div style="text-align: center;">
                            <strong style="color: #6f42c1;">{scenario['CO2_Reduction_t']:.0f}t</strong><br>
                            <small>CO₂ Saved (5 years)</small>
                        </div>
                    </div>
                </div>
                """, unsafe_allow_html=True)
        
        st.markdown("---")
        
        # Scenario selection with enhanced description
        st.subheader("🔍 Detailed Scenario Analysis")
        scenario_names = scenarios['ScenarioName'].tolist()
        selected_scenario = st.selectbox(
            "Choose a scenario to explore in detail:",
            scenario_names,
            help="Select a scenario to see detailed financial projections and implementation timeline"
        )
        
        # Get selected scenario data
        scenario_data = scenarios[scenarios['ScenarioName'] == selected_scenario].iloc[0]
        desc_info = scenario_descriptions.get(selected_scenario, {})
        
        # Enhanced scenario details
        st.markdown(f"### {desc_info.get('icon', '🚛')} {selected_scenario} - Detailed Analysis")
        
        # Key metrics with explanations
        col1, col2, col3, col4 = st.columns(4)
        
        with col1:
            npv_color = "green" if scenario_data['NPV_EUR'] > 0 else "red"
            st.markdown(f"""
            <div style="text-align: center; padding: 15px; border-radius: 10px; background: #f8f9fa;">
                <h3 style="color: {npv_color}; margin: 0;">€{scenario_data['NPV_EUR']:,.0f}</h3>
                <p style="margin: 5px 0; font-weight: bold;">Net Present Value</p>
                <small>Total profit/loss over 5 years</small>
            </div>
            """, unsafe_allow_html=True)
        
        with col2:
            roi_color = "green" if scenario_data['ROI_Percent'] > 0 else "red"
            st.markdown(f"""
            <div style="text-align: center; padding: 15px; border-radius: 10px; background: #f8f9fa;">
                <h3 style="color: {roi_color}; margin: 0;">{scenario_data['ROI_Percent']:.1f}%</h3>
                <p style="margin: 5px 0; font-weight: bold;">Return on Investment</p>
                <small>Profit per euro invested</small>
            </div>
            """, unsafe_allow_html=True)
        
        with col3:
            st.markdown(f"""
            <div style="text-align: center; padding: 15px; border-radius: 10px; background: #f8f9fa;">
                <h3 style="color: #ffc107; margin: 0;">{scenario_data['Payback_Years']:.1f}</h3>
                <p style="margin: 5px 0; font-weight: bold;">Payback Period</p>
                <small>Years to recover investment</small>
            </div>
            """, unsafe_allow_html=True)
        
        with col4:
            st.markdown(f"""
            <div style="text-align: center; padding: 15px; border-radius: 10px; background: #f8f9fa;">
                <h3 style="color: #28a745; margin: 0;">{scenario_data['CO2_Reduction_t']:,.0f}t</h3>
                <p style="margin: 5px 0; font-weight: bold;">CO₂ Reduction</p>
                <small>Emissions saved over 5 years</small>
            </div>
            """, unsafe_allow_html=True)
        
        # Investment breakdown
        st.subheader("💰 Investment Breakdown")
        
        col1, col2 = st.columns(2)
        
        with col1:
            # Cost breakdown pie chart
            cost_data = {
                'Initial Investment': scenario_data['CapEx_EUR'],
                'Operational Savings': -scenario_data['OpEx_EUR'],
                'ETS Cost Savings': -scenario_data['ETS_Savings_EUR']
            }
            
            fig_costs = go.Figure(data=[go.Pie(
                labels=list(cost_data.keys()),
                values=[abs(v) for v in cost_data.values()],
                hole=0.4,
                marker=dict(colors=['#ff6b6b', '#4ecdc4', '#45b7d1'])
            )])
            
            fig_costs.update_layout(
                title="5-Year Investment Breakdown",
                annotations=[dict(text=f'Net: €{scenario_data["NPV_EUR"]:,.0f}', x=0.5, y=0.5, font_size=16, showarrow=False)]
            )
            
            st.plotly_chart(fig_costs, use_container_width=True)
        
        with col2:
            # Timeline visualization
            years = list(range(1, 6))
            annual_savings = (scenario_data['OpEx_EUR'] + scenario_data['ETS_Savings_EUR']) / 5
            cumulative_savings = [annual_savings * year - scenario_data['CapEx_EUR'] for year in years]
            
            fig_timeline = go.Figure()
            fig_timeline.add_trace(go.Scatter(
                x=years,
                y=cumulative_savings,
                mode='lines+markers',
                name='Cumulative Cash Flow',
                line=dict(color='#45b7d1', width=3),
                marker=dict(size=8)
            ))
            
            fig_timeline.add_hline(y=0, line_dash="dash", line_color="red", annotation_text="Break-even")
            fig_timeline.update_layout(
                title="Cash Flow Over Time",
                xaxis_title="Year",
                yaxis_title="Cumulative Cash Flow (€)",
                height=400
            )
            
            st.plotly_chart(fig_timeline, use_container_width=True)
        
        # Pros and cons
        if desc_info:
            col1, col2 = st.columns(2)
            
            with col1:
                st.markdown("### ✅ Advantages")
                for pro in desc_info.get('pros', []):
                    st.markdown(f"• {pro}")
            
            with col2:
                st.markdown("### ⚠️ Considerations")
                for con in desc_info.get('cons', []):
                    st.markdown(f"• {con}")
            
            st.info(f"**Best for:** {desc_info.get('best_for', 'General fleet modernization')}")
        
        # Scenario comparison
        st.markdown("---")
        st.subheader("📊 Compare All Scenarios")
        
        # Multi-metric comparison chart
        fig_comparison = go.Figure()
        
        # NPV bars
        colors = ['#ff6b6b' if npv < 0 else '#4ecdc4' for npv in scenarios['NPV_EUR']]
        fig_comparison.add_trace(go.Bar(
            name='Net Present Value (€)',
            x=scenarios['ScenarioName'],
            y=scenarios['NPV_EUR'],
            marker_color=colors,
            text=[f"€{npv:,.0f}" for npv in scenarios['NPV_EUR']],
            textposition='outside'
        ))
        
        fig_comparison.update_layout(
            title='Financial Performance Comparison (5-Year NPV)',
            xaxis_title='Scenario',
            yaxis_title='Net Present Value (€)',
            height=500,
            showlegend=False
        )
        
        st.plotly_chart(fig_comparison, use_container_width=True)
        
        # Risk/Reward matrix
        st.subheader("🎯 Risk vs. Reward Analysis")
        
        # Create risk scores based on investment size and payback period
        scenarios['Risk_Score'] = (scenarios['CapEx_EUR'] / 1000000) + (scenarios['Payback_Years'] / 10)
        scenarios['Reward_Score'] = abs(scenarios['NPV_EUR'] / 1000000) + (scenarios['CO2_Reduction_t'] / 1000)
        
        fig_risk = px.scatter(
            scenarios,
            x='Risk_Score',
            y='Reward_Score',
            size='VehiclesReplaced',
            color='ScenarioName',
            hover_data=['NPV_EUR', 'Payback_Years', 'CO2_Reduction_t'],
            title='Risk vs. Reward Matrix',
            labels={'Risk_Score': 'Risk Level →', 'Reward_Score': 'Potential Reward →'}
        )
        
        fig_risk.update_layout(height=500)
        st.plotly_chart(fig_risk, use_container_width=True)
        
        st.markdown("""
        **How to read this chart:**
        - **Top-left**: Low risk, high reward (ideal scenarios)
        - **Top-right**: High risk, high reward (aggressive strategies)
        - **Bottom-left**: Low risk, low reward (conservative approaches)
        - **Bottom-right**: High risk, low reward (avoid these)
        """)
        
        # Implementation roadmap
        st.subheader("🗺️ Implementation Roadmap")
        
        roadmap_data = {
            "Conservative Replacement": [
                "Month 1-3: Identify oldest vehicles for replacement",
                "Month 4-6: Secure financing and select suppliers",
                "Month 7-12: Gradual vehicle replacement",
                "Month 13-24: Monitor performance and optimize",
                "Month 25-60: Continue operations and evaluate next phase"
            ],
            "Aggressive Electrification": [
                "Month 1-6: Infrastructure planning and charging station installation",
                "Month 4-12: Large-scale vehicle procurement and delivery",
                "Month 7-18: Staff training and operational adaptation",
                "Month 13-36: Performance monitoring and optimization",
                "Month 25-60: Full operation and continuous improvement"
            ],
            "Hybrid Approach": [
                "Month 1-3: Fleet analysis and vehicle selection strategy",
                "Month 4-9: Phased procurement of mixed vehicle types",
                "Month 7-15: Gradual deployment and staff training",
                "Month 13-30: Performance monitoring and route optimization",
                "Month 25-60: Optimized mixed-fleet operations"
            ]
        }
        
        if selected_scenario in roadmap_data:
            st.markdown(f"**{selected_scenario} - Key Milestones:**")
            for milestone in roadmap_data[selected_scenario]:
                st.markdown(f"• {milestone}")
        
    else:
        st.info("No scenario data available")

def show_compliance_centre(data, metrics):
    """Compliance Centre page"""
    display_page_logo("Compliance Centre")
    
    # Compliance metrics
    col1, col2, col3 = st.columns(3)
    
    with col1:
        fleet_efficiency = metrics.get('fleet_efficiency', 0)
        st.metric("Fleet Efficiency", f"{fleet_efficiency:.1f}", "L/100km")
    
    with col2:
        st.metric("Compliance Status", "✅ Compliant", "Current")
    
    with col3:
        st.metric("Next Reporting", "Q1 2025", "deadline")
    
    # ETS price tracking
    if 'price_curve' in data:
        st.subheader("💹 ETS Price Tracking")
        
        fig = px.line(data['price_curve'], x='PriceMonth', y='EUR_per_t', 
                      title='ETS Price Evolution')
        fig.update_layout(height=400)
        st.plotly_chart(fig, use_container_width=True)
    
    # Compliance summary table
    st.subheader("📊 Compliance Summary")
    
    if 'price_curve' in data:
        compliance_data = data['price_curve'].copy()
        compliance_data['CO2_Emissions_t'] = metrics.get('co2_emissions', 0)
        compliance_data['ETS_Cost_EUR'] = compliance_data['EUR_per_t'] * compliance_data['CO2_Emissions_t']
        
        # Select only needed columns for display
        display_data = compliance_data[['PriceMonth', 'EUR_per_t', 'CO2_Emissions_t', 'ETS_Cost_EUR']].copy()
        display_data['PriceMonth'] = display_data['PriceMonth'].dt.strftime('%Y-%m')
        display_data['EUR_per_t'] = display_data['EUR_per_t'].apply(lambda x: f"€{x:.2f}")
        display_data['CO2_Emissions_t'] = display_data['CO2_Emissions_t'].apply(lambda x: f"{x:.1f}")
        display_data['ETS_Cost_EUR'] = display_data['ETS_Cost_EUR'].apply(lambda x: f"€{x:,.0f}")
        
        display_data.columns = ['Month', 'ETS Price (€/t)', 'CO₂ Emissions (t)', 'ETS Cost (€)']
        
        st.dataframe(display_data.head(10), use_container_width=True)
    
    # Action buttons
    st.subheader("🔄 Actions")
    
    col1, col2, col3 = st.columns(3)
    
    with col1:
        if st.button("📄 Generate Report"):
            st.success("✅ Compliance report generated successfully!")
    
    with col2:
        if st.button("📧 Send Alert"):
            st.success("✅ Alert sent to fleet managers!")
    
    with col3:
        if st.button("🛒 Generate Purchase Order"):
            st.success("✅ Purchase order generated for carbon credits!")

if __name__ == "__main__":
    main() 