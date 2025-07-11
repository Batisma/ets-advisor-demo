#!/bin/bash

# ETS Impact Advisor Demo Startup Script
echo "🚀 Starting ETS Impact Advisor Demo..."
echo "======================================"

# Check if Streamlit is installed
if ! command -v streamlit &> /dev/null; then
    echo "❌ Streamlit not found. Installing..."
    pip3 install --break-system-packages streamlit plotly folium streamlit-folium
fi

# Set environment variables to avoid email prompt
export STREAMLIT_BROWSER_GATHER_USAGE_STATS=false
export STREAMLIT_GLOBAL_SHOW_ERROR_DETAILS=false

# Kill any existing streamlit processes
pkill -f streamlit 2>/dev/null || true

# Start Streamlit
echo "📊 Starting dashboard on http://localhost:8501"
echo "👉 Open the URL in your browser to view the demo"
echo "💡 Use Ctrl+C to stop the demo"
echo ""

streamlit run app.py --server.port=8501 --server.headless=false --browser.gatherUsageStats=false 