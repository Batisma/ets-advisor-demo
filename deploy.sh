#!/bin/bash

# ETS Impact Advisor Demo Deployment Script
# This script helps deploy the demo to Microsoft Fabric

set -e

echo "🚀 ETS Impact Advisor Demo Deployment"
echo "======================================"

# Check if Azure CLI is installed
if ! command -v az &> /dev/null; then
    echo "❌ Azure CLI not found. Please install Azure CLI first."
    echo "   https://docs.microsoft.com/en-us/cli/azure/install-azure-cli"
    exit 1
fi

# Check if Python is installed
if ! command -v python3 &> /dev/null; then
    echo "❌ Python 3 not found. Please install Python 3 first."
    exit 1
fi

# Check if required Python packages are installed
echo "🔍 Checking Python dependencies..."
python3 -c "import pandas, numpy, faker" 2>/dev/null || {
    echo "📦 Installing Python dependencies..."
    pip3 install pandas numpy faker
}

# Generate sample data if not exists
if [ ! -f "lakehouse/tripfacts_sample.csv" ]; then
    echo "📊 Generating sample data..."
    python3 scripts/generate_sample_data.py --output-dir lakehouse/
else
    echo "✅ Sample data already exists"
fi

# Login to Azure
echo "🔐 Logging into Azure..."
az login

# Get subscription info
SUBSCRIPTION_ID=$(az account show --query id --output tsv)
echo "📋 Using subscription: $SUBSCRIPTION_ID"

# Set default variables
RESOURCE_GROUP=${FABRIC_RESOURCE_GROUP:-"rg-ets-advisor-demo"}
LOCATION=${FABRIC_LOCATION:-"westeurope"}
WORKSPACE_NAME=${FABRIC_WORKSPACE_NAME:-"ets-advisor-demo"}

echo "🏗️  Deployment Configuration:"
echo "   Resource Group: $RESOURCE_GROUP"
echo "   Location: $LOCATION"
echo "   Workspace: $WORKSPACE_NAME"

# Create resource group if it doesn't exist
echo "🏢 Creating resource group..."
az group create --name $RESOURCE_GROUP --location $LOCATION

# Deploy infrastructure
echo "🏗️  Deploying infrastructure..."
az deployment group create \
    --resource-group $RESOURCE_GROUP \
    --template-file infra/fabric_workspace.bicep \
    --parameters workspaceName=$WORKSPACE_NAME \
    --parameters location=$LOCATION

# Get deployment outputs
echo "📄 Getting deployment outputs..."
STORAGE_ACCOUNT=$(az deployment group show --resource-group $RESOURCE_GROUP --name fabric_workspace --query properties.outputs.storageAccountName.value --output tsv)
LAKEHOUSE_NAME=$(az deployment group show --resource-group $RESOURCE_GROUP --name fabric_workspace --query properties.outputs.lakehouseName.value --output tsv)

echo "✅ Infrastructure deployed successfully!"
echo "   Storage Account: $STORAGE_ACCOUNT"
echo "   Lakehouse: $LAKEHOUSE_NAME"

# Upload sample data
echo "📤 Uploading sample data to storage..."
az storage blob upload-batch \
    --account-name $STORAGE_ACCOUNT \
    --destination "lakehouse/Files" \
    --source "lakehouse" \
    --pattern "*.csv"

echo "🎉 Deployment completed successfully!"
echo ""
echo "Next steps:"
echo "1. Open Microsoft Fabric workspace: $WORKSPACE_NAME"
echo "2. Import the Power BI project from powerbi/ETS_Advisor.pbip"
echo "3. Configure the lakehouse connection in Power BI"
echo "4. Run the lakehouse/create_lakehouse.sql script"
echo "5. Load the CSV files into Delta tables"
echo ""
echo "For detailed instructions, see README.md" 