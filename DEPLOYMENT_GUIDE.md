# 🚀 Deployment Guide: ETS Impact Advisor Demo

## 📋 Overview

This guide will help you deploy the ETS Impact Advisor demo to GitHub and make it accessible online for your director and team.

## 🎯 Deployment Options

1. **✅ Streamlit Cloud** (Recommended - Free & Easy)
2. **GitHub Codespaces** (Cloud development environment)
3. **Local sharing** (Via ngrok or similar)

---

## 🔥 Option 1: Streamlit Cloud (Recommended)

### **Step 1: Push to GitHub**

1. **Create a new repository on GitHub:**
   - Go to [github.com](https://github.com) and sign in
   - Click "New repository" (green button)
   - Repository name: `ets-advisor-demo` 
   - Description: `Fleet ETS Impact Advisory Demo - Real-time analytics & compliance management`
   - Make it **Public** (required for free Streamlit Cloud)
   - **Don't** initialize with README (we already have files)
   - Click "Create repository"

2. **Push your local code:**
   ```bash
   # Add the GitHub remote (replace YOUR_USERNAME)
   git remote add origin https://github.com/YOUR_USERNAME/ets-advisor-demo.git
   
   # Push to GitHub
   git branch -M main
   git push -u origin main
   ```

### **Step 2: Deploy to Streamlit Cloud**

1. **Go to [share.streamlit.io](https://share.streamlit.io)**

2. **Sign in with GitHub account**

3. **Click "New app"**

4. **Configure deployment:**
   - **Repository:** `YOUR_USERNAME/ets-advisor-demo`
   - **Branch:** `main`
   - **Main file path:** `app.py`
   - **App URL:** Choose a custom URL like `ets-advisor-demo`

5. **Click "Deploy!"**

6. **Wait 2-3 minutes** for deployment to complete

### **Step 3: Share with Your Director**

**🎉 Your demo will be live at:**
```
https://YOUR_APP_NAME.streamlit.app
```

**Sample sharing message:**
```
Hi [Director Name],

I've built a comprehensive ETS Impact Advisor demo showcasing our fleet analytics capabilities.

🌐 Live Demo: https://ets-advisor-demo.streamlit.app
📊 Features: Fleet Digital Twin, ETS Cost Simulator, Scenario Planning, Compliance Centre
⏱️ Duration: <10 minutes for full demo

The demo includes:
- Real fleet data (40 vehicles, 88k+ trips)
- Interactive European route maps  
- Dynamic ETS cost modeling (€6.3M baseline)
- 3 electrification scenarios with NPV/ROI analysis

Please explore and share your feedback!

Best regards,
[Your Name]
```

---

## ⚡ Option 2: GitHub Codespaces (Instant Demo)

### **Step 1: Push to GitHub** (same as above)

### **Step 2: Enable Codespaces**
1. **Go to your GitHub repository**
2. **Click the green "Code" button**
3. **Select "Codespaces" tab**
4. **Click "Create codespace on main"**

### **Step 3: Run Demo in Codespace**
```bash
# Install dependencies (automatic with requirements.txt)
pip install -r requirements.txt

# Run the demo
streamlit run app.py
```

### **Step 4: Access Demo**
- Codespace will show a "Open in Browser" popup
- Demo will be accessible at a GitHub-hosted URL
- Share this URL with your director

---

## 🏠 Option 3: Local Sharing (Quick Test)

### **If you want to quickly share your local demo:**

1. **Install ngrok:**
   ```bash
   # macOS
   brew install ngrok
   
   # Or download from https://ngrok.com
   ```

2. **Run your local demo:**
   ```bash
   streamlit run app.py --server.port 8501
   ```

3. **In another terminal, create public tunnel:**
   ```bash
   ngrok http 8501
   ```

4. **Share the ngrok URL** (e.g., `https://abc123.ngrok.io`)

**⚠️ Note:** ngrok URLs expire when you close the session.

---

## ✅ Verification Checklist

After deployment, verify these features work:

### **Page Navigation:**
- [ ] Fleet Digital Twin loads with metrics
- [ ] ETS Cost Simulator slider works
- [ ] Scenario Cockpit shows 3 scenarios  
- [ ] Compliance Centre displays data

### **Interactive Elements:**
- [ ] European map displays trip markers
- [ ] ETS price slider updates costs in real-time
- [ ] Scenario selector changes NPV/ROI values
- [ ] Charts and tables load properly

### **Performance:**
- [ ] Initial load time < 10 seconds
- [ ] Navigation between pages is smooth
- [ ] No error messages in console

---

## 🎬 Demo Presentation Tips

### **For Your Director:**

1. **Start with overview:** "This demo represents our Microsoft Fabric capabilities"

2. **Navigate systematically:**
   - Fleet Digital Twin (fleet overview)
   - ETS Cost Simulator (cost impact) 
   - Scenario Cockpit (investment planning)
   - Compliance Centre (regulatory management)

3. **Highlight key insights:**
   - "74,067 tons CO₂ = €6.3M annual exposure"
   - "€2.6M cost increase if ETS price rises to €120/ton"
   - "Aggressive electrification delivers €8.5M NPV"

4. **Technical capabilities:**
   - "Real-time data processing (88k+ records)"
   - "Interactive analytics and what-if scenarios"
   - "Direct Lake semantic model architecture"

---

## 🔧 Troubleshooting

### **Common Issues:**

**❌ Streamlit Cloud deployment fails:**
- Check that `requirements.txt` is in root directory
- Ensure `app.py` is the main file
- Verify all dependencies are listed

**❌ Demo loads but shows errors:**
- Check that all CSV files are in `lakehouse/` folder
- Verify logo file exists at `assets/ets_advisor_logo.png`
- Run `python verify_demo.py` to check data integrity

**❌ Slow loading:**
- Streamlit Cloud free tier has limited resources
- Consider upgrading to Streamlit Cloud Pro
- Or use GitHub Codespaces for better performance

### **Support:**

- **GitHub Issues:** [Repository Issues](https://github.com/YOUR_USERNAME/ets-advisor-demo/issues)
- **Streamlit Docs:** [docs.streamlit.io](https://docs.streamlit.io)
- **Community Forum:** [discuss.streamlit.io](https://discuss.streamlit.io)

---

## 🎉 Success Criteria

**You'll know the deployment is successful when:**

✅ **Director can access the demo via a URL**  
✅ **All 4 pages load without errors**  
✅ **Interactive elements respond properly**  
✅ **Demo completes in <10 minutes**  
✅ **Data visualizations are clear and professional**

---

**🚀 Ready to deploy? Start with Option 1 (Streamlit Cloud) for the best results!** 