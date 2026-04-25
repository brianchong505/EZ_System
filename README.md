# EZ_System
Z.AI: Intelligent SME Resilience
System
AI-Driven Decision Support for Small-to-Medium Enterprises
Flutter FastAPI MySQL ILMU AI (GLM-5.1)
Project Overview
Developed for UMHackathon 2026, Z.AI is a sophisticated decision-support tool
designed to empower SME owners. It bridges the gap between raw operational data
and strategic growth by utilizing the ILMU GLM-5.1 engine to provide real-time
inventory alerts, sales forecasting, and comprehensive CEO-level strategic reports.
System Architecture
Frontend (UI Tier): Built with Flutter, featuring real-time state management,
interactive performance charts, and intuitive inventory controls.
Backend (Logic Tier): Powered by FastAPI, managing data orchestration,
prompt engineering for ILMU AI, and secure database transactions.
Data Tier (Persistence): A structured MySQL database designed for high
integrity, tracking products, sales trends, and AI-generated insights.
Key Features
1. AI Business Intelligence
Utilizes the Integrity Shield logic to filter raw sales data and generate strategic
actions. Includes 30-day demand forecasting and trade-off analysis.
• 
• 
• 
2. CEO Strategic Report
A global analysis module that synthesizes store-wide performance into an executive
summary, calculating a unique Financial Health Score (0-100) based on margins and
stock stability.
3. Dynamic Inventory Management
Real-time stock tracking with visual low-stock indicators and seamless
synchronization between mobile updates and the centralized database.
Installation & Setup
# Backend Setup
- Clone the repository
git clone https://github.com/your-repo/z-ai-sme.git
- Install dependencies
pip install fastapi uvicorn sqlalchemy pymysql requests numpy
- Run the server
uvicorn main:app --reload --host 0.0.0.0
# Frontend Setup
- Navigate to flutter project
cd z_ai_app
- Fetch dependencies
flutter pub get
- Run the app
flutter run

# Monitoring & Evaluation (M&E)
• Margin Accuracy - Ensures AI suggestions maintain profitability above the break-even floor.
• Parsing Success - Monitors the integrity of AI JSON responses during database insertion.
• Parsing Success - Automatic 60-second pulse checks between FastAPI and MySQL Data Tier

# Technical Dependencies
• ILMU GLM-5.1: Primary reasoning service.
• SQLAlchemy: Database ORM and orchestration.
• Shared Preferences: Local session management for user_id persistence.
