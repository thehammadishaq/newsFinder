# MongoDB Setup Guide for Windows 11

This guide will help you set up MongoDB locally on Windows 11 and configure it for the News Scraper application.

## Step 1: Install MongoDB Community Server

### Option A: Using MongoDB Installer (Recommended)

1. **Download MongoDB Community Server:**
   - Visit: https://www.mongodb.com/try/download/community
   - Select:
     - Version: Latest (e.g., 7.0)
     - Platform: Windows
     - Package: MSI
   - Click "Download"

2. **Run the Installer:**
   - Double-click the downloaded `.msi` file
   - Choose "Complete" installation
   - Check "Install MongoDB as a Service"
   - Select "Run service as Network Service user"
   - Check "Install MongoDB Compass" (GUI tool)
   - Click "Install"

3. **Verify Installation:**
   - Open Command Prompt or PowerShell
   - Run: `mongod --version`
   - You should see MongoDB version information

### Option B: Using Chocolatey (If you have it)

```powershell
choco install mongodb
```

## Step 2: Start MongoDB Service

MongoDB should start automatically as a Windows service. To verify:

1. **Check Service Status:**
   - Press `Win + R`, type `services.msc`, press Enter
   - Look for "MongoDB" service
   - Status should be "Running"

2. **Start MongoDB Manually (if needed):**
   ```powershell
   net start MongoDB
   ```

3. **Stop MongoDB (if needed):**
   ```powershell
   net stop MongoDB
   ```

## Step 3: Install MongoDB Compass (GUI)

If you didn't install Compass during MongoDB installation:

1. **Download MongoDB Compass:**
   - Visit: https://www.mongodb.com/try/download/compass
   - Download the Windows installer
   - Run the installer and follow the setup wizard

2. **Connect to Local MongoDB:**
   - Open MongoDB Compass
   - Connection string: `mongodb://localhost:27017`
   - Click "Connect"

## Step 4: Configure the Application

### Environment Variables (Optional)

You can set these environment variables to customize MongoDB connection:

```powershell
# Set MongoDB host (default: localhost)
$env:MONGO_HOST = "localhost"

# Set MongoDB port (default: 27017)
$env:MONGO_PORT = "27017"

# Set database name (default: news_scraper)
$env:MONGO_DB_NAME = "news_scraper"

# Set collection name (default: pipelines_overview)
$env:MONGO_COLLECTION_NAME = "pipelines_overview"
```

### Or create a `.env` file in the project root:

```env
MONGO_HOST=localhost
MONGO_PORT=27017
MONGO_DB_NAME=news_scraper
MONGO_COLLECTION_NAME=pipelines_overview
```

## Step 5: Install Python Dependencies

```powershell
# Activate virtual environment (if using one)
.\.venv\Scripts\Activate.ps1

# Install dependencies
pip install -r requirements.txt
```

## Step 6: Switch from SQLite to MongoDB

The application now uses MongoDB by default. To switch:

1. **Update imports in your code:**
   - Replace `from overview_store import ...` with `from mongodb_store import ...`
   - Or keep using `overview_store` if you want to use SQLite

2. **For API services:**
   - The `api/services.py` file needs to import from `mongodb_store` instead of `overview_store`

## Step 7: Test MongoDB Connection

### Using Python:

```python
from mongodb_store import init_db, upsert_overview, export_csv

# Initialize database
init_db()

# Test insert/update
upsert_overview("example.com", {
    "Overall pipelines Status": "Success",
    "Raw Articles scraped": "10"
})

# Export to CSV
export_csv()
```

### Using MongoDB Compass:

1. Open MongoDB Compass
2. Connect to `mongodb://localhost:27017`
3. Navigate to `news_scraper` database
4. Open `pipelines_overview` collection
5. You should see your data

## Troubleshooting

### MongoDB won't start

1. **Check if port 27017 is in use:**
   ```powershell
   netstat -ano | findstr :27017
   ```

2. **Check MongoDB logs:**
   - Default log location: `C:\Program Files\MongoDB\Server\<version>\log\mongod.log`

3. **Restart MongoDB service:**
   ```powershell
   net stop MongoDB
   net start MongoDB
   ```

### Connection refused errors

1. **Verify MongoDB is running:**
   ```powershell
   net start MongoDB
   ```

2. **Test connection:**
   ```powershell
   mongosh mongodb://localhost:27017
   ```

3. **Check firewall settings:**
   - MongoDB uses port 27017
   - Make sure Windows Firewall allows it

### Python can't connect

1. **Verify pymongo is installed:**
   ```powershell
   pip show pymongo
   ```

2. **Test connection:**
   ```python
   from pymongo import MongoClient
   client = MongoClient("mongodb://localhost:27017")
   client.admin.command('ping')
   print("Connected successfully!")
   ```

## MongoDB Compass Features

MongoDB Compass provides a GUI to:

- **Browse Collections:** View all documents in your collections
- **Query Data:** Write MongoDB queries visually
- **Edit Documents:** Update, delete, or create documents
- **View Indexes:** See database indexes
- **Performance Metrics:** Monitor database performance
- **Schema Analysis:** Understand your data structure

## Default Connection Details

- **Host:** localhost
- **Port:** 27017
- **Database:** news_scraper
- **Collection:** pipelines_overview

## Migration from SQLite

If you have existing SQLite data:

1. **Export SQLite data to CSV:**
   ```python
   from overview_store import export_csv
   export_csv("pipelines_overview.csv")
   ```

2. **Import to MongoDB:**
   ```python
   import csv
   from mongodb_store import upsert_overview
   
   with open("pipelines_overview.csv", "r", encoding="utf-8") as f:
       reader = csv.DictReader(f)
       for row in reader:
           domain = row["Domain (sources)"]
           updates = {k: v for k, v in row.items() if k != "Domain (sources)"}
           upsert_overview(domain, updates)
   ```

## Next Steps

1. Start MongoDB service
2. Install Python dependencies: `pip install -r requirements.txt`
3. Update code to use `mongodb_store` instead of `overview_store`
4. Run your application
5. Open MongoDB Compass to view your data

For more information, visit: https://www.mongodb.com/docs/manual/

