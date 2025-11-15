# Quick Start: MongoDB Setup for Windows 11

## 🚀 Quick Installation Steps

### 1. Install MongoDB Community Server

**Download and Install:**
1. Go to: https://www.mongodb.com/try/download/community
2. Select: Windows → MSI → Download
3. Run installer → Choose "Complete" → Install MongoDB as Service → Install Compass
4. Click "Install"

### 2. Verify MongoDB is Running

Open PowerShell and run:
```powershell
net start MongoDB
```

Or check in Services:
- Press `Win + R` → type `services.msc` → Look for "MongoDB" → Should be "Running"

### 3. Install Python Dependencies

```powershell
# Activate virtual environment (if using)
.\.venv\Scripts\Activate.ps1

# Install pymongo
pip install pymongo>=4.6.0

# Or install all requirements
pip install -r requirements.txt
```

### 4. Test MongoDB Connection

```powershell
python test_mongodb_connection.py
```

If successful, you'll see: `✓ All tests passed! MongoDB is ready to use.`

### 5. Migrate Existing Data (Optional)

If you have SQLite data to migrate:
```powershell
python migrate_to_mongodb.py
```

### 6. Open MongoDB Compass

1. Open MongoDB Compass (installed with MongoDB)
2. Connect to: `mongodb://localhost:27017`
3. Navigate to: `news_scraper` database → `pipelines_overview` collection

## 📝 Configuration (Optional)

Create a `.env` file or set environment variables:

```env
MONGO_HOST=localhost
MONGO_PORT=27017
MONGO_DB_NAME=news_scraper
MONGO_COLLECTION_NAME=pipelines_overview
```

## ✅ Verify Everything Works

1. **Test Connection:**
   ```powershell
   python test_mongodb_connection.py
   ```

2. **Start API:**
   ```powershell
   python run_api.py
   ```

3. **Check MongoDB Compass:**
   - Connect to `mongodb://localhost:27017`
   - View data in `news_scraper.pipelines_overview`

## 🔧 Troubleshooting

### MongoDB won't start
```powershell
# Check if port is in use
netstat -ano | findstr :27017

# Restart service
net stop MongoDB
net start MongoDB
```

### Connection refused
- Make sure MongoDB service is running
- Check firewall allows port 27017
- Verify connection: `mongosh mongodb://localhost:27017`

### Python can't connect
- Install pymongo: `pip install pymongo`
- Test: `python test_mongodb_connection.py`

## 📚 More Information

See `MONGODB_SETUP.md` for detailed setup instructions.

