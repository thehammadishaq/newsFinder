"""
Test MongoDB connection script
Run this to verify MongoDB is set up correctly
"""
import sys

def test_mongodb():
    """Test MongoDB connection and basic operations"""
    print("Testing MongoDB Connection...")
    print("=" * 50)
    
    # Test 1: Import pymongo
    print("\n1. Testing pymongo import...")
    try:
        from pymongo import MongoClient
        print("   ✓ pymongo imported successfully")
    except ImportError:
        print("   ✗ pymongo not installed")
        print("   Run: pip install pymongo")
        return False
    
    # Test 2: Connect to MongoDB
    print("\n2. Testing MongoDB connection...")
    try:
        client = MongoClient(
            "mongodb://localhost:27017",
            serverSelectionTimeoutMS=5000
        )
        client.admin.command('ping')
        print("   ✓ Connected to MongoDB successfully")
    except Exception as e:
        print(f"   ✗ Connection failed: {e}")
        print("\n   Troubleshooting:")
        print("   - Make sure MongoDB is installed")
        print("   - Start MongoDB service: net start MongoDB")
        print("   - Check if MongoDB is running on localhost:27017")
        return False
    
    # Test 3: Test database and collection
    print("\n3. Testing database and collection...")
    try:
        db = client["news_scraper"]
        collection = db["pipelines_overview"]
        print("   ✓ Database and collection accessed")
    except Exception as e:
        print(f"   ✗ Error accessing database: {e}")
        client.close()
        return False
    
    # Test 4: Test insert/read
    print("\n4. Testing insert and read operations...")
    try:
        test_doc = {
            "Domain (sources)": "test.example.com",
            "Overall pipelines Status": "Test",
            "updated_at": "2024-01-01T00:00:00"
        }
        
        # Insert test document
        collection.insert_one(test_doc)
        print("   ✓ Insert operation successful")
        
        # Read test document
        result = collection.find_one({"Domain (sources)": "test.example.com"})
        if result:
            print("   ✓ Read operation successful")
        else:
            print("   ✗ Read operation failed")
            client.close()
            return False
        
        # Delete test document
        collection.delete_one({"Domain (sources)": "test.example.com"})
        print("   ✓ Delete operation successful")
        
    except Exception as e:
        print(f"   ✗ Error in operations: {e}")
        client.close()
        return False
    
    # Test 5: Test mongodb_store module
    print("\n5. Testing mongodb_store module...")
    try:
        from mongodb_store import init_db, upsert_overview, export_csv
        print("   ✓ mongodb_store imported successfully")
        
        # Initialize database
        init_db()
        print("   ✓ Database initialized")
        
        # Test upsert
        upsert_overview("test.example.com", {
            "Overall pipelines Status": "Test",
            "Raw Articles scraped": "5"
        })
        print("   ✓ Upsert operation successful")
        
        # Clean up test data
        collection.delete_one({"Domain (sources)": "test.example.com"})
        print("   ✓ Test data cleaned up")
        
    except ImportError as e:
        print(f"   ✗ mongodb_store not found: {e}")
        print("   Make sure mongodb_store.py exists in the project root")
        client.close()
        return False
    except Exception as e:
        print(f"   ✗ Error in mongodb_store: {e}")
        client.close()
        return False
    
    client.close()
    
    print("\n" + "=" * 50)
    print("✓ All tests passed! MongoDB is ready to use.")
    print("\nNext steps:")
    print("  1. Run: python migrate_to_mongodb.py (if you have SQLite data)")
    print("  2. Update your code to use mongodb_store")
    print("  3. Open MongoDB Compass to view your data")
    return True


if __name__ == "__main__":
    success = test_mongodb()
    sys.exit(0 if success else 1)

