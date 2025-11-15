"""
Migration script to switch from SQLite to MongoDB
Run this script to migrate existing data from SQLite to MongoDB
"""
import os
import sys
import csv
from pathlib import Path

# Try to import both stores
try:
    from overview_store import export_csv as sqlite_export_csv
    from mongodb_store import init_db, upsert_overview
    print("✓ Both SQLite and MongoDB stores imported successfully")
except ImportError as e:
    print(f"✗ Error importing stores: {e}")
    print("Make sure both overview_store.py and mongodb_store.py exist")
    sys.exit(1)


def migrate_data():
    """Migrate data from SQLite to MongoDB"""
    print("\n=== Migrating from SQLite to MongoDB ===\n")
    
    # Step 1: Export SQLite data to CSV
    print("Step 1: Exporting SQLite data to CSV...")
    csv_path = "pipelines_overview_migration.csv"
    try:
        sqlite_export_csv(csv_path)
        print(f"✓ Exported SQLite data to {csv_path}")
    except Exception as e:
        print(f"✗ Error exporting SQLite data: {e}")
        return False
    
    # Step 2: Initialize MongoDB
    print("\nStep 2: Initializing MongoDB...")
    try:
        init_db()
        print("✓ MongoDB initialized successfully")
    except Exception as e:
        print(f"✗ Error initializing MongoDB: {e}")
        print("Make sure MongoDB is running on localhost:27017")
        return False
    
    # Step 3: Import CSV data to MongoDB
    print("\nStep 3: Importing data to MongoDB...")
    if not os.path.exists(csv_path):
        print(f"✗ CSV file not found: {csv_path}")
        return False
    
    imported_count = 0
    error_count = 0
    
    try:
        with open(csv_path, "r", encoding="utf-8") as f:
            reader = csv.DictReader(f)
            for row in reader:
                try:
                    domain = row.get("Domain (sources)", "").strip()
                    if not domain:
                        continue
                    
                    # Prepare updates (exclude domain field)
                    updates = {k: v for k, v in row.items() if k != "Domain (sources)"}
                    
                    # Upsert to MongoDB
                    upsert_overview(domain, updates)
                    imported_count += 1
                    
                    if imported_count % 10 == 0:
                        print(f"  Imported {imported_count} records...")
                        
                except Exception as e:
                    error_count += 1
                    print(f"  ✗ Error importing {row.get('Domain (sources)', 'unknown')}: {e}")
        
        print(f"\n✓ Migration completed!")
        print(f"  - Imported: {imported_count} records")
        if error_count > 0:
            print(f"  - Errors: {error_count} records")
        
        # Clean up CSV file
        try:
            os.remove(csv_path)
            print(f"\n✓ Cleaned up temporary file: {csv_path}")
        except Exception:
            pass
        
        return True
        
    except Exception as e:
        print(f"✗ Error importing data: {e}")
        return False


if __name__ == "__main__":
    print("MongoDB Migration Script")
    print("=" * 50)
    
    # Check if MongoDB is available
    try:
        from pymongo import MongoClient
        client = MongoClient("mongodb://localhost:27017", serverSelectionTimeoutMS=2000)
        client.admin.command('ping')
        client.close()
        print("✓ MongoDB connection successful")
    except Exception as e:
        print(f"✗ MongoDB connection failed: {e}")
        print("\nPlease make sure:")
        print("  1. MongoDB is installed and running")
        print("  2. MongoDB service is started (net start MongoDB)")
        print("  3. MongoDB is accessible on localhost:27017")
        sys.exit(1)
    
    # Run migration
    success = migrate_data()
    
    if success:
        print("\n" + "=" * 50)
        print("Migration completed successfully!")
        print("\nNext steps:")
        print("  1. Update your code to use 'mongodb_store' instead of 'overview_store'")
        print("  2. Test your application")
        print("  3. Verify data in MongoDB Compass")
    else:
        print("\n" + "=" * 50)
        print("Migration failed. Please check the errors above.")
        sys.exit(1)

