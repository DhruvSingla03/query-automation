#!/usr/bin/env python3
"""Test Oracle database connection"""

import os
import oracledb
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

def test_connection():
    """Test Oracle database connection"""
    try:
        print("=" * 60)
        print("Testing Oracle Database Connection")
        print("=" * 60)
        
        # Get credentials from .env
        host = os.getenv('DB_HOST')
        port = int(os.getenv('DB_PORT', 1521))
        database = os.getenv('DB_DATABASE')
        username = os.getenv('DB_USERNAME')
        password = os.getenv('DB_PASSWORD')
        
        print(f"\nConnection Details:")
        print(f"  Host: {host}")
        print(f"  Port: {port}")
        print(f"  Database/Service: {database}")
        print(f"  Username: {username}")
        print(f"  Password: {'*' * len(password) if password else 'NOT SET'}")
        
        # Build DSN
        print(f"\nCreating DSN...")
        dsn = oracledb.makedsn(host, port, service_name=database)
        print(f"  DSN: {dsn}")
        
        # Connect
        print(f"\nConnecting to Oracle...")
        conn = oracledb.connect(
            user=username,
            password=password,
            dsn=dsn
        )
        
        print("✅ Connection successful!")
        
        # Test query
        print(f"\nTesting query...")
        cursor = conn.cursor()
        
        # Get Oracle version
        cursor.execute("SELECT * FROM v$version WHERE ROWNUM = 1")
        version = cursor.fetchone()
        print(f"  Oracle Version: {version[0]}")
        
        # Get current timestamp
        cursor.execute("SELECT SYSDATE FROM dual")
        timestamp = cursor.fetchone()
        print(f"  Current Database Time: {timestamp[0]}")
        
        # Test a sample query (check if plaza table exists)
        print(f"\nChecking tables...")
        try:
            cursor.execute("SELECT COUNT(*) FROM NETCACQ_PLAZA_DTLS")
            count = cursor.fetchone()
            print(f"  ✅ NETCACQ_PLAZA_DTLS table exists: {count[0]} rows")
        except Exception as e:
            print(f"  ⚠️  NETCACQ_PLAZA_DTLS table check: {str(e)}")
        
        cursor.close()
        conn.close()
        
        print("\n" + "=" * 60)
        print("✅ All connection tests passed!")
        print("=" * 60)
        
    except oracledb.DatabaseError as e:
        error, = e.args
        print(f"\n❌ Database Error:")
        print(f"  Code: {error.code}")
        print(f"  Message: {error.message}")
        print(f"\nCommon issues:")
        print(f"  - Check host/port in .env")
        print(f"  - Verify username/password")
        print(f"  - Ensure Oracle listener is running")
        print(f"  - Check network connectivity")
        
    except Exception as e:
        print(f"\n❌ Error: {str(e)}")
        print(f"\nMake sure:")
        print(f"  - .env file exists with correct values")
        print(f"  - oracledb package is installed: pip install oracledb")


if __name__ == '__main__':
    test_connection()
