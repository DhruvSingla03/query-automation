#!/usr/bin/env python3
import sys
from pathlib import Path

project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

import os
import logging
from dotenv import load_dotenv

load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format='[%(asctime)s] [%(levelname)s] %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)

def test_vault_and_db():
    try:
        from products.fastagacq.FastagAcqConfig import VaultConfig, Product
        from common.VaultClient import VaultClient
        import oracledb
        
        env = os.getenv('ENV', 'dev')
        logging.info(f"Testing connection for environment: {env}")
        logging.info(f"Product: {Product.NAME}")
        
        logging.info("Step 1: Testing Vault connection...")
        vault = VaultClient(VaultConfig)
        logging.info(f"✓ Connected to Vault: {vault.vault_url}")
        
        logging.info("Step 2: Fetching database credentials...")
        creds = vault.get_db_credentials()
        logging.info(f"✓ Retrieved credentials for host: {creds['host']}")
        
        logging.info("Step 3: Testing database connection...")
        dsn = oracledb.makedsn(
            creds['host'],
            creds['port'],
            service_name=creds['database']
        )
        
        conn = oracledb.connect(
            user=creds['username'],
            password=creds['password'],
            dsn=dsn
        )
        
        cursor = conn.cursor()
        cursor.execute("SELECT 'Connection successful!' FROM DUAL")
        result = cursor.fetchone()
        cursor.close()
        conn.close()
        
        logging.info(f"✓ Database connection successful: {result[0]}")
        logging.info("")
        logging.info("=" * 50)
        logging.info("ALL TESTS PASSED ✓")
        logging.info("=" * 50)
        return True
        
    except Exception as e:
        logging.error(f"✗ Test failed: {e}")
        import traceback
        logging.error(traceback.format_exc())
        return False

if __name__ == '__main__':
    success = test_vault_and_db()
    sys.exit(0 if success else 1)
