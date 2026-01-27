import hvac
import os
import logging
import threading
import time
from typing import Dict


class VaultClient:
    
    def __init__(self):
        if os.getenv('ENV', 'dev').lower() == 'dev':
            self.dev_mode = True
            logging.info("Running in DEV_MODE - using direct DB credentials from .env")
            return
        
        self.dev_mode = False
        self.vault_addr = os.getenv('VAULT_ADDR')
        self.role_id = os.getenv('VAULT_ROLE_ID')
        self.secret_id = os.getenv('VAULT_SECRET_ID')
        
        if not self.vault_addr or not self.role_id or not self.secret_id:
            raise ValueError(
                "VAULT_ADDR, VAULT_ROLE_ID, and VAULT_SECRET_ID must be set"
            )
        
        self.client = hvac.Client(url=self.vault_addr)
        self._authenticate()
        self._start_token_renewal()
    
    def _authenticate(self):
        try:
            auth_response = self.client.auth.approle.login(
                role_id=self.role_id,
                secret_id=self.secret_id
            )
            self.client.token = auth_response['auth']['client_token']
            self.token_lease_duration = auth_response['auth']['lease_duration']
            
            if not self.client.is_authenticated():
                raise Exception("Failed to authenticate with Vault AppRole")
                
        except Exception as e:
            raise Exception(f"Vault authentication failed: {e}")
    
    def _start_token_renewal(self):
        def renew_token():
            while True:
                sleep_time = self.token_lease_duration * 0.8
                time.sleep(sleep_time)
                
                try:
                    renew_response = self.client.auth.token.renew_self()
                    self.token_lease_duration = renew_response['auth']['lease_duration']
                    print(f"Vault token renewed. New TTL: {self.token_lease_duration}s")
                except Exception as e:
                    print(f"Token renewal failed, re-authenticating: {e}")
                    self._authenticate()
        
        renewal_thread = threading.Thread(target=renew_token, daemon=True)
        renewal_thread.start()
    
    def get_db_credentials(self, product_code: str) -> Dict:
        
        if self.dev_mode:
            return {
                'host': os.getenv('DB_HOST', 'localhost'),
                'port': int(os.getenv('DB_PORT', 3306)),
                'database': os.getenv('DB_DATABASE'),
                'username': os.getenv('DB_USERNAME'),
                'password': os.getenv('DB_PASSWORD')
            }
        
        path_env = f'VAULT_PATH_{product_code.upper()}'
        vault_path = os.getenv(path_env)
        
        if not vault_path:
            raise ValueError(
                f"Vault path not configured for product {product_code}. "
                f"Expected environment variable: {path_env}"
            )
        
        try:
            secret = self.client.secrets.kv.v2.read_secret_version(path=vault_path)
            return secret['data']['data']
        except Exception as e:
            raise Exception(f"Failed to fetch credentials for {product_code}: {e}")
    
    def get_audit_db_credentials(self) -> Dict:
        
        if self.dev_mode:
            return {
                'host': os.getenv('DB_HOST', 'localhost'),
                'port': int(os.getenv('DB_PORT', 3306)),
                'database': os.getenv('DB_DATABASE'),
                'username': os.getenv('DB_USERNAME'),
                'password': os.getenv('DB_PASSWORD')
            }
        
        audit_path = os.getenv('VAULT_PATH_AUDIT', 'secret/data/audit/database')
        
        try:
            secret = self.client.secrets.kv.v2.read_secret_version(path=audit_path)
            return secret['data']['data']
        except Exception as e:
            raise Exception(f"Failed to fetch audit DB credentials: {e}")
