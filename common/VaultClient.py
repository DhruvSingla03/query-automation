import hvac
import os
import logging
from typing import Dict


class VaultClient:
    
    def __init__(self, vault_config=None):
        if vault_config is None:
            raise ValueError("vault_config is required (product's VaultConfig class)")
        
        env = os.getenv('ENV', 'dev').lower()
        
        if env == 'dev':
            config = vault_config.DEV
        elif env in ('prod', 'production'):
            config = vault_config.PROD
        else:
            raise ValueError(f"Invalid ENV value: {env}. Must be 'dev' or 'prod'")
        
        self.vault_url = config['url']
        self.vault_token = config['token']
        self.secret_path = config['secret_path']
        
        if not self.vault_url or self.vault_url.startswith('<'):
            raise ValueError(f"Vault URL not configured for {env} environment in VaultConfig")
        
        if not self.vault_token or self.vault_token.startswith('<'):
            raise ValueError(f"Vault token not configured for {env} environment in VaultConfig")
        
        if not self.secret_path or self.secret_path.startswith('<'):
            raise ValueError(f"Secret path not configured for {env} environment in VaultConfig")
        
        try:
            self.client = hvac.Client(
                url=self.vault_url,
                token=self.vault_token,
                verify=False
            )
            
            if not self.client.is_authenticated():
                raise Exception("Failed to authenticate with Vault - invalid token")
            
            logging.info(f"Connected to Vault ({env}): {self.vault_url}")
            
        except Exception as e:
            raise Exception(f"Vault connection failed: {e}")
    
    def get_secret(self, path: str) -> Dict:
        try:
            response = self.client.secrets.kv.v2.read_secret_version(path=path)
            return response['data']['data']
        except Exception as e:
            raise Exception(f"Failed to read secret from path '{path}': {e}")
    
    def get_db_credentials(self) -> Dict:
        try:
            credentials = self.get_secret(self.secret_path)
            
            required_fields = ['host', 'username', 'password', 'database']
            missing = [f for f in required_fields if f not in credentials]
            if missing:
                raise ValueError(f"Missing required fields in Vault secret: {missing}")
            
            if 'port' in credentials:
                credentials['port'] = int(credentials['port'])
            else:
                credentials['port'] = 1521
            
            return credentials
            
        except Exception as e:
            raise Exception(f"Failed to fetch credentials from {self.secret_path}: {e}")
