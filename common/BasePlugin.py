import oracledb
import logging
from typing import Dict, Optional, List
from .VaultClient import VaultClient


class BasePlugin:
    
    def __init__(self, product_code: str, vault_config):
        self.product_code = product_code
        self.vault = VaultClient(vault_config)
        self._db_conn = None
        self.sql_queries = []
    
    def get_db_connection(self):
        if self._db_conn is None:
            creds = self.vault.get_db_credentials()
            
            dsn = oracledb.makedsn(
                creds['host'],
                creds.get('port', 1521),
                service_name=creds['database']
            )
            
            self._db_conn = oracledb.connect(
                user=creds['username'],
                password=creds['password'],
                dsn=dsn
            )
        return self._db_conn
    
    def begin_transaction(self):
        conn = self.get_db_connection()
        conn.autocommit = False
        logging.debug("Transaction started")
    
    def commit_transaction(self):
        conn = self.get_db_connection()
        conn.commit()
        logging.debug("Transaction committed")
    
    def rollback_transaction(self):
        conn = self.get_db_connection()
        conn.rollback()
        logging.debug("Transaction rolled back")
    
    def execute_query(self, sql: str, params: Optional[tuple] = None, fetch_one: bool = False):
        self.sql_queries.append({
            'sql': sql,
            'params': params or []
        })
        
        conn = self.get_db_connection()
        cursor = conn.cursor()
        try:
            cursor.execute(sql, params or [])
            if fetch_one:
                columns = [col[0].lower() for col in cursor.description]
                row = cursor.fetchone()
                if row:
                    return dict(zip(columns, row))
                return None
            else:
                return None
        finally:
            cursor.close()
    
    def close_connection(self):
        if self._db_conn:
            self._db_conn.close()
    
    def fetch_current_record(self, table: str, key_fields: dict) -> Optional[Dict]:
        where_parts = []
        values = []
        param_num = 1
        
        for field, value in key_fields.items():
            where_parts.append(f"{field} = :{param_num}")
            values.append(value)
            param_num += 1
        
        where_clause = ' AND '.join(where_parts)
        sql = f"SELECT * FROM {table} WHERE {where_clause}"
        return self.execute_query(sql, values, fetch_one=True)
    
    def detect_changes(self, current: Dict, incoming: Dict, fields: List[str]) -> Dict:
        changes = {}
        mutable_fields = self.get_mutable_fields(incoming.get('_table', ''))
        
        for field in fields:
            incoming_val = incoming.get(field)
            current_val = current.get(field)
            
            if incoming_val is None or incoming_val == '':
                continue
            
            incoming_str = str(incoming_val).strip()
            current_str = str(current_val).strip() if current_val is not None else ''
            
            if incoming_str != current_str:
                changes[field] = {
                    'old': current_val,
                    'new': incoming_val,
                    'mutable': field in mutable_fields
                }
        
        return changes
    
    def validate_mutability(self, changes: Dict, override: bool):
        if override:
            return
        
        immutable_changes = []
        for field, change_info in changes.items():
            if not change_info['mutable']:
                immutable_changes.append(field)
        
        if immutable_changes:
            raise ValueError(
                f"Cannot update immutable fields without override=true: {', '.join(immutable_changes)}"
            )
    
    def extract_table_data(self, row: Dict, prefix: str) -> Dict:
        table_data = {}
        prefix_dot = f"{prefix}."
        
        for key, value in row.items():
            if key.startswith(prefix_dot):
                field_name = key.replace(prefix_dot, '', 1)
                if value and value.strip():
                    table_data[field_name] = value.strip()
        
        return table_data
    
    def has_table_data(self, row: Dict, prefix: str) -> bool:
        table_data = self.extract_table_data(row, prefix)
        return len(table_data) > 0
    
    def get_mutable_fields(self, table: str) -> List[str]:
        raise NotImplementedError("Subclass must implement get_mutable_fields()")
    
    def process_row(self, row: Dict, metadata: Dict):
        raise NotImplementedError("Subclass must implement process_row()")
    
    def get_sql_queries(self) -> List[Dict]:
        return self.sql_queries
    
    def reset_sql_queries(self):
        self.sql_queries = []
