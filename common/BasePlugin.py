import oracledb
import logging
import re
from abc import ABC, abstractmethod
from typing import Dict, Optional, List, Tuple
from .VaultClient import VaultClient
from .Constants import SqlProcessing


class BasePlugin(ABC):
    
    def __init__(self, product_code: str, vault_config):
        self.product_code = product_code
        self.vault = VaultClient(vault_config)
        self._db_conn = None
        self.sql_queries = []
    
    def get_sftp_config(self, env: str = 'dev'):
        raise NotImplementedError(
            f"{self.__class__.__name__} must implement get_sftp_config() if SFTP mode is required"
        )
    
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
    
    def process_value_for_sql(self, value) -> Tuple[bool, any]:
        if not isinstance(value, str):
            return (False, value)
        
        value_stripped = value.strip()
        value_upper = value_stripped.upper()
        
        if value_upper in SqlProcessing.SQL_KEYWORDS:
            return (True, value_upper)
        
        for pattern, oracle_format in SqlProcessing.DATE_PATTERNS:
            if re.match(pattern, value_stripped):
                return (True, f"TO_DATE('{value_stripped}', '{oracle_format}')")
        
        return (False, value)
    
    def execute_query(self, sql: str, params: Optional[tuple] = None, fetch_one: bool = False):
        if sql.strip().upper().startswith(('INSERT', 'UPDATE')):
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
    
    def _insert(self, table: str, data: Dict):
        fields = []
        values = []
        placeholders = []
        
        param_num = 1
        for key, value in data.items():
            if key != '_table':
                is_sql_func, processed_value = self.process_value_for_sql(value)
                fields.append(key)
                if is_sql_func:
                    placeholders.append(processed_value)
                else:
                    placeholders.append(f':{param_num}')
                    values.append(processed_value)
                    param_num += 1
        
        sql = f"INSERT INTO {table} ({', '.join(fields)}) VALUES ({', '.join(placeholders)})"
        self.execute_query(sql, values)
    
    def _update(self, table: str, pk_fields: Dict, data: Dict, changes: Dict):
        set_parts = []
        values = []
        
        param_num = 1
        for field in changes.keys():
            if field in data:
                is_sql_func, processed_value = self.process_value_for_sql(data[field])
                if is_sql_func:
                    set_parts.append(f"{field} = {processed_value}")
                else:
                    set_parts.append(f"{field} = :{param_num}")
                    values.append(processed_value)
                    param_num += 1
        
        where_parts = []
        for field, value in pk_fields.items():
            where_parts.append(f"{field} = :{param_num}")
            values.append(value)
            param_num += 1
        
        where_clause = ' AND '.join(where_parts)
        sql = f"UPDATE {table} SET {', '.join(set_parts)} WHERE {where_clause}"
        self.execute_query(sql, values)
    
    def _process_entity(
        self,
        table: str,
        prefix: str,
        pk_fields: List[str],
        row: Dict,
        operation: str,
        override: bool,
        entity_name: str = None
    ) -> str:
        from .Constants import Operation, ProcessStatus
        
        data = self.extract_table_data(row, prefix)
        data['_table'] = table
        
        for pk_field in pk_fields:
            if not data.get(pk_field):
                raise ValueError(f"{prefix}.{pk_field} is required")
        
        pk_dict = {pk_field: data[pk_field] for pk_field in pk_fields}
        pk_values = [data[pk_field] for pk_field in pk_fields]
        pk_display = '/'.join(pk_values)
        
        if entity_name is None:
            entity_name = prefix.capitalize()
        
        if operation == Operation.INSERT:
            existing = self.fetch_current_record(table, pk_dict)
            if existing:
                logging.warning(f"{entity_name} {pk_display} already exists, skipping INSERT")
                return ProcessStatus.SKIPPED
            
            self._insert(table, data)
            logging.info(f"Inserted {entity_name.lower()}: {pk_display}")
            return ProcessStatus.INSERTED
            
        elif operation == Operation.UPDATE:
            current = self.fetch_current_record(table, pk_dict)
            if not current:
                raise ValueError(f"{entity_name} {pk_display} does not exist. Use INSERT operation.")
            
            all_fields = list(data.keys())
            changes = self.detect_changes(current, data, all_fields)
            
            if not changes:
                logging.info(f"No changes detected for {entity_name.lower()} {pk_display}")
                return ProcessStatus.SKIPPED
            
            self.validate_mutability(changes, override)
            self._update(table, pk_dict, data, changes)
            logging.info(f"Updated {entity_name.lower()}: {pk_display}, fields: {list(changes.keys())}")
            return ProcessStatus.UPDATED
    
    def get_sql_queries(self) -> List[Dict]:
        return self.sql_queries
    
    def reset_sql_queries(self):
        self.sql_queries = []
        
    @abstractmethod
    def get_mutable_fields(self, table: str) -> List[str]:
        raise NotImplementedError("Subclass must implement get_mutable_fields()")
    
    @abstractmethod
    def process_row(self, row: Dict, metadata: Dict):
        raise NotImplementedError("Subclass must implement process_row()")
