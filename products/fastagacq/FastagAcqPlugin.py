import sys
from pathlib import Path
sys.path.append(str(Path(__file__).parent.parent.parent))

import os
import re
import logging
from typing import Dict, List
from .FastagAcqConfig import Product, Tables, FieldRules, VaultConfig
from common.BasePlugin import BasePlugin
from common.Constants import Operation, ProcessStatus

class FastagAcqPlugin(BasePlugin):
    
    def __init__(self):
        super().__init__(Product.CODE, VaultConfig)
        
        self.TABLE_PLAZA = Tables.PLAZA
        self.TABLE_CONCESSIONAIRE = Tables.CONCESSIONAIRE
        self.TABLE_LANE = Tables.LANE
        self.TABLE_FARE = Tables.FARE
        self.TABLE_VEHICLE_MAPPING = Tables.VEHICLE_MAPPING
        self.TABLE_USER_MAPPING = Tables.USER_MAPPING
        
        self.MUTABLE_FIELDS = FieldRules.MUTABLE
    
    def get_mutable_fields(self, table: str) -> List[str]:
        return self.MUTABLE_FIELDS.get(table, [])
    
    def validate_metadata(self, metadata: Dict):
        required = ['product', 'submitted_by', 'jira', 'operation']
        for field in required:
            if not metadata.get(field):
                raise ValueError(f"Missing required metadata field: meta.{field}")
        
        if not re.match(r'^APB-[0-9]+$', metadata['jira']):
            raise ValueError(
                f"Invalid jira format: {metadata['jira']}. "
                "Expected format: APB-XXXXXX"
            )
        
        if metadata['operation'] not in [Operation.INSERT, Operation.UPDATE]:
            raise ValueError(
                f"Invalid operation: {metadata['operation']}. Must be INSERT or UPDATE"
            )
        
        if os.getenv('ENV') == 'production':
            allowed_submitters = ['olm_id']
            if metadata['submitted_by'] not in allowed_submitters:
                raise ValueError(
                    f"Submitter '{metadata['submitted_by']}' not in allowlist"
                )
    
    def process_row(self, row: Dict, metadata: Dict):
        self.validate_metadata(metadata)
        
        operation = metadata['operation']
        override = metadata.get('override', 'false').lower() == 'true'
        
        logging.info(f"Processing row: jira={metadata['jira']}, operation={operation}, override={override}")
        
        if self.has_table_data(row, 'plaza'):
            plaza_type = self.extract_table_data(row, 'plaza').get('type', '').strip().lower()
            
            if plaza_type == 'parking':
                if not self.has_table_data(row, 'conc'):
                    raise ValueError("For parking plaza type, concessionaire data is mandatory")
                if not self.has_table_data(row, 'lane'):
                    raise ValueError("For parking plaza type, lane data is mandatory")
            elif plaza_type == 'toll':
                if not self.has_table_data(row, 'conc'):
                    raise ValueError("For toll plaza type, concessionaire data is mandatory")
                if not self.has_table_data(row, 'lane'):
                    raise ValueError("For toll plaza type, lane data is mandatory")
                if not self.has_table_data(row, 'fare'):
                    raise ValueError("For toll plaza type, fare data is mandatory")
                if not self.has_table_data(row, 'vmap'):
                    raise ValueError("For toll plaza type, vehicle mapping data is mandatory")
        
        results = {
            ProcessStatus.INSERTED: [],
            ProcessStatus.SKIPPED: [],
            ProcessStatus.UPDATED: []
        }
        
        if self.has_table_data(row, 'plaza'):
            status = self.process_plaza(row, metadata, override)
            results[status].append('plaza')
        
        if self.has_table_data(row, 'conc'):
            status = self.process_concessionaire(row, metadata, override)
            results[status].append('concessionaire')
        
        if self.has_table_data(row, 'lane'):
            status = self.process_lane(row, metadata, override)
            results[status].append('lane')
        
        if self.has_table_data(row, 'fare'):
            status = self.process_fare(row, metadata, override)
            results[status].append('fare')
        
        if self.has_table_data(row, 'vmap'):
            status = self.process_vehicle_mapping(row, metadata, override)
            results[status].append('vehicle_mapping')
        
        if self.has_table_data(row, 'umap'):
            status = self.process_user_mapping(row, metadata, override)
            results[status].append('user_mapping')
        
        if results[ProcessStatus.INSERTED]:
            logging.info(f"Tables inserted: {results[ProcessStatus.INSERTED]}")
        if results[ProcessStatus.SKIPPED]:
            logging.warning(f"Tables skipped (already exist): {results[ProcessStatus.SKIPPED]}")
        if results[ProcessStatus.UPDATED]:
            logging.info(f"Tables updated: {results[ProcessStatus.UPDATED]}")
    
    def process_plaza(self, row: Dict, metadata: Dict, override: bool) -> str:
        data = self.extract_table_data(row, 'plaza')
        data['_table'] = self.TABLE_PLAZA
        
        if not data.get('plaza_id'):
            raise ValueError("plaza.plaza_id is required")
        
        plaza_id = data['plaza_id']
        operation = metadata['operation']
        
        if operation == Operation.INSERT:
            existing = self.fetch_current_record(self.TABLE_PLAZA, {'plaza_id': plaza_id})
            if existing:
                logging.warning(f"Plaza {plaza_id} already exists, skipping INSERT")
                return ProcessStatus.SKIPPED
            
            self._insert_plaza(data)
            logging.info(f"Inserted plaza: {plaza_id}")
            return ProcessStatus.INSERTED
            
        elif operation == Operation.UPDATE:
            current = self.fetch_current_record(self.TABLE_PLAZA, {'plaza_id': plaza_id})
            if not current:
                raise ValueError(f"Plaza {plaza_id} does not exist. Use INSERT operation.")
            
            all_fields = list(data.keys())
            changes = self.detect_changes(current, data, all_fields)
            
            if not changes:
                logging.info(f"No changes detected for plaza {plaza_id}")
                return ProcessStatus.SKIPPED
            
            self.validate_mutability(changes, override)
            
            self._update_plaza(plaza_id, data, changes)
            logging.info(f"Updated plaza: {plaza_id}, fields: {list(changes.keys())}")
            return ProcessStatus.UPDATED
    
    def process_concessionaire(self, row: Dict, metadata: Dict, override: bool) -> str:
        data = self.extract_table_data(row, 'conc')
        data['_table'] = self.TABLE_CONCESSIONAIRE
        
        if not data.get('concessionaire_id'):
            raise ValueError("conc.concessionaire_id is required")
        
        conc_id = data['concessionaire_id']
        operation = metadata['operation']
        
        if operation == Operation.INSERT:
            existing = self.fetch_current_record(self.TABLE_CONCESSIONAIRE, {'concessionaire_id': conc_id})
            if existing:
                logging.warning(f"Concessionaire {conc_id} already exists, skipping INSERT")
                return ProcessStatus.SKIPPED
            
            self._insert_concessionaire(data)
            logging.info(f"Inserted concessionaire: {conc_id}")
            return ProcessStatus.INSERTED
            
        elif operation == Operation.UPDATE:
            current = self.fetch_current_record(self.TABLE_CONCESSIONAIRE, {'concessionaire_id': conc_id})
            if not current:
                raise ValueError(f"Concessionaire {conc_id} does not exist. Use INSERT operation.")
            
            all_fields = list(data.keys())
            changes = self.detect_changes(current, data, all_fields)
            
            if not changes:
                logging.info(f"No changes detected for concessionaire {conc_id}")
                return ProcessStatus.SKIPPED
            
            self.validate_mutability(changes, override)
            self._update_concessionaire(conc_id, data, changes)
            logging.info(f"Updated concessionaire: {conc_id}, fields: {list(changes.keys())}")
            return ProcessStatus.UPDATED
    
    def process_lane(self, row: Dict, metadata: Dict, override: bool) -> str:
        data = self.extract_table_data(row, 'lane')
        data['_table'] = self.TABLE_LANE
        
        if not data.get('plaza_id') or not data.get('lane_id'):
            raise ValueError("lane.plaza_id and lane.lane_id are required")
        
        plaza_id = data['plaza_id']
        lane_id = data['lane_id']
        operation = metadata['operation']
        
        if operation == Operation.INSERT:
            existing = self.fetch_current_record(self.TABLE_LANE, {'plaza_id': plaza_id, 'lane_id': lane_id})
            if existing:
                logging.warning(f"Lane {plaza_id}/{lane_id} already exists, skipping INSERT")
                return ProcessStatus.SKIPPED
            
            self._insert_lane(data)
            logging.info(f"Inserted lane: {plaza_id}/{lane_id}")
            return ProcessStatus.INSERTED
            
        elif operation == Operation.UPDATE:
            current = self.fetch_current_record(self.TABLE_LANE, {'plaza_id': plaza_id, 'lane_id': lane_id})
            if not current:
                raise ValueError(f"Lane {plaza_id}/{lane_id} does not exist. Use INSERT operation.")
            
            all_fields = list(data.keys())
            changes = self.detect_changes(current, data, all_fields)
            
            if not changes:
                logging.info(f"No changes detected for lane {plaza_id}/{lane_id}")
                return ProcessStatus.SKIPPED
            
            self.validate_mutability(changes, override)
            self._update_lane(plaza_id, lane_id, data, changes)
            logging.info(f"Updated lane: {plaza_id}/{lane_id}, fields: {list(changes.keys())}")
            return ProcessStatus.UPDATED
    
    def process_fare(self, row: Dict, metadata: Dict, override: bool) -> str:
        data = self.extract_table_data(row, 'fare')
        data['_table'] = self.TABLE_FARE
        
        if not data.get('fare_id'):
            raise ValueError("fare.fare_id is required")
        
        fare_id = data['fare_id']
        operation = metadata['operation']
        
        if operation == Operation.INSERT:
            existing = self.fetch_current_record(self.TABLE_FARE, {'fare_id': fare_id})
            if existing:
                logging.warning(f"Fare {fare_id} already exists, skipping INSERT")
                return ProcessStatus.SKIPPED
            
            self._insert_fare(data)
            logging.info(f"Inserted fare details: {fare_id}")
            return ProcessStatus.INSERTED
            
        elif operation == Operation.UPDATE:
            current = self.fetch_current_record(self.TABLE_FARE, {'fare_id': fare_id})
            if not current:
                raise ValueError(f"Fare {fare_id} does not exist. Use INSERT operation.")
            
            all_fields = list(data.keys())
            changes = self.detect_changes(current, data, all_fields)
            
            if not changes:
                logging.info(f"No changes detected for fare {fare_id}")
                return ProcessStatus.SKIPPED
            
            self.validate_mutability(changes, override)
            self._update_fare(fare_id, data, changes)
            logging.info(f"Updated fare: {fare_id}, fields: {list(changes.keys())}")
            return ProcessStatus.UPDATED
    
    def process_vehicle_mapping(self, row: Dict, metadata: Dict, override: bool) -> str:
        data = self.extract_table_data(row, 'vmap')
        data['_table'] = self.TABLE_VEHICLE_MAPPING
        
        if not data.get('plaza_id') or not data.get('mvc_id'):
            raise ValueError("vmap.plaza_id and vmap.mvc_id are required")
        
        plaza_id = data['plaza_id']
        mvc_id = data['mvc_id']
        operation = metadata['operation']
        
        if operation == Operation.INSERT:
            existing = self.fetch_current_record(self.TABLE_VEHICLE_MAPPING, {'plaza_id': plaza_id, 'mvc_id': mvc_id})
            if existing:
                logging.warning(f"Vehicle mapping {plaza_id}/{mvc_id} already exists, skipping INSERT")
                return ProcessStatus.SKIPPED
            
            self._insert_vehicle_mapping(data)
            logging.info(f"Inserted VC mapping for plaza: {plaza_id}")
            return ProcessStatus.INSERTED
            
        elif operation == Operation.UPDATE:
            current = self.fetch_current_record(self.TABLE_VEHICLE_MAPPING, {'plaza_id': plaza_id, 'mvc_id': mvc_id})
            if not current:
                raise ValueError(f"Vehicle mapping {plaza_id}/{mvc_id} does not exist. Use INSERT operation.")
            
            all_fields = list(data.keys())
            changes = self.detect_changes(current, data, all_fields)
            
            if not changes:
                logging.info(f"No changes detected for vehicle mapping {plaza_id}/{mvc_id}")
                return ProcessStatus.SKIPPED
            
            self.validate_mutability(changes, override)
            self._update_vehicle_mapping(plaza_id, mvc_id, data, changes)
            logging.info(f"Updated vehicle mapping: {plaza_id}/{mvc_id}, fields: {list(changes.keys())}")
            return ProcessStatus.UPDATED
    
    def process_user_mapping(self, row: Dict, metadata: Dict, override: bool) -> str:
        data = self.extract_table_data(row, 'umap')
        data['_table'] = self.TABLE_USER_MAPPING
        
        if not data.get('user_id'):
            raise ValueError("umap.user_id is required")
        
        user_id = data['user_id']
        operation = metadata['operation']
        
        if operation == Operation.INSERT:
            existing = self.fetch_current_record(self.TABLE_USER_MAPPING, {'user_id': user_id})
            if existing:
                logging.warning(f"User mapping {user_id} already exists, skipping INSERT")
                return ProcessStatus.SKIPPED
            
            self._insert_user_mapping(data)
            logging.info(f"Inserted user mapping: {user_id}")
            return ProcessStatus.INSERTED
            
        elif operation == Operation.UPDATE:
            current = self.fetch_current_record(self.TABLE_USER_MAPPING, {'user_id': user_id})
            if not current:
                raise ValueError(f"User mapping {user_id} does not exist. Use INSERT operation.")
            
            all_fields = list(data.keys())
            changes = self.detect_changes(current, data, all_fields)
            
            if not changes:
                logging.info(f"No changes detected for user_mapping {user_id}")
                return ProcessStatus.SKIPPED
            
            self.validate_mutability(changes, override)
            self._update_user_mapping(user_id, data, changes)
            logging.info(f"Updated user_mapping: {user_id}, fields: {list(changes.keys())}")
            return ProcessStatus.UPDATED
    
    
    def _insert_plaza(self, data: Dict):
        fields = []
        values = []
        placeholders = []
        
        param_num = 1
        for key, value in data.items():
            if key != '_table':
                fields.append(key)
                values.append(value)
                placeholders.append(f':{param_num}')
                param_num += 1
        
        fields.append('created_ts')
        placeholders.append('SYSDATE')
        fields.append('modified_ts')
        placeholders.append('SYSDATE')
        
        sql = f"INSERT INTO {self.TABLE_PLAZA} ({', '.join(fields)}) VALUES ({', '.join(placeholders)})"
        self.execute_query(sql, values)
    
    def _update_plaza(self, plaza_id: str, data: Dict, changes: Dict):
        set_parts = []
        values = []
        
        param_num = 1
        for field in changes.keys():
            if field in data:
                set_parts.append(f"{field} = :{param_num}")
                values.append(data[field])
                param_num += 1
        
        set_parts.append("modified_ts = SYSDATE")
        values.append(plaza_id)
        
        sql = f"UPDATE {self.TABLE_PLAZA} SET {', '.join(set_parts)} WHERE plaza_id = :{param_num}"
        self.execute_query(sql, values)
    
    def _insert_concessionaire(self, data: Dict):
        fields = []
        values = []
        placeholders = []
        
        param_num = 1
        for key, value in data.items():
            if key != '_table':
                fields.append(key)
                values.append(value)
                placeholders.append(f':{param_num}')
                param_num += 1
        
        fields.append('created_ts')
        placeholders.append('SYSDATE')
        fields.append('modified_ts')
        placeholders.append('SYSDATE')
        
        sql = f"INSERT INTO {self.TABLE_CONCESSIONAIRE} ({', '.join(fields)}) VALUES ({', '.join(placeholders)})"
        self.execute_query(sql, values)
    
    def _update_concessionaire(self, conc_id: str, data: Dict, changes: Dict):
        set_parts = []
        values = []
        
        param_num = 1
        for field in changes.keys():
            if field in data:
                set_parts.append(f"{field} = :{param_num}")
                values.append(data[field])
                param_num += 1
        
        set_parts.append("modified_ts = SYSDATE")
        values.append(conc_id)
        
        sql = f"UPDATE {self.TABLE_CONCESSIONAIRE} SET {', '.join(set_parts)} WHERE concessionaire_id = :{param_num}"
        self.execute_query(sql, values)
    
    def _insert_lane(self, data: Dict):
        fields = []
        values = []
        placeholders = []
        
        param_num = 1
        for key, value in data.items():
            if key != '_table':
                fields.append(key)
                values.append(value)
                placeholders.append(f':{param_num}')
                param_num += 1
        
        fields.append('created_ts')
        placeholders.append('SYSDATE')
        fields.append('modified_ts')
        placeholders.append('SYSDATE')
        
        sql = f"INSERT INTO {self.TABLE_LANE} ({', '.join(fields)}) VALUES ({', '.join(placeholders)})"
        self.execute_query(sql, values)
    
    def _update_lane(self, plaza_id: str, lane_id: str, data: Dict, changes: Dict):
        set_parts = []
        values = []
        
        param_num = 1
        for field in changes.keys():
            if field in data:
                set_parts.append(f"{field} = :{param_num}")
                values.append(data[field])
                param_num += 1
        
        set_parts.append("modified_ts = SYSDATE")
        values.extend([plaza_id, lane_id])
        
        sql = f"UPDATE {self.TABLE_LANE} SET {', '.join(set_parts)} WHERE plaza_id = :{param_num} AND lane_id = :{param_num+1}"
        self.execute_query(sql, values)
    
    def _insert_fare(self, data: Dict):
        fields = []
        values = []
        placeholders = []
        
        param_num = 1
        for key, value in data.items():
            if key != '_table':
                fields.append(key)
                values.append(value)
                placeholders.append(f':{param_num}')
                param_num += 1
        
        fields.append('created_ts')
        placeholders.append('SYSDATE')
        fields.append('modified_ts')
        placeholders.append('SYSDATE')
        
        sql = f"INSERT INTO {self.TABLE_FARE} ({', '.join(fields)}) VALUES ({', '.join(placeholders)})"
        self.execute_query(sql, values)
    
    def _update_fare(self, fare_id: str, data: Dict, changes: Dict):
        set_parts = []
        values = []
        
        param_num = 1
        for field in changes.keys():
            if field in data:
                set_parts.append(f"{field} = :{param_num}")
                values.append(data[field])
                param_num += 1
        
        set_parts.append("modified_ts = SYSDATE")
        values.append(fare_id)
        
        sql = f"UPDATE {self.TABLE_FARE} SET {', '.join(set_parts)} WHERE fare_id = :{param_num}"
        self.execute_query(sql, values)
    
    def _insert_vehicle_mapping(self, data: Dict):
        fields = []
        values = []
        placeholders = []
        
        param_num = 1
        for key, value in data.items():
            if key != '_table':
                fields.append(key)
                values.append(value)
                placeholders.append(f':{param_num}')
                param_num += 1
        
        fields.append('created_ts')
        placeholders.append('SYSDATE')
        fields.append('modified_ts')
        placeholders.append('SYSDATE')
        
        sql = f"INSERT INTO {self.TABLE_VEHICLE_MAPPING} ({', '.join(fields)}) VALUES ({', '.join(placeholders)})"
        self.execute_query(sql, values)
    
    def _update_vehicle_mapping(self, plaza_id: str, mvc_id: str, data: Dict, changes: Dict):
        set_parts = []
        values = []
        
        param_num = 1
        for field in changes.keys():
            if field in data:
                set_parts.append(f"{field} = :{param_num}")
                values.append(data[field])
                param_num += 1
        
        set_parts.append("modified_ts = SYSDATE")
        values.extend([plaza_id, mvc_id])
        
        sql = f"UPDATE {self.TABLE_VEHICLE_MAPPING} SET {', '.join(set_parts)} WHERE plaza_id = :{param_num} AND mvc_id = :{param_num+1}"
        self.execute_query(sql, values)
    
    def _insert_user_mapping(self, data: Dict):
        fields = []
        values = []
        placeholders = []
        
        param_num = 1
        for key, value in data.items():
            if key != '_table':
                fields.append(key)
                values.append(value)
                placeholders.append(f':{param_num}')
                param_num += 1
        
        fields.append('created_ts')
        placeholders.append('SYSDATE')
        fields.append('modified_ts')
        placeholders.append('SYSDATE')
        
        sql = f"INSERT INTO {self.TABLE_USER_MAPPING} ({', '.join(fields)}) VALUES ({', '.join(placeholders)})"
        self.execute_query(sql, values)
    
    def _update_user_mapping(self, user_id: str, data: Dict, changes: Dict):
        set_parts = []
        values = []
        
        param_num = 1
        for field in changes.keys():
            if field in data:
                set_parts.append(f"{field} = :{param_num}")
                values.append(data[field])
                param_num += 1
        
        set_parts.append("modified_ts = SYSDATE")
        values.append(user_id)
        
        sql = f"UPDATE {self.TABLE_USER_MAPPING} SET {', '.join(set_parts)} WHERE user_id = :{param_num}"
        self.execute_query(sql, values)
