import sys
from pathlib import Path
sys.path.append(str(Path(__file__).parent.parent.parent))

import os
import re
import logging
from typing import Dict, List
from .FastagAcqConfig import Product, Tables, FieldRules, VaultConfig, SftpConfig
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
    
    def get_sftp_config(self, env: str = 'dev'):
        return SftpConfig.DEV if env == 'dev' else SftpConfig.PROD
    
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
        return self._process_entity(
            table=self.TABLE_PLAZA,
            prefix='plaza',
            pk_fields=['plaza_id'],
            row=row,
            operation=metadata['operation'],
            override=override,
            entity_name='Plaza'
        )
    
    def process_concessionaire(self, row: Dict, metadata: Dict, override: bool) -> str:
        return self._process_entity(
            table=self.TABLE_CONCESSIONAIRE,
            prefix='conc',
            pk_fields=['concessionaire_id'],
            row=row,
            operation=metadata['operation'],
            override=override,
            entity_name='Concessionaire'
        )
    
    def process_lane(self, row: Dict, metadata: Dict, override: bool) -> str:
        return self._process_entity(
            table=self.TABLE_LANE,
            prefix='lane',
            pk_fields=['plaza_id', 'lane_id'],
            row=row,
            operation=metadata['operation'],
            override=override,
            entity_name='Lane'
        )
    
    def process_fare(self, row: Dict, metadata: Dict, override: bool) -> str:
        return self._process_entity(
            table=self.TABLE_FARE,
            prefix='fare',
            pk_fields=['fare_id'],
            row=row,
            operation=metadata['operation'],
            override=override,
            entity_name='Fare'
        )
    
    def process_vehicle_mapping(self, row: Dict, metadata: Dict, override: bool) -> str:
        return self._process_entity(
            table=self.TABLE_VEHICLE_MAPPING,
            prefix='vmap',
            pk_fields=['plaza_id', 'mvc_id'],
            row=row,
            operation=metadata['operation'],
            override=override,
            entity_name='Vehicle Mapping'
        )
    
    def process_user_mapping(self, row: Dict, metadata: Dict, override: bool) -> str:
        return self._process_entity(
            table=self.TABLE_USER_MAPPING,
            prefix='umap',
            pk_fields=['user_id'],
            row=row,
            operation=metadata['operation'],
            override=override,
            entity_name='User Mapping'
        )
