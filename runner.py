#!/usr/bin/env python3

import csv
import os
import sys
import logging
import traceback
from pathlib import Path
from typing import Dict
from dotenv import load_dotenv

from plugins.fastagAcqPlugin import FastagAcqPlugin


load_dotenv()

def setup_logging():
    log_dir = Path(__file__).parent / 'logs'
    log_dir.mkdir(exist_ok=True)
    log_file = log_dir / 'onboarding.log'
    
    formatter = logging.Formatter(
        '%(asctime)s - [%(filename)s:%(lineno)d] - %(funcName)s() - %(levelname)s - %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S'
    )
    
    file_handler = logging.FileHandler(log_file)
    file_handler.setLevel(logging.DEBUG)
    file_handler.setFormatter(formatter)
    
    root_logger = logging.getLogger()
    root_logger.setLevel(logging.DEBUG)
    root_logger.addHandler(file_handler)
    
    if os.getenv('ENV') != 'production' or sys.stdout.isatty():
        console_handler = logging.StreamHandler()
        console_handler.setLevel(logging.INFO)
        console_handler.setFormatter(formatter)
        root_logger.addHandler(console_handler)
    
    return log_file

LOG_FILE = setup_logging()
logging.info("=" * 80)
logging.info("QUERY AUTOMATION STARTED")
logging.info(f"Log file: {LOG_FILE}")
logging.info("=" * 80)


class OnboardingRunner:
    
    def __init__(self):
        base_dir = Path(__file__).parent
        self.products_dir = base_dir / 'products'
        self.products_dir.mkdir(exist_ok=True)
        
        self.plugins = {
            'FASTAG_ACQ': FastagAcqPlugin()
        }
        
        self.product_paths = {}
        for product_code in self.plugins.keys():
            product_dir = self.products_dir / product_code
            self.product_paths[product_code] = {
                'inbox': product_dir / 'inbox',
                'processing': product_dir / 'processing',
                'processed': product_dir / 'processed',
                'failed': product_dir / 'failed',
                'logs': product_dir / 'logs'
            }
            
            for path in self.product_paths[product_code].values():
                path.mkdir(parents=True, exist_ok=True)
    
    def extract_metadata(self, row: Dict) -> Dict:
        metadata = {
            'product': row.get('meta.product', '').strip(),
            'submitted_by': row.get('meta.submitted_by', '').strip(),
            'jira': row.get('meta.jira', '').strip(),
            'operation': row.get('meta.operation', '').strip().upper(),
            'override': row.get('meta.override', 'false').strip().lower()
        }
        
        for field in ['product', 'submitted_by', 'jira', 'operation']:
            if not metadata[field]:
                raise ValueError(f"Missing required metadata: meta.{field}")
        
        return metadata
    
    def process_csv_file(self, filepath: Path, product_code: str, paths: dict):
        logging.info("")
        logging.info("=" * 80)
        logging.info(f"PROCESSING FILE: {filepath.name} (Product: {product_code})")
        logging.info("=" * 80)
        
        processing_path = paths['processing'] / filepath.name
        filepath.rename(processing_path)
        logging.info(f"Moved to processing: {processing_path}")
        
        total_rows = 0
        successful_rows = []
        failed_rows = []
        
        try:
            with open(processing_path, 'r') as f:
                reader = csv.DictReader(f)
                rows = list(reader)
            
            total_rows = len(rows)
            logging.info(f"Total rows to process: {total_rows}")
            
            if not rows:
                raise ValueError("CSV file is empty")
            
            for row_num, row in enumerate(rows, start=2):
                logging.info("")
                logging.info("-" * 80)
                logging.info(f"PROCESSING ROW {row_num}/{total_rows + 1}")
                logging.info("-" * 80)
                
                try:
                    metadata = self.extract_metadata(row)
                    jira = metadata['jira']
                    product = metadata['product']
                    
                    if product != product_code:
                        raise ValueError(f"File in {product_code} folder but meta.product is {product}")
                    
                    logging.info(
                        f"Row {row_num}: Task ID={jira}, Product={product}, "
                        f"Operation={metadata['operation']}, Override={metadata['override']}"
                    )
                    
                    plugin = self.plugins.get(product)
                    if not plugin:
                        raise ValueError(f"No plugin found for product: {product}")
                    
                    logging.info(f"Processing row {row_num}")
                    plugin.begin_transaction()
                    
                    try:
                        logging.info(f"Calling plugin: {product}")
                        plugin.process_row(row, metadata)
                        
                        plugin.commit_transaction()
                        successful_rows.append(row_num)
                        logging.info(f"ROW {row_num} PROCESSED SUCCESSFULLY")
                        
                    except Exception as e:
                        plugin.rollback_transaction()
                        logging.error(f"Changes rolled back for row {row_num}")
                        raise
                    
                except Exception as e:
                    failed_rows.append(row_num)
                    error_msg = str(e)
                    
                    logging.error(f"ROW {row_num} FAILED")
                    logging.error(f"Error message: {error_msg}")
                    logging.error("Full stack trace:")
                    logging.error(traceback.format_exc())
            
            for plugin in self.plugins.values():
                plugin.close_connection()
            
            if successful_rows:
                processed_path = paths['processed'] / filepath.name
                processing_path.rename(processed_path)
                if failed_rows:
                    final_status = "PARTIAL SUCCESS"
                else:
                    final_status = "SUCCESS"
                final_path = processed_path
            else:
                failed_path = paths['failed'] / filepath.name
                processing_path.rename(failed_path)
                final_status = "FAILED"
                final_path = failed_path
            
            logging.info("")
            logging.info("=" * 80)
            logging.info(f"FILE PROCESSING COMPLETE: {filepath.name}")
            logging.info("=" * 80)
            logging.info(f"Status: {final_status}")
            logging.info(f"Total Rows: {total_rows}")
            logging.info(f"Successful: {len(successful_rows)} rows")
            logging.info(f"Failed: {len(failed_rows)} rows")
            if successful_rows:
                logging.info(f"Successful rows: {successful_rows}")
            if failed_rows:
                logging.error(f"Failed rows: {failed_rows}")
            logging.info(f"Final location: {final_path}")
            logging.info("=" * 80)
            
        except Exception as e:
            logging.error("")
            logging.error("=" * 80)
            logging.error(f"ERROR PROCESSING FILE: {filepath.name}")
            logging.error("=" * 80)
            logging.error(f"Error: {str(e)}")
            logging.error("Full stack trace:")
            logging.error(traceback.format_exc())
            logging.error("=" * 80)
            
            failed_path = paths['failed'] / filepath.name
            processing_path.rename(failed_path)
            logging.error(f"File moved to: {failed_path}")
            
            raise
    
    def scan_inbox(self):
        logging.info("Scanning product inboxes for CSV files...")
        
        for product_code, paths in self.product_paths.items():
            inbox_path = paths['inbox']
            csv_files = list(inbox_path.glob('*.csv'))
            
            if csv_files:
                logging.info(f"Found {len(csv_files)} file(s) in {product_code}/inbox/")
            
            for csv_file in csv_files:
                try:
                    self.process_csv_file(csv_file, product_code, paths)
                except Exception as e:
                    logging.error(f"Failed to process {csv_file.name}: {str(e)}")


if __name__ == '__main__':
    try:
        runner = OnboardingRunner()
        runner.scan_inbox()
        logging.info("")
        logging.info("=" * 80)
        logging.info("QUERY AUTOMATION ENDED")
        logging.info("=" * 80)
    except Exception as e:
        logging.critical("")
        logging.critical("=" * 80)
        logging.critical("QUERY AUTOMATION FAILED")
        logging.critical("=" * 80)
        logging.critical(f"Error: {str(e)}")
        logging.critical("Full stack trace:")
        logging.critical(traceback.format_exc())
        logging.critical("=" * 80)
        sys.exit(1)