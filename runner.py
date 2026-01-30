#!/usr/bin/env python3

import os
import sys
import re
import logging
import traceback
from pathlib import Path
from typing import Dict
from dotenv import load_dotenv

from utils.Formatting import get_separator, get_log_formatter
from utils.FileValidator import FileValidator
from common.Constants import Directories
from common.PluginManager import PluginManager
from common.CsvProcessor import CsvProcessor
from common.SftpService import SftpService



load_dotenv()

def setup_logging():
    log_dir = Path(__file__).parent / 'logs'
    log_dir.mkdir(exist_ok=True)
    log_file = log_dir / 'query.log'
    
    formatter = get_log_formatter()
    
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
logging.info(get_separator())
logging.info("QUERY AUTOMATION STARTED")
logging.info(f"Log file: {LOG_FILE}")
logging.info(get_separator())



class QueryRunner:
    
    def __init__(self):
        base_dir = Path(__file__).parent
        products_dir = base_dir / 'products'
        
        self.plugin_manager = PluginManager(products_dir)
        self.csv_processor = CsvProcessor(self.plugin_manager)
        
        poll_interval = int(os.getenv('SFTP_POLL_INTERVAL', 60))
        self.sftp_service = SftpService(self.plugin_manager, self.csv_processor, poll_interval)
        
        self.sql_dir = base_dir / Directories.SQL_QUERIES
        self.sql_dir.mkdir(exist_ok=True)

    
    def scan_inbox(self):
        logging.info("Scanning product inboxes for CSV files...")
        
        for folder_name in self.plugin_manager.get_all_products():
            paths = self.plugin_manager.get_product_paths(folder_name)
            inbox_path = paths[Directories.INBOX]
            plugin = self.plugin_manager.get_plugin(folder_name)
            product_code = plugin.product_code
            
            csv_files = list(inbox_path.glob('*.csv'))
            
            if csv_files:
                logging.info(f"Found {len(csv_files)} file(s) in {folder_name}/inbox/")
            
            valid_files = []
            for csv_file in csv_files:
                failed_path = paths[Directories.FAILED] / csv_file.name
                
                if FileValidator.validate_csv_filename(csv_file.name, product_code):
                    valid_files.append(csv_file)
                else:
                    csv_file.rename(failed_path)
                    logging.error(f"File moved to: {failed_path}")
            
            for csv_file in valid_files:
                try:
                    self.csv_processor.process_csv_file(csv_file, folder_name, self.sql_dir)
                except Exception as e:
                    logging.error(f"Failed to process {csv_file.name}: {str(e)}")
    
    
    
    def sftp_mode(self):
        self.sftp_service.sftp_mode(self.sql_dir)
    


if __name__ == '__main__':
    try:
        runner = QueryRunner()
        
        if '--sftp' in sys.argv or '-s' in sys.argv:
            runner.sftp_mode()
        else:
            runner.scan_inbox()
            logging.info("")
            logging.info(get_separator())
            logging.info("QUERY AUTOMATION ENDED")
            logging.info(get_separator())
            
    except Exception as e:
        logging.critical("")
        logging.critical(get_separator())
        logging.critical("QUERY AUTOMATION FAILED")
        logging.critical(get_separator())
        logging.critical(f"Error: {str(e)}")
        logging.critical("Full stack trace:")
        logging.critical(traceback.format_exc())
        logging.critical(get_separator())
        sys.exit(1)