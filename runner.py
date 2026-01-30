#!/usr/bin/env python3

import csv
import os
import sys
import re
import logging
import traceback
import importlib.util
import time
from pathlib import Path
from typing import Dict
from dotenv import load_dotenv
from watchdog.observers import Observer
from watchdog.events import FileSystemEventHandler

from utils.Formatting import get_separator, get_log_formatter, format_sql
from common.Constants import FilePatterns, Directories



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


class CSVFileHandler(FileSystemEventHandler):
    
    def __init__(self, runner, folder_name: str, paths: dict, product_code: str):
        self.runner = runner
        self.folder_name = folder_name
        self.paths = paths
        self.product_code = product_code
        self.processing_files = set()
    
    def on_created(self, event):
        if event.is_directory:
            return
        
        if not event.src_path.endswith('.csv'):
            return
        
        csv_file = Path(event.src_path)
        
        if csv_file.name in self.processing_files:
            logging.debug(f"File {csv_file.name} already being processed, skipping")
            return
        
        try:
            self.processing_files.add(csv_file.name)
            
            logging.info("")
            logging.info(get_separator("-"))
            logging.info(f"NEW FILE DETECTED: {csv_file.name}")
            logging.info(get_separator("-"))
            
            time.sleep(1)
            
            if not csv_file.exists():
                logging.warning(f"File {csv_file.name} no longer exists, skipping")
                return
            
            match = re.match(FilePatterns.CSV_FILENAME, csv_file.name)
            
            if not match:
                logging.error(
                    f"Skipping file with invalid name format: {csv_file.name}. "
                    f"Expected format: OLMID_PRODUCT_YYYYMMDD.csv"
                )
                failed_path = self.paths[Directories.FAILED] / csv_file.name
                csv_file.rename(failed_path)
                logging.error(f"File moved to: {failed_path}")
                return
            
            olmid, file_product, date = match.groups()
            
            if file_product != self.product_code:
                logging.error(
                    f"Product code mismatch for file: {csv_file.name}. "
                    f"File product code '{file_product}' does not match folder product '{self.product_code}'. "
                    f"File should be in products/{file_product.lower()}/inbox/"
                )
                failed_path = self.paths[Directories.FAILED] / csv_file.name
                csv_file.rename(failed_path)
                logging.error(f"File moved to: {failed_path}")
                return
            
            logging.info(
                f"Valid file: {csv_file.name} (OLMID: {olmid}, Product: {file_product}, Date: {date})"
            )
            
            self.runner.process_csv_file(csv_file, self.folder_name, self.paths)
            
        except Exception as e:
            logging.error(f"Error processing file {csv_file.name}: {str(e)}")
            logging.error(traceback.format_exc())
            try:
                if csv_file.exists():
                    failed_path = self.paths[Directories.FAILED] / csv_file.name
                    csv_file.rename(failed_path)
                    logging.error(f"File moved to failed: {failed_path}")
            except Exception as move_error:
                logging.error(f"Failed to move file to failed folder: {move_error}")
        finally:
            self.processing_files.discard(csv_file.name)


class QueryRunner:
    
    def __init__(self):
        base_dir = Path(__file__).parent
        self.products_dir = base_dir / 'products'
        self.products_dir.mkdir(exist_ok=True)
        
        self.sql_dir = base_dir / Directories.SQL_QUERIES
        self.sql_dir.mkdir(exist_ok=True)
        
        self.plugins = self.discover_products()
        
        self.product_paths = {}
        for folder_name in self.plugins.keys():
            product_dir = self.products_dir / folder_name
            self.product_paths[folder_name] = {
                Directories.INBOX: product_dir / Directories.INBOX,
                Directories.PROCESSING: product_dir / Directories.PROCESSING,
                Directories.PROCESSED: product_dir / Directories.PROCESSED,
                Directories.FAILED: product_dir / Directories.FAILED,
                Directories.LOGS: product_dir / Directories.LOGS
            }
            
            for path in self.product_paths[folder_name].values():
                path.mkdir(parents=True, exist_ok=True)
        
        logging.info(f"Loaded {len(self.plugins)} product(s): {list(self.plugins.keys())}")
    
    def discover_products(self):
        plugins = {}
        
        for product_dir in self.products_dir.iterdir():
            if not product_dir.is_dir():
                continue
            
            folder_name = product_dir.name
            plugin_files = list(product_dir.glob('*Plugin.py'))
            
            if not plugin_files:
                logging.warning(f"No plugin file found in {folder_name}/, skipping")
                continue
            
            if len(plugin_files) > 1:
                logging.warning(f"Multiple plugin files in {folder_name}/, using first: {plugin_files[0].name}")
            
            plugin_file = plugin_files[0]
            plugin_class_name = plugin_file.stem
            
            try:
                module_path = f'products.{folder_name}.{plugin_class_name}'
                spec = importlib.util.spec_from_file_location(module_path, plugin_file)
                
                if spec is None or spec.loader is None:
                    raise ImportError(f"Could not load spec for {plugin_file}")
                
                module = importlib.util.module_from_spec(spec)
                spec.loader.exec_module(module)
                
                if not hasattr(module, plugin_class_name):
                    raise AttributeError(f"Module does not have class {plugin_class_name}")
                
                plugin_class = getattr(module, plugin_class_name)
                plugin_instance = plugin_class()
                
                plugins[folder_name] = plugin_instance
                logging.info(f"Loaded {plugin_class_name} for product: {folder_name}")
                
            except ImportError as e:
                logging.error(f"Import error loading {folder_name}: {e}")
                logging.error(f"Make sure {plugin_file.name} imports are correct")
                continue
                
            except AttributeError as e:
                logging.error(f"Class not found in {folder_name}: {e}")
                logging.error(f"Expected class name: {plugin_class_name}")
                continue
                
            except Exception as e:
                logging.error(f"Failed to load plugin for {folder_name}: {e}")
                logging.error(f"Check {plugin_file.name} for errors")
                logging.error(traceback.format_exc())
                continue
        
        if not plugins:
            raise RuntimeError("No plugins loaded! Check products/ directory")
        
        return plugins
    
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
    
    def add_product_log_handler(self, folder_name: str, csv_filename: str):
        log_dir = self.product_paths[folder_name][Directories.LOGS]
        log_file = log_dir / f"{csv_filename}.log"
        
        formatter = get_log_formatter()
        
        file_handler = logging.FileHandler(log_file)
        file_handler.setLevel(logging.DEBUG)
        file_handler.setFormatter(formatter)
        file_handler.set_name(f'product_{folder_name}')
        
        root_logger = logging.getLogger()
        root_logger.addHandler(file_handler)
        
        logging.info(f"Product-specific log file: {log_file}")
        
        return file_handler
    
    def remove_product_log_handler(self, handler):
        root_logger = logging.getLogger()
        root_logger.removeHandler(handler)
        handler.close()
    
    def process_csv_file(self, filepath: Path, folder_name: str, paths: dict):
        plugin = self.plugins[folder_name]
        product_code = plugin.product_code
        
        logging.info("")
        logging.info(get_separator())
        logging.info(f"PROCESSING FILE: {filepath.name} (Product: {product_code})")
        logging.info(get_separator())
        
        product_handler = self.add_product_log_handler(folder_name, filepath.stem)
        
        try:
            processing_path = paths[Directories.PROCESSING] / filepath.name
            filepath.rename(processing_path)
            logging.info(f"Moved to processing: {processing_path}")
            
            total_rows = 0
            successful_rows = []
            failed_rows = []
            jira_queries = {}
            
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
                    logging.info(get_separator("-"))
                    logging.info(f"PROCESSING ROW {row_num}/{total_rows + 1}")
                    logging.info(get_separator("-"))
                    
                    try:
                        metadata = self.extract_metadata(row)
                        jira = metadata['jira']
                        product = metadata['product']
                        
                        if product != product_code:
                            raise ValueError(
                                f"Metadata mismatch: CSV in {folder_name}/ folder has meta.product={product}, "
                                f"expected {product_code}"
                            )
                        
                        logging.info(
                            f"Row {row_num}: Task ID={jira}, Product={product}, "
                            f"Operation={metadata['operation']}, Override={metadata['override']}"
                        )
                        
                        logging.info(f"Processing row {row_num}")
                        plugin.begin_transaction()
                        plugin.reset_sql_queries()
                        
                        try:
                            logging.info(f"Calling plugin: {folder_name} ({product_code})")
                            plugin.process_row(row, metadata)
                            
                            plugin.commit_transaction()
                            successful_rows.append(row_num)
                            logging.info(f"ROW {row_num} PROCESSED SUCCESSFULLY")
                            
                            if jira not in jira_queries:
                                jira_queries[jira] = []
                            jira_queries[jira].extend(plugin.get_sql_queries())
                            
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
                
                if jira_queries:
                    self.save_sql_queries(filepath.stem, jira_queries)
                
                if successful_rows:
                    processed_path = paths[Directories.PROCESSED] / filepath.name
                    processing_path.rename(processed_path)
                    if failed_rows:
                        final_status = "PARTIAL SUCCESS"
                    else:
                        final_status = "SUCCESS"
                    final_path = processed_path
                else:
                    failed_path = paths[Directories.FAILED] / filepath.name
                    processing_path.rename(failed_path)
                    final_status = "FAILED"
                    final_path = failed_path
                
                logging.info("")
                logging.info(get_separator())
                logging.info(f"FILE PROCESSING COMPLETE: {filepath.name}")
                logging.info(get_separator())
                logging.info(f"Status: {final_status}")
                logging.info(f"Total Rows: {total_rows}")
                logging.info(f"Successful: {len(successful_rows)} rows")
                logging.info(f"Failed: {len(failed_rows)} rows")
                if successful_rows:
                    logging.info(f"Successful rows: {successful_rows}")
                if failed_rows:
                    logging.error(f"Failed rows: {failed_rows}")
                logging.info(f"Final location: {final_path}")
                logging.info(get_separator())
                
            except Exception as e:
                logging.error("")
                logging.error(get_separator())
                logging.error(f"ERROR PROCESSING FILE: {filepath.name}")
                logging.error(get_separator())
                logging.error(f"Error: {str(e)}")
                logging.error("Full stack trace:")
                logging.error(traceback.format_exc())
                logging.error(get_separator())
                
                failed_path = paths[Directories.FAILED] / filepath.name
                processing_path.rename(failed_path)
                logging.error(f"File moved to: {failed_path}")
                
                raise
        
        finally:
            self.remove_product_log_handler(product_handler)
    
    def scan_inbox(self):
        logging.info("Scanning product inboxes for CSV files...")
        
        for folder_name, paths in self.product_paths.items():
            inbox_path = paths[Directories.INBOX]
            plugin = self.plugins[folder_name]
            product_code = plugin.product_code
            
            csv_files = list(inbox_path.glob('*.csv'))
            
            if csv_files:
                logging.info(f"Found {len(csv_files)} file(s) in {folder_name}/inbox/")
            
            valid_files = []
            for csv_file in csv_files:
                match = re.match(FilePatterns.CSV_FILENAME, csv_file.name)
                
                if not match:
                    logging.error(
                        f"Skipping file with invalid name format: {csv_file.name}. "
                        f"Expected format: OLMID_PRODUCT_YYYYMMDD.csv"
                    )
                    failed_path = paths[Directories.FAILED] / csv_file.name
                    csv_file.rename(failed_path)
                    logging.error(f"File moved to: {failed_path}")
                    continue
                
                olmid, file_product, date = match.groups()
                
                if file_product != product_code:
                    logging.error(
                        f"Product code mismatch for file: {csv_file.name}. "
                        f"File product code '{file_product}' does not match folder product '{product_code}'. "
                        f"File should be in products/{file_product.lower()}/inbox/"
                    )
                    failed_path = paths[Directories.FAILED] / csv_file.name
                    csv_file.rename(failed_path)
                    logging.error(f"File moved to: {failed_path}")
                    continue
                
                valid_files.append(csv_file)
                logging.info(
                    f"Valid file: {csv_file.name} (OLMID: {olmid}, Product: {file_product}, Date: {date})"
                )
            
            for csv_file in valid_files:
                try:
                    self.process_csv_file(csv_file, folder_name, paths)
                except Exception as e:
                    logging.error(f"Failed to process {csv_file.name}: {str(e)}")
    
    def save_sql_queries(self, csv_filename: str, jira_queries: dict):
        for jira, queries in jira_queries.items():
            if not queries:
                logging.debug(f"No queries to save for {jira}")
                continue
            
            sql_filename = f"{csv_filename}_{jira}.sql"
            sql_filepath = self.sql_dir / sql_filename
            
            try:
                with open(sql_filepath, 'w') as f:
                    f.write(f"-- SQL Queries for {csv_filename}\n")
                    f.write(f"-- JIRA: {jira}\n")
                    f.write(f"-- Total queries: {len(queries)}\n")
                    f.write(f"-- Generated: {Path(__file__).parent}\n\n")
                    
                    for idx, query_info in enumerate(queries, start=1):
                        sql = query_info['sql']
                        params = query_info['params']
                        
                        formatted_sql = format_sql(sql, params)
                        
                        f.write(f"-- Query {idx}\n")
                        f.write(f"{formatted_sql};\n\n")
                
                logging.info(f"Saved {len(queries)} SQL queries to: {sql_filepath}")
            
            except Exception as e:
                logging.error(f"Failed to save SQL queries for {jira}: {str(e)}")


    
    def sftp_mode(self):
        logging.info(get_separator())
        logging.info("SFTP POLLING SERVICE")
        logging.info(get_separator())
        
        from common.SftpClient import SftpClient
        
        poll_interval = int(os.getenv('SFTP_POLL_INTERVAL', 60))
        logging.info(f"Poll interval: {poll_interval} seconds")
        logging.info("")
        
        sftp_clients = {}
        
        try:
            for folder_name, plugin in self.plugins.items():
                product_config_path = plugin.__class__.__module__.rsplit('.', 1)[0]
                config_module = importlib.import_module(f"{product_config_path}.FastagAcqConfig")
                
                env = os.getenv('ENV', 'dev').lower()
                sftp_config = config_module.SftpConfig.DEV if env == 'dev' else config_module.SftpConfig.PROD
                
                sftp_clients[folder_name] = {
                    'client': SftpClient(sftp_config),
                    'base_path': sftp_config['base_path']
                }
                logging.info(f"Connected to SFTP for {folder_name}: {sftp_config['host']}")
            
            logging.info(get_separator())
            logging.info("Polling started. Press Ctrl+C to stop.")
            logging.info(get_separator())
            
            while True:
                for folder_name, sftp_info in sftp_clients.items():
                    sftp = sftp_info['client']
                    base_path = sftp_info['base_path']
                    remote_inbox = f"{base_path}/inbox"
                    
                    try:
                        files = sftp.list_files(remote_inbox)
                        
                        for filename in files:
                            if not filename.endswith('.csv'):
                                continue
                            
                            try:
                                match = re.match(FilePatterns.CSV_FILENAME, filename)
                                if not match:
                                    logging.warning(f"Skipping invalid filename: {filename}")
                                    remote_failed = f"{base_path}/failed/{filename}"
                                    sftp.move_file(f"{remote_inbox}/{filename}", remote_failed)
                                    continue
                                
                                olmid, file_product, date = match.groups()
                                product_code = self.plugins[folder_name].product_code
                                
                                if file_product != product_code:
                                    logging.error(f"Product mismatch in {filename}: {file_product} != {product_code}")
                                    remote_failed = f"{base_path}/failed/{filename}"
                                    sftp.move_file(f"{remote_inbox}/{filename}", remote_failed)
                                    continue
                                
                                logging.info("")
                                logging.info(get_separator("-"))
                                logging.info(f"NEW FILE ON SFTP: {filename}")
                                logging.info(get_separator("-"))
                                
                                remote_processing = f"{base_path}/processing/{filename}"
                                sftp.move_file(f"{remote_inbox}/{filename}", remote_processing)
                                
                                local_inbox = self.product_paths[folder_name][Directories.INBOX]
                                local_file = local_inbox / filename
                                
                                sftp.download_file(remote_processing, str(local_file))
                                logging.info(f"Downloaded to: {local_file}")
                                
                                self.process_csv_file(local_file, folder_name, self.product_paths[folder_name])
                                
                                if local_file.exists():
                                    processed_path = self.product_paths[folder_name][Directories.PROCESSED] / filename
                                    if processed_path.exists():
                                        remote_processed = f"{base_path}/processed/{filename}"
                                        sftp.move_file(remote_processing, remote_processed)
                                        logging.info(f"Moved on SFTP to: processed/{filename}")
                                    else:
                                        failed_path = self.product_paths[folder_name][Directories.FAILED] / filename
                                        if failed_path.exists():
                                            remote_failed = f"{base_path}/failed/{filename}"
                                            sftp.move_file(remote_processing, remote_failed)
                                            logging.info(f"Moved on SFTP to: failed/{filename}")
                                
                            except Exception as e:
                                logging.error(f"Error processing {filename}: {e}")
                                logging.error(traceback.format_exc())
                                try:
                                    remote_failed = f"{base_path}/failed/{filename}"
                                    sftp.move_file(f"{remote_inbox}/{filename}", remote_failed)
                                except:
                                    pass
                    
                    except Exception as e:
                        logging.error(f"Error polling {folder_name}: {e}")
                
                time.sleep(poll_interval)
        
        except KeyboardInterrupt:
            logging.info("")
            logging.info(get_separator())
            logging.info("Polling stopped by user")
            logging.info(get_separator())
        finally:
            for sftp_info in sftp_clients.values():
                try:
                    sftp_info['client'].close()
                except:
                    pass
    
    def watch_mode(self):
        logging.info(get_separator())
        logging.info("STARTING WATCH SERVICE")
        logging.info(get_separator())
        logging.info(f"Monitoring {len(self.product_paths)} product inbox(es) for new CSV files...")
        logging.info("Press Ctrl+C to stop")
        logging.info(get_separator())
        
        observers = []
        handlers = []
        
        try:
            for folder_name, paths in self.product_paths.items():
                inbox_path = paths[Directories.INBOX]
                plugin = self.plugins[folder_name]
                product_code = plugin.product_code
                
                handler = CSVFileHandler(self, folder_name, paths, product_code)
                handlers.append(handler)
                
                observer = Observer()
                observer.schedule(handler, str(inbox_path), recursive=False)
                observer.start()
                observers.append(observer)
                
                logging.info(f"Watching: {inbox_path} (Product: {product_code})")
            
            logging.info(get_separator())
            logging.info("Watch service active. Waiting for files...")
            logging.info(get_separator())
            
            while True:
                time.sleep(1)
                
        except KeyboardInterrupt:
            logging.info("")
            logging.info(get_separator())
            logging.info("STOPPING WATCH SERVICE (Ctrl+C received)")
            logging.info(get_separator())
            
        except Exception as e:
            logging.critical("")
            logging.critical(get_separator())
            logging.critical("WATCH SERVICE ENCOUNTERED CRITICAL ERROR")
            logging.critical(get_separator())
            logging.critical(f"Error: {str(e)}")
            logging.critical(traceback.format_exc())
            logging.critical(get_separator())
            
        finally:
            logging.info("Stopping file observers...")
            for observer in observers:
                observer.stop()
            
            logging.info("Waiting for observers to finish...")
            for observer in observers:
                observer.join()
            
            logging.info(get_separator())
            logging.info("WATCH SERVICE STOPPED")
            logging.info(get_separator())


if __name__ == '__main__':
    try:
        runner = QueryRunner()
        
        if '--sftp' in sys.argv or '-s' in sys.argv:
            runner.sftp_mode()
        elif '--watch' in sys.argv or '-w' in sys.argv:
            runner.watch_mode()
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