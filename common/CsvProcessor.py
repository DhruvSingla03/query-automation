import csv
import logging
import traceback
from pathlib import Path
from typing import Dict
from common.PluginManager import PluginManager
from common.Constants import Directories
from utils.Formatting import get_separator, get_log_formatter, format_sql

class CsvProcessor:
    
    def __init__(self, plugin_manager: PluginManager):
        self.plugin_manager = plugin_manager
    
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
        log_dir = self.plugin_manager.get_product_paths(folder_name)[Directories.LOGS]
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
    
    def process_csv_file(self, filepath: Path, folder_name: str, sql_dir: Path) -> bool:
        paths = self.plugin_manager.get_product_paths(folder_name)
        plugin = self.plugin_manager.get_plugin(folder_name)
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
                
                plugin.close_connection()
                
                if jira_queries:
                    self.save_sql_queries(filepath.stem, jira_queries, sql_dir)
                
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
                
                return final_status != "FAILED"
                
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
    
    def save_sql_queries(self, csv_filename: str, jira_queries: dict, sql_dir: Path):
        logging.info("")
        logging.info("Saving SQL queries...")
        
        for jira, queries in jira_queries.items():
            sql_file = sql_dir / f"{jira}_{csv_filename}.sql"
            
            with open(sql_file, 'w') as f:
                f.write(f"JIRA Ticket: {jira}\n")
                f.write(f"CSV File: {csv_filename}.csv\n")
                f.write(f"Total Queries: {len(queries)}\n")
                f.write("=" * 80 + "\n\n")
                
                for idx, query in enumerate(queries, 1):
                    formatted_query = format_sql(query)
                    f.write(f"Query {idx}:\n")
                    f.write(formatted_query)
                    f.write("\n\n" + "=" * 80 + "\n\n")
            
            logging.info(f"Saved {len(queries)} queries to: {sql_file}")
        
        logging.info(f"Total JIRA tickets: {len(jira_queries)}")
