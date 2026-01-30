import os
import logging
import time
import traceback
import re
from pathlib import Path
from typing import Dict
from common.PluginManager import PluginManager
from common.CsvProcessor import CsvProcessor
from common.Constants import Directories
from utils.FileValidator import FileValidator

class SftpService:
    
    def __init__(
        self,
        plugin_manager: PluginManager,
        csv_processor: CsvProcessor,
        poll_interval: int = 60
    ):
        self.plugin_manager = plugin_manager
        self.csv_processor = csv_processor
        self.poll_interval = poll_interval
    
    def sftp_mode(self, sql_dir: Path):
        from utils.Formatting import get_separator
        from common.SftpClient import SftpClient
        
        logging.info(get_separator())
        logging.info("SFTP POLLING SERVICE")
        logging.info(get_separator())
        
        logging.info(f"Poll interval: {self.poll_interval} seconds")
        logging.info("")
        
        sftp_clients = {}
        
        try:
            for folder_name in self.plugin_manager.get_all_products():
                plugin = self.plugin_manager.get_plugin(folder_name)
                
                env = os.getenv('ENV', 'dev').lower()
                sftp_config = plugin.get_sftp_config(env)
                
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
                                remote_failed = f"{base_path}/failed/{filename}"
                                product_code = self.plugin_manager.get_plugin(folder_name).product_code
                                
                                if not FileValidator.validate_csv_filename(filename, product_code):
                                    sftp.move_file(f"{remote_inbox}/{filename}", remote_failed)
                                    continue
                                
                                logging.info("")
                                logging.info(get_separator("-"))
                                logging.info(f"NEW FILE ON SFTP: {filename}")
                                logging.info(get_separator("-"))
                                
                                remote_processing = f"{base_path}/processing/{filename}"
                                sftp.move_file(f"{remote_inbox}/{filename}", remote_processing)
                                
                                local_inbox = self.plugin_manager.get_product_paths(folder_name)[Directories.INBOX]
                                local_file = local_inbox / filename
                                
                                sftp.download_file(remote_processing, str(local_file))
                                logging.info(f"Downloaded to: {local_file}")
                                
                                self.csv_processor.process_csv_file(local_file, folder_name, sql_dir)
                                
                                if local_file.exists():
                                    processed_path = self.plugin_manager.get_product_paths(folder_name)[Directories.PROCESSED] / filename
                                    if processed_path.exists():
                                        remote_processed = f"{base_path}/processed/{filename}"
                                        sftp.move_file(remote_processing, remote_processed)
                                        logging.info(f"Moved on SFTP to: processed/{filename}")
                                    else:
                                        failed_path = self.plugin_manager.get_product_paths(folder_name)[Directories.FAILED] / filename
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
                
                time.sleep(self.poll_interval)
        
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
