import paramiko
import logging
import os
from pathlib import Path
from typing import List, Dict


class SftpClient:
    
    def __init__(self, sftp_config):
        self.host = sftp_config['host']
        self.port = sftp_config.get('port', 22)
        self.username = sftp_config['username']
        self.password = sftp_config.get('password')
        self.key_file = sftp_config.get('key_file')
        
        self.transport = None
        self.sftp = None
        self.connect()
    
    def connect(self):
        try:
            self.transport = paramiko.Transport((self.host, self.port))
            
            if self.key_file:
                private_key = paramiko.RSAKey.from_private_key_file(self.key_file)
                self.transport.connect(username=self.username, pkey=private_key)
            else:
                self.transport.connect(username=self.username, password=self.password)
            
            self.sftp = paramiko.SFTPClient.from_transport(self.transport)
            logging.info(f"Connected to SFTP: {self.host}:{self.port}")
            
        except Exception as e:
            raise Exception(f"SFTP connection failed: {e}")
    
    def list_files(self, remote_path: str) -> List[str]:
        try:
            files = []
            for entry in self.sftp.listdir_attr(remote_path):
                if not entry.st_mode & 0o040000:
                    files.append(entry.filename)
            return files
        except FileNotFoundError:
            return []
        except Exception as e:
            logging.error(f"Failed to list files in {remote_path}: {e}")
            return []
    
    def download_file(self, remote_path: str, local_path: str):
        try:
            temp_path = f"{local_path}.tmp"
            self.sftp.get(remote_path, temp_path)
            
            os.rename(temp_path, local_path)
            logging.debug(f"Downloaded: {remote_path} -> {local_path}")
            
        except Exception as e:
            if os.path.exists(f"{local_path}.tmp"):
                os.remove(f"{local_path}.tmp")
            raise Exception(f"Failed to download {remote_path}: {e}")
    
    def move_file(self, source_path: str, dest_path: str):
        try:
            self.sftp.rename(source_path, dest_path)
            logging.debug(f"Moved on SFTP: {source_path} -> {dest_path}")
        except Exception as e:
            raise Exception(f"Failed to move file on SFTP: {e}")
    
    def delete_file(self, remote_path: str):
        try:
            self.sftp.remove(remote_path)
            logging.debug(f"Deleted from SFTP: {remote_path}")
        except Exception as e:
            logging.error(f"Failed to delete {remote_path}: {e}")
    
    def ensure_directory(self, remote_path: str):
        try:
            self.sftp.stat(remote_path)
        except FileNotFoundError:
            try:
                self.sftp.mkdir(remote_path)
                logging.debug(f"Created SFTP directory: {remote_path}")
            except Exception as e:
                logging.error(f"Failed to create directory {remote_path}: {e}")
    
    def close(self):
        if self.sftp:
            self.sftp.close()
        if self.transport:
            self.transport.close()
        logging.debug("SFTP connection closed")
