import re
import logging
from pathlib import Path
from common.Constants import FilePatterns

class FileValidator:
    
    @staticmethod
    def validate_csv_filename(filename: str, expected_product: str) -> bool:
        match = re.match(FilePatterns.CSV_FILENAME, filename)
        
        if not match:
            logging.error(
                f"Skipping file with invalid name format: {filename}. "
                f"Expected format: OLMID_PRODUCT_YYYYMMDD.csv"
            )
            return False
        
        olmid, file_product, date = match.groups()
        
        if file_product != expected_product:
            logging.error(
                f"Product code mismatch for file: {filename}. "
                f"File product code '{file_product}' does not match folder product '{expected_product}'. "
                f"File should be in products/{file_product.lower()}/inbox/"
            )
            return False
        
        logging.info(
            f"Valid file: {filename} (OLMID: {olmid}, Product: {file_product}, Date: {date})"
        )
        return True
