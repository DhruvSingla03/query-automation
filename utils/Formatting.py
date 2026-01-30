import shutil
import logging
import sys
import re
from common.Constants import Formatting as FormattingConstants

def get_separator(char="="):
    try:
        frame = sys._getframe(1)
        filename = frame.f_code.co_filename.split('/')[-1]
        lineno = frame.f_lineno 
        funcname = frame.f_code.co_name
        
        timestamp_width = FormattingConstants.TIMESTAMP_WIDTH
        file_width = len(f"[{filename}:{lineno}]") + 1
        func_width = len(f"[{funcname}()]") + 1
        level_width = FormattingConstants.LEVEL_WIDTH
        
        prefix_width = timestamp_width + file_width + func_width + level_width
        
        terminal_width = shutil.get_terminal_size().columns
        message_width = max(terminal_width - prefix_width, 40)
        
    except Exception:
        try:
            terminal_width = shutil.get_terminal_size().columns
            prefix_width = FormattingConstants.MAX_LOG_PREFIX_WIDTH
            message_width = max(terminal_width - prefix_width, 40)
        except Exception:
            message_width = 80
    
    return char * message_width

def get_log_formatter():
    return logging.Formatter(
        '[%(asctime)s] [%(filename)s:%(lineno)d] [%(funcName)s()] [%(levelname)s] %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S'
    )
    
def format_sql(sql: str, params: list) -> str:
    if not params:
        return sql
    
    formatted_sql = sql
    
    for i, value in enumerate(params, start=1):
        placeholder_pattern = rf':{i}(?!\d)'
        
        if value is None:
            formatted_value = 'NULL'
        elif isinstance(value, str):
            escaped_value = value.replace("'", "''")
            formatted_value = f"'{escaped_value}'"
        elif isinstance(value, (int, float)):
            formatted_value = str(value)
        else:
            formatted_value = f"'{str(value)}'"
        
        formatted_sql = re.sub(placeholder_pattern, formatted_value, formatted_sql)
    
    return formatted_sql

