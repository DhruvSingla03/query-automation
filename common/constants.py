class Operation:
    INSERT = 'INSERT'
    UPDATE = 'UPDATE'

class ProcessStatus:
    INSERTED = 'inserted'
    SKIPPED = 'skipped'
    UPDATED = 'updated'

class MetadataField:
    PRODUCT = 'product'
    SUBMITTED_BY = 'submitted_by'
    JIRA = 'jira'
    OPERATION = 'operation'
    OVERRIDE = 'override'

class FilePatterns:
    CSV_FILENAME = r'^([a-zA-Z0-9_]+)_([A-Z_]+)_(\d{8})\.csv$'

class Directories:
    SQL_QUERIES = 'sql_queries'
    INBOX = 'inbox'
    PROCESSING = 'processing'
    PROCESSED = 'processed'
    FAILED = 'failed'
    LOGS = 'logs'
