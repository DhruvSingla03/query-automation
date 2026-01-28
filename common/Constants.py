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
    CSV_FILENAME = r'^([Bb]\d{7})_([A-Z_]+)_(\d{8})\.csv$'

class Directories:
    SQL_QUERIES = 'sqlqueries'
    INBOX = 'inbox'
    PROCESSING = 'processing'
    PROCESSED = 'processed'
    FAILED = 'failed'
    LOGS = 'logs'

class Formatting:
    MAX_LOG_PREFIX_WIDTH = 90
    TIMESTAMP_WIDTH = 23
    LEVEL_WIDTH = 12
