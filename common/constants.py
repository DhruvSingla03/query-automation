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
