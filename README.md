# Partner Onboarding Query Automation

Automated system for processing partner onboarding CSV files.

## Overview

This system processes onboarding requests via **CSV files** where each row can contain data for multiple related tables. It features:

- **Row-level transactions**: Each row is atomic (all-or-nothing)
- **Field mutability control**: Protect critical fields from accidental changes
- **Product-centric architecture**: Each product has its own isolated folder structure
- **Comprehensive logging**: Unified log with full stack traces and row-level tracking

## Quick Start

### 1. Setup Environment

```bash
cd query-automation
cp .env.template .env
# Edit .env with your database credentials
```

### 2. Install Dependencies

```bash
pip install -r requirements.txt
```

**Dependencies:**
- `oracledb` - Oracle database driver
- `hvac` - HashiCorp Vault client
- `python-dotenv` - Environment variable management

### 3. Configure Environment

**Development Mode** (bypass Vault):
```bash
# .env
# For now using hardcoded credentials
DEV_MODE=true
DB_HOST=localhost
DB_PORT=1521
DB_DATABASE=ORCL
DB_USERNAME=system
DB_PASSWORD=oracle
```

### 4. Run the System

```bash
python runner.py
```

The runner scans `products/*/inbox/` for CSV files and processes them automatically.

## Directory Structure

```
query-automation/
├── products/                           # Product-specific folders
│   └── FASTAG_ACQ/                     # Each product isolated
│       ├── inbox/                      # Drop CSV files here
│       ├── processing/                 # Files being processed
│       ├── processed/                  # Successfully processed
│       ├── failed/                     # Failed files
│       └── logs/                       # Product-specific logs
│       └── FastagAcqPlugin.py          # Product-specific plugin
├── common/                             # Shared modules
│   ├── BasePlugin.py                   # Base plugin with DB & transaction logic
│   ├── VaultClient.py                  # Pending: Vault integration
│   ├── Constants.py                    # Shared constants (Operation, ProcessStatus)
│   └── TestConnection.py               # DB connection testing utility
├── logs/                               # System-wide logs
│   └── query.log                       # Unified log for all products
├── runner.py                           # Main orchestrator
├── .env                                # Environment configuration
├── .gitignore                          # Git ignore rules
└── README.md                           # This file
```

## CSV Format

### Structure

Each CSV row contains:
- **Metadata columns** (mandatory): `meta.product`, `meta.submitted_by`, `meta.jira`, `meta.operation`, `meta.override`
- **Table-specific columns** (prefixed): `plaza.*`, `conc.*`, `lane.*`, `fare.*`, `vmap.*`, `umap.*`

### Example

```csv
meta.product,meta.submitted_by,meta.jira,meta.operation,meta.override,plaza.plaza_id,plaza.plaza_name,lane.plaza_id,lane.lane_id
FASTAG_ACQ,user123,APB-001,INSERT,false,PLZ001,Highway Plaza,PLZ001,L1
FASTAG_ACQ,user123,APB-001,INSERT,false,PLZ001,Highway Plaza,PLZ001,L2
FASTAG_ACQ,user123,APB-001,INSERT,false,PLZ001,Highway Plaza,PLZ001,L3
```

**Note:** Row 2 and 3 will skip the plaza (already exists) and insert only the lanes.

### Metadata Fields

| Field | Required | Description | Example |
|-------|----------|-------------|---------|
| meta.product | Yes | Product code (must match folder) | FASTAG_ACQ |
| meta.submitted_by | Yes | User who submitted | user123 |
| meta.jira | Yes | JIRA ticket (format: APB-\d+) | APB-001 |
| meta.operation | Yes | INSERT or UPDATE | INSERT |
| meta.override | No | Allow immutable field changes | false |

### Table Prefixes

- `plaza.*` - Plaza details (plaza_id, plaza_name, etc.)
- `conc.*` - Concessionaire details (concessionaire_id, concessionaire_name, etc.)
- `lane.*` - Lane details (plaza_id, lane_id, lane_type, etc.)
- `fare.*` - Fare details (fare_id, plaza_id, fare_amount, etc.)
- `vmap.*` - Vehicle class mapping (plaza_id, mvc_id, avc_id, etc.)
- `umap.*` - User mapping (user_id, role, status, etc.)

## Operations

### INSERT

**Behavior:** Creates new records, **skips existing records** instead of failing.

**Example:**
```csv
meta.operation,plaza.plaza_id,plaza.plaza_name,lane.plaza_id,lane.lane_id
INSERT,PLZ001,Highway Plaza,PLZ001,L1
INSERT,PLZ001,Highway Plaza,PLZ001,L2  # Plaza skipped, lane inserted
```

**Log Output:**
```
Row 2: Tables inserted: ['plaza', 'lane']
Row 3: Tables skipped (already exist): ['plaza']
       Tables inserted: ['lane']
```

**Database Result:**
- Plaza PLZ001: Inserted once
- Lanes L1, L2: Both inserted

### UPDATE

**Behavior:** Updates existing records. **Fails if record doesn't exist.**

**Example:**
```csv
meta.operation,plaza.plaza_id,plaza.merchant_id
UPDATE,PLZ001,MERCH_NEW
```

**Field Mutability:**
- Only `modified_ts` is mutable by default for most tables
- Override required for other fields

### UPDATE with Override

**Behavior:** Allows changes to **immutable fields**.

```csv
meta.operation,meta.override,plaza.plaza_id,plaza.plaza_name
UPDATE,true,PLZ001,Updated Plaza Name
```

**⚠️ Use with caution** - All overrides are logged.

## Field Mutability Rules

### Current Mutable Fields

**All Tables:** Only `modified_ts` is mutable without override.

To change other fields, set `meta.override=true`.

### How It Works

1. **INSERT**: All fields can be set
2. **UPDATE (override=false)**: Only mutable fields can change
3. **UPDATE (override=true)**: Any field can change (logged as override)

## Transaction Management

### Row-Level Atomicity

Each CSV row is processed in a database transaction:

```
BEGIN TRANSACTION
  ├─ Process plaza → SUCCESS
  ├─ Process lane → SUCCESS
  └─ Process fare → FAIL
ROLLBACK  # All changes for this row are rolled back
```

**Result:** Row fails, no partial data inserted.

### File Processing Logic

**Partial Success Allowed:**
- Row 1: Success ✅
- Row 2: Failed ❌
- Row 3: Success ✅

**Outcome:** File moved to `processed/` (2 of 3 rows succeeded)

**All Rows Failed:**
- Row 1: Failed ❌
- Row 2: Failed ❌

**Outcome:** File moved to `failed/`

## Logging

### Unified Log File

All logs go to `logs/query.log` with format:
├── runner.py                           # Main orchestrator
├── .env                                # Environment configuration
└── README.md                           # This file
```

## CSV Format

### Structure

Each CSV row contains:
- **Metadata columns** (mandatory): `meta.product`, `meta.submitted_by`, `meta.jira`, `meta.operation`, `meta.override`
- **Table-specific columns** (prefixed):
For example, for FASTag Acquiring, the columns would be `plaza.*`, `conc.*`, `lane.*`, `fare.*`, `vmap.*`, `umap.*`

### Example

```csv
meta.product,meta.submitted_by,meta.jira,meta.operation,meta.override,plaza.plaza_id,plaza.plaza_name,lane.plaza_id,lane.lane_id
FASTAG_ACQ,user123,APB-001,INSERT,false,PLZ001,Highway Plaza,PLZ001,L1
FASTAG_ACQ,user123,APB-001,INSERT,false,PLZ001,Highway Plaza,PLZ001,L2
FASTAG_ACQ,user123,APB-001,INSERT,false,PLZ001,Highway Plaza,PLZ001,L3
```

**Note:** Row 2 and 3 will skip the plaza (already exists) and insert only the lanes.

### Metadata Fields

| Field | Required | Description | Example |
|-------|----------|-------------|---------|
| meta.product | Yes | Product code (must match folder) | FASTAG_ACQ |
| meta.submitted_by | Yes | User who submitted | user123 |
| meta.jira | Yes | JIRA ticket (format: APB-\d+) | APB-001 |
| meta.operation | Yes | INSERT or UPDATE | INSERT |
| meta.override | No | Allow immutable field changes | false |

## Operations

### INSERT

**Behavior:** Creates new records, **skips existing records**.

**Example:**
```csv
meta.operation,plaza.plaza_id,plaza.plaza_name,lane.plaza_id,lane.lane_id
INSERT,PLZ001,Highway Plaza,PLZ001,L1
INSERT,PLZ001,Highway Plaza,PLZ001,L2  # Plaza skipped, lane inserted
```

**Log Output:**
```
Row 2: Tables inserted: ['plaza', 'lane']
Row 3: Tables skipped (already exist): ['plaza']
       Tables inserted: ['lane']
```

**Database Result:**
- Plaza PLZ001: Inserted once
- Lanes L1, L2: Both inserted

### UPDATE

**Behavior:** Updates existing records. **Fails if record doesn't exist.**

**Example:**
```csv
meta.operation,plaza.plaza_id,plaza.merchant_id
UPDATE,PLZ001,MERCH_NEW
```

**Field Mutability:**
- Only `modified_ts` is mutable by default for most tables
- Override required for other fields

### UPDATE with Override

**Behavior:** Allows changes to **immutable fields**.

```csv
meta.operation,meta.override,plaza.plaza_id,plaza.plaza_name
UPDATE,true,PLZ001,Updated Plaza Name
```

**⚠️ Use with caution** - All overrides are logged.

## Field Mutability Rules

### How It Works

1. **INSERT**: All fields can be set
2. **UPDATE (override=false)**: Only mutable fields can change
3. **UPDATE (override=true)**: Any field can change (logged as override)

## Transaction Management

### Row-Level Atomicity

Each CSV row is processed in a database transaction:

```
BEGIN TRANSACTION
  ├─ Process plaza → SUCCESS
  ├─ Process lane → SUCCESS
  └─ Process fare → FAIL
ROLLBACK  # All changes for this row are rolled back
```

**Result:** Row fails, no partial data inserted.

### File Processing Logic

**Partial Success Allowed:**
- Row 1: Success ✅
- Row 2: Failed ❌
- Row 3: Success ✅

**Outcome:** File moved to `processed/` (2 of 3 rows succeeded)

**All Rows Failed:**
- Row 1: Failed ❌
- Row 2: Failed ❌

**Outcome:** File moved to `failed/`

## Logging

### Unified Log File

All logs go to `logs/onboarding.log` with format:

```
[2026-01-23 18:27:51] [runner.py:123] [process_csv_file()] [INFO] [PROCESSING FILE: sample.csv]
```

### Log Levels

- **INFO**: Normal operations (file processing, row success)
- **WARNING**: Skipped records, non-critical issues
- **ERROR**: Failed rows, exceptions, file processing errors

### Row-Level Tracking

Each row logs:
- Row number being processed
- Metadata (JIRA, operation, override)
- Tables inserted/skipped/updated
- Success or failure with full stack trace on error

### File Processing Summary

At end of each file:
```
========================================
FILE PROCESSING COMPLETE: sample.csv
========================================
Status: PARTIAL SUCCESS
Total Rows: 10
Successful: 8 rows
Failed: 2 rows
Successful rows: [2, 3, 4, 5, 6, 7, 8, 9]
Failed rows: [10, 11]
Final location: products/FASTAG_ACQ/processed/sample.csv
========================================
```

### Searching Logs

```bash
# Find all operations for a file
grep "sample.csv" logs/query.log

# Find failures
grep "\[ERROR\]" logs/query.log

# Find skipped records
grep "skipped (already exist)" logs/query.log

# View last 100 lines
tail -100 logs/query.log
```

## Adding a New Product

### 1. Create Product Folder

```bash
mkdir -p products/newproduct/{inbox,processing,processed,failed,logs}
```

### 2. Create Plugin

**File:** `plugins/NewProductPlugin.py`

```python
import sys
from pathlib import Path
sys.path.append(str(Path(__file__).parent.parent))

from common.BasePlugin import BasePlugin
from common.Constants import Operation, ProcessStatus

class NewProductPlugin(BasePlugin):
    
    # Define table names
    TABLE_ENTITY = 'YOUR_TABLE_NAME'
    
    # Define mutable fields
    MUTABLE_FIELDS = {
        TABLE_ENTITY: ['modified_ts']
    }
    
    def __init__(self):
        super().__init__('NEW_PRODUCT')  # Product code
    
    def process_row(self, row, metadata):
        # Implement processing logic
        pass
```

### 3. Register Plugin

**File:** `runner.py`

```python
from plugins.NewProductPlugin import NewProductPlugin

self.plugins = {
    'FASTAG_ACQ': FastagAcqPlugin(),
    'newproduct': NewProductPlugin()  # Folder name as key
}
```

### 4. Test

Drop CSV in `products/newproduct/inbox/` and run `python runner.py`.

## Security Features

- ✅ **Production guardrails** (JIRA validation, submitter validation)
- ✅ **Field-level mutability** control
- ✅ **Override tracking** in logs
- ✅ **Per-product database** isolation (via Vault)
- ✅ **Row-level transactions** prevent partial inserts
- ✅ **Comprehensive logging** for audit trail

## Troubleshooting

### File Stuck in Processing

**Cause:** Script crashed mid-processing.

**Solution:**
```bash
# Check logs
tail -100 logs/query.log

# Move file manually
mv products/FASTAG_ACQ/processing/file.csv products/FASTAG_ACQ/failed/
```

### Row Failures

**Check logs** for error messages:
```bash
grep "ROW.*FAILED" logs/query.log
grep "Full stack trace" logs/query.log
```

Common issues:
- Missing required fields
- Invalid data format
- Database constraints violated
- Immutable field changes without override

### Oracle Connection Issues

**Test connection:**
```bash
python common/TestConnection.py
```

**Common fixes:**
- Verify DB_HOST and DB_PORT
- Check Oracle listener is running
- Verify database name/SID
- Check username/password

## Naming Conventions

- **Files**: PascalCase for Python files (except runner.py)
  - `BasePlugin.py`, `VaultClient.py`, `FastagAcqPlugin.py`
- **Folders**: Lowercase
  - `common/`, `plugins/`, `products/`
- **Product folders**: Lowercase
  - `fastagacq/`, `productb/`
- **Classes**: PascalCase
  - `FastagAcqPlugin`, `VaultClient`
- **Constants**: UPPER_SNAKE_CASE
  - `TABLE_PLAZA`, `MUTABLE_FIELDS`
