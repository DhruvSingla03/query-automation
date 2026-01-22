# Partner Onboarding System

Automated partner onboarding system using **plugin architecture**, **multi-table CSV format**, and **Vault AppRole** authentication.

## Overview

This system processes onboarding requests via **CSV files** where each row contains data for multiple related tables (plaza, concessionaire, lane, fare, vehicle mapping, user mapping). It supports **INSERT** and **UPDATE** operations with **field-level mutability control**.

## Quick Start

### 1. Setup Environment

```bash
cd onboarding
cp .env.template .env
# Edit .env with your Vault credentials
```

### 2. Install Dependencies

```bash
pip install -r requirements.txt
```

### 3. Configure Vault

Each product team must create a Vault policy:

```bash
vault policy write fastag-db-read - <<EOF
path "secret/data/fastag/database" {
  capabilities = ["read"]
}
EOF
```

Store database credentials in Vault:

```bash
vault kv put secret/fastag/database \
  host=fastag-db.example.com \
  port=3306 \
  database=fastag_production \
  username=fastag_user \
  password=<password>
```

### 4. Run the System

```bash
python runner.py
```

The runner scans `inbox/` for CSV files and processes them automatically.

## CSV Format

### Structure

Each CSV row contains:
- **Metadata columns** (mandatory): `meta.product`, `meta.submitted_by`, `meta.jira`, `meta.operation`, `meta.override`
- **Table-specific columns** (prefixed): `plaza.*`, `conc.*`, `lane.*`, `fare.*`, `vmap.*`, `umap.*`

### Example

```csv
meta.product,meta.submitted_by,meta.jira,meta.operation,meta.override,plaza.plaza_id,plaza.plaza_name,conc.concessionaire_id,conc.concessionaire_name,...
FASTAG_ACQ,olm_id,JIRA-12345,INSERT,false,P001,Mumbai Plaza,C001,ABC Corp,...
```

### Metadata Fields

| Field | Required | Description |
|-------|----------|-------------|
| meta.product | Yes | Product code (FASTAG_ACQ) |
| meta.submitted_by | Yes | User who submitted |
| meta.jira | Yes | JIRA/SNOW ticket (e.g., JIRA-12345) |
| meta.operation | Yes | INSERT or UPDATE |
| meta.override | No | true/false (default: false) |

### Table Prefixes

- `plaza.*` - Plaza details (plaza_id, type, plaza_name, geocode, etc.)
- `conc.*` - Concessionaire details (concessionaire_id, concessionaire_name, pan_no, etc.)
- `lane.*` - Lane details (plaza_id, lane_id, directions, lane_status, etc.)
- `fare.*` - Fare details (fare_id, plaza_id, avc_id, single_journey_fare, etc.)
- `vmap.*` - Vehicle class mapping (plaza_id, mvc_id, avc_id, tvc_id, etc.)
- `umap.*` - User mapping (user_id, role, status, etc.)

## Field Mutability

### Immutable Fields (cannot change without override=true)

**Plaza**: plaza_id, type, plaza_type, plaza_name, geocode, resp_pay_url, etc.  
**Concessionaire**: concessionaire_id, onboarding_date, concessionaire_status, created_ts  
**Lane**: plaza_id, lane_id, directions, lane_status, lane_mode, lane_type  
**Fare**: fare_id, plaza_id, avc_id, single_journey_fare, return_journey_fare  
**Vehicle Mapping**: plaza_id, mvc_id, avc_id, tvc_id  
**User Mapping**: user_id, role, status, plaza_id, concessionaire_id

### Mutable Fields (can always change)

**Plaza**: internal_account, settlement_account, biller_code, merchant_id, modified_ts  
**Concessionaire**: Most fields except immutable ones (name, contact info, account_info, etc.)  
**Lane**: modified_ts, merchant_vpa  
**Fare**: modified_ts  
**Vehicle Mapping**: modified_ts  
**User Mapping**: modified_ts

## Operations

### INSERT

- Creates new record
- Fails if record already exists
- All provided fields are set

```csv
meta.operation,plaza.plaza_id,plaza.plaza_name,...
INSERT,P001,New Plaza,...
```

### UPDATE

- Updates existing record
- Fails if record doesn't exist
- **Only mutable fields** can be changed (unless override=true)
- Validates field mutability
- Detects and logs changes

```csv
meta.operation,meta.override,plaza.plaza_id,plaza.merchant_id,...
UPDATE,false,P001,MERCH_NEW,...
```

### UPDATE with Override

- Allows changes to **immutable fields**
- Use with caution (logged in audit)

```csv
meta.operation,meta.override,plaza.plaza_id,plaza.plaza_name,plaza.geocode,...
UPDATE,true,P001,Updated Plaza Name,New Coordinates,...
```

## Audit Logging

All operations are logged to `logs/audit.log` in **JSON Lines format**.

### Log Entry Example

```json
{"timestamp": "2026-01-19T15:13:09+05:30", "jira": "JIRA-12345", "product": "FASTAG_ACQ", "submitted_by": "olm_id", "operation": "UPDATE", "table": "plaza", "record_id": "P001", "override_used": false, "changes": {"merchant_id": {"old": "M123", "new": "M456", "mutable": true}}, "status": "SUCCESS"}
```

### Searching Logs

```bash
# Find all operations for a JIRA
grep '"jira": "JIRA-12345"' logs/audit.log

# Find failures
grep '"status": "FAILED"' logs/audit.log

# Find operations with override
grep '"override_used": true' logs/audit.log

# Use jq for complex queries
cat logs/audit.log | jq 'select(.operation == "UPDATE" and .override_used == true)'
```

## Directory Structure

```
onboarding/
├── runner.py                 # Main orchestrator
├── common/                   # Shared utilities
│   ├── vault_client.py      # Vault AppRole auth
│   ├── audit.py             # File-based audit logging
│   └── base_plugin.py       # Plugin interface
├── plugins/                  # Product-specific logic
│   └── fastag_acq.py        # FASTAG plugin
├── inbox/                    # Drop CSV files here
│   └── FASTAG_ACQ/
├── processing/               # Files being processed
├── processed/                # Successfully completed
├── failed/                   # Failed files
└── logs/                     # Execution and audit logs
    ├── audit.log            # JSON Lines audit log
    └── JIRA-XXXXX.log       # Per-ticket logs
```

## Adding a New Product

1. Create plugin file: `plugins/your_product.py`
2. Inherit from `BasePlugin`
3. Implement:
   - `get_mutable_fields(table)` - Define mutable fields per table
   - `process_row(row, metadata)` - Main processing logic
4. Register in `runner.py`
5. Add Vault path to `.env`

## Security Features

- ✅ AppRole authentication with auto-renewal
- ✅ Production guardrails (jira validation, submitter allowlist)
- ✅ Field-level mutability control
- ✅ Override tracking
- ✅ Per-product database isolation
- ✅ Comprehensive audit logging

## Troubleshooting

### File stuck in processing/

Check logs:
- `logs/audit.log` - Audit trail
- `logs/JIRA-XXXXX.log` - Per-ticket details

Move to failed manually if needed:
```bash
mv processing/file.csv failed/
```

### Vault authentication fails

Verify environment variables:
```bash
echo $VAULT_ADDR
echo $VAULT_ROLE_ID
```

Test authentication:
```python
from common.vault_client import VaultClient
vault = VaultClient()
```

### Immutable field change rejected

Error: `Cannot update immutable fields without override=true: plaza_name`

Solution: Either remove the field from CSV or set `meta.override=true`

## Production Deployment

1. Copy project to `/srv/onboarding/`
2. Create systemd service
3. Set ENV=production in environment
4. Configure file permissions
5. Setup log rotation for `logs/audit.log`

See deployment guide for details.
