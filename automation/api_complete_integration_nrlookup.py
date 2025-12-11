#!/usr/bin/env python3
"""
UNIFIED INTEGRATION SCRIPT - API Data Fetch + New Relic Account Lookup
api_complete_integration_nrlookup.py

Combines:
1. API data fetching (Applications, Mappings, Apps)
2. Resource/Service enrichment with Process State
3. Automatic New Relic account lookup
4. Final CSV generation (2 files, 7 and 16 columns)

Single command execution:
python api_complete_integration_nrlookup.py APP01915 ASIA "Nov 2025"

Output:
- app_services_YYYYMMDD_HHMMSS.csv (7 columns)
- app_resources_YYYYMMDD_HHMMSS_final.csv (16 columns with NR Account)
"""

import sys
import os
import json
import datetime
import argparse
import requests
import pandas as pd
from pathlib import Path
from typing import Dict, List, Any, Optional
import traceback

# SSL config
try:
import urllib3
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
except:
pass

try:
requests.packages.urllib3.disable_warnings()
except:
pass

# ============================================================================
# CONFIGURATION
# ============================================================================

# API Endpoints
API_BASE = "https://application-resource-mapping.platform-insights.dev.cac.corp.aks.manulife.com/api/v1"

API_ENDPOINTS = {
"applications": f"{API_BASE}/application-resources/applications",
"mappings": f"{API_BASE}/application-resources/mappings",
"apps": f"{API_BASE}/apps/"
}

# New Relic API
NR_API_URL = "https://api.newrelic.com/graphql"
NR_API_KEY = os.getenv("NR_API_KEY")

# Request config
SSL_VERIFY = False
TIMEOUT = 60

# Path setup
SCRIPT_DIR = Path(__file__).resolve().parent
BASE_DIR = SCRIPT_DIR.parent
OUTPUT_DIR = BASE_DIR / "output"
CSV_DIR = OUTPUT_DIR / "csv"
LOG_DIR = OUTPUT_DIR / "logs"

CSV_DIR.mkdir(parents=True, exist_ok=True)
LOG_DIR.mkdir(parents=True, exist_ok=True)

TIMESTAMP = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")

# ============================================================================
# LOGGER
# ============================================================================

class Logger:
def __init__(self, log_file: Path):
self.log_file = log_file
self.messages = []

def log(self, message: str, level: str = "INFO"):
ts = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]
formatted = f"[{ts}] [{level:8s}] {message}"
print(formatted)
self.messages.append(formatted)

def save(self):
try:
with open(self.log_file, 'w', encoding='utf-8') as f:
f.write("\n".join(self.messages))
except Exception as e:
print(f"✗ Failed to save log: {e}")

logger = Logger(LOG_DIR / f"integration_{TIMESTAMP}.log")

# ============================================================================
# UTILITY FUNCTIONS
# ============================================================================

def extract_meter_category(full_path: str) -> str:
"""
Extract meter category from full resource path.

Input: /SUBSCRIPTIONS/.../PROVIDERS/MICROSOFT.COMPUTE/DISKS/OSDISK-...
Output: MICROSOFT.COMPUTE/DISKS
"""
if not full_path or not isinstance(full_path, str):
return ''

try:
path_upper = full_path.upper()
providers_idx = path_upper.find('/PROVIDERS/')

if providers_idx == -1:
return ''

start_idx = providers_idx + len('/PROVIDERS/')
remaining = full_path[start_idx:]
parts = remaining.split('/')

if len(parts) >= 2:
return f"{parts[0]}/{parts[1]}"
elif len(parts) == 1:
return parts[0]
else:
return ''

except Exception as e:
logger.log(f"Error parsing meter category: {str(e)[:50]}", "ERROR")
return ''

def normalize_resource_id(resource_id: str) -> str:
"""Normalize resource ID for comparison (lowercase, trim whitespace)"""
if not resource_id:
return ''
return resource_id.lower().strip()

def find_first_key(obj: Any, target_key: str) -> Optional[Any]:
"""
Recursively search for the first occurrence of target_key
anywhere inside a nested dict/list structure.
"""
if isinstance(obj, dict):
for k, v in obj.items():
if k == target_key:
return v
result = find_first_key(v, target_key)
if result is not None:
return result
elif isinstance(obj, list):
for item in obj:
result = find_first_key(item, target_key)
if result is not None:
return result
return None

def build_resource_service_lookup(apps_data: List[Dict]) -> Dict[str, Dict[str, str]]:
"""
Build lookup table:
resource_id -> {
app_service_name,
app_service_ci_number,
Resource Type- Class,
Process State
}
"""
lookup: Dict[str, Dict[str, str]] = {}

for app in apps_data:
app_process_state = find_first_key(app, 'process_state') or ''
services = app.get('app_services') or []

for service in services:
app_service_name = service.get('app_service_name', '')
app_service_ci_number = service.get('app_service_ci_number', '')
app_service_type = service.get('app_service_sys_class_name', '')

service_process_state = find_first_key(service, 'process_state')
process_state = service_process_state or app_process_state or ''

resources = service.get('resources') or []
if isinstance(resources, dict):
resources = list(resources.values())

for resource in resources:
resource_id = resource.get('resource_id') or resource.get('path_end_resource_id') or ''

if resource_id:
normalized_id = normalize_resource_id(resource_id)
lookup[normalized_id] = {
'app_service_name': app_service_name,
'app_service_ci_number': app_service_ci_number,
'Resource Type- Class': app_service_type,
'Process State': process_state
}

logger.log(f"Built resource lookup: {len(lookup)} resource IDs indexed", "LOOKUP")
return lookup

# ============================================================================
# API FETCHING
# ============================================================================

def fetch_applications_api() -> List[Dict]:
"""Fetch API #1: Applications"""
logger.log("Fetching API #1: Applications", "FETCH")
try:
response = requests.get(
API_ENDPOINTS["applications"],
params={"format": "json"},
timeout=TIMEOUT,
verify=SSL_VERIFY
)
if response.status_code != 200:
logger.log(f"Status {response.status_code}", "ERROR")
return []

data = response.json()
if isinstance(data, dict):
data = [data]
if not isinstance(data, list):
data = []

logger.log(f"✓ Got {len(data)} applications", "FETCH")
return data
except Exception as e:
logger.log(f"Error: {str(e)[:100]}", "ERROR")
return []

def fetch_mappings_api(app_code: str, segment: str, month: str) -> List[Dict]:
"""Fetch API #2: Mappings - PRIMARY DATA SOURCE"""
logger.log(f"Fetching API #2: Mappings (app={app_code}, seg={segment})", "FETCH")
try:
response = requests.get(
API_ENDPOINTS["mappings"],
params={
"app_code": app_code,
"segment": segment,
"month": month,
"format": "json"
},
timeout=TIMEOUT,
verify=SSL_VERIFY
)
if response.status_code != 200:
logger.log(f"Status {response.status_code}", "ERROR")
return []

data = response.json()
if isinstance(data, dict):
data = [data]
if not isinstance(data, list):
data = []

logger.log(f"✓ Got {len(data)} mappings", "FETCH")
return data
except Exception as e:
logger.log(f"Error: {str(e)[:100]}", "ERROR")
return []

def fetch_apps_api(app_code: str) -> List[Dict]:
"""Fetch API #3: Apps - For services and resource details"""
logger.log(f"Fetching API #3: Apps (app={app_code})", "FETCH")
try:
response = requests.get(
API_ENDPOINTS["apps"],
params={
"mfc_app_code": app_code,
"format": "json",
"include_resource": "true"
},
timeout=TIMEOUT,
verify=SSL_VERIFY
)
if response.status_code != 200:
logger.log(f"Status {response.status_code}", "ERROR")
return []

data = response.json()
if isinstance(data, dict):
data = [data]
if not isinstance(data, list):
data = []

logger.log(f"✓ Got {len(data)} app(s)", "FETCH")
return data
except Exception as e:
logger.log(f"Error: {str(e)[:100]}", "ERROR")
return []

# ============================================================================
# DATA EXTRACTION
# ============================================================================

def extract_resources_from_mappings(
mappings_data: List[Dict],
resource_lookup: Dict[str, Dict[str, str]]
) -> List[Dict]:
"""Extract resources from Mappings API with service enrichment"""
logger.log(f"Extracting {len(mappings_data)} resources from Mappings API", "EXTRACT")

resources: List[Dict[str, Any]] = []
matched_count = 0
unmatched_count = 0

for mapping in mappings_data:
ci_number = mapping.get('path_end_ci_number') or ''
resource_id = mapping.get('path_end_resource_id') or ''
full_path = resource_id

meter_category = extract_meter_category(full_path)

# Defaults for service enrichment
app_service_name = ''
app_service_ci_number = ''
app_service_resource_type = ''
app_service_process_state = ''

# Look up service information by RESOURCE ID
if resource_id:
normalized_id = normalize_resource_id(resource_id)
svc_info = resource_lookup.get(normalized_id)
if svc_info:
app_service_name = svc_info.get('app_service_name', '')
app_service_ci_number = svc_info.get('app_service_ci_number', '')
app_service_resource_type = svc_info.get('Resource Type- Class', '')
app_service_process_state = svc_info.get('Process State', '')
matched_count += 1
else:
unmatched_count += 1
else:
unmatched_count += 1

# Create resource record
resource = {
'Resource Name': mapping.get('path_end_name') or '',
'Resource Type': mapping.get('path_end_sys_class') or '',
'CI Number': ci_number,
'Business Application': mapping.get('app_ci_number') or '',
'Meter Category': meter_category,
'App Code': mapping.get('app_code') or '',
'App Name': mapping.get('app_name') or '',
'App Cost Center': mapping.get('app_cost_center') or '',
'Segment': mapping.get('segment') or '',
'Sub Segment': mapping.get('sub_segment') or '',
'Resource ID': mapping.get('path_end_resource_id') or '',
'app_service_name': app_service_name,
'app_service_ci_number': app_service_ci_number,
'Resource Type- Class': app_service_resource_type,
'Process State': app_service_process_state
}
resources.append(resource)

logger.log(f"✓ Extracted {len(resources)} resources", "EXTRACT")
logger.log(f" Matched with service: {matched_count}, Unmatched: {unmatched_count}", "EXTRACT")

return resources

def extract_services_from_apps(apps_data: List[Dict]) -> List[Dict]:
"""Extract services from Apps API"""
logger.log(f"Extracting services from {len(apps_data)} app(s)", "EXTRACT")

all_services: List[Dict[str, Any]] = []

for app in apps_data:
app_code = app.get('mfc_app_code') or ''
app_ci_number = app.get('apm_app_id') or ''
app_process_state = find_first_key(app, 'process_state') or ''
services = app.get('app_services') or []

logger.log(f" App {app_code}: Processing {len(services)} services", "EXTRACT")

for service in services:
service_process_state = find_first_key(service, 'process_state')
process_state = (service_process_state or app_process_state or '')

svc = {
'Resource Name': service.get('app_service_name') or '',
'Resource Type': service.get('app_service_sys_class_name') or '',
'CI Number': service.get('app_service_ci_number') or '',
'Application Code': app_code,
'Environment': service.get('mfc_env') or '',
'Parent CI Number': app_ci_number or '',
'Process State': process_state
}
all_services.append(svc)

logger.log(f"✓ Extracted {len(all_services)} services", "EXTRACT")
return all_services

# ============================================================================
# NEW RELIC ACCOUNT LOOKUP
# ============================================================================

class NewRelicLookup:
"""Handles New Relic API queries for account information"""

def __init__(self):
self.account_cache = {}
self.nr_api_key = NR_API_KEY

if not self.nr_api_key:
logger.log("WARNING: NR_API_KEY not set. New Relic lookups will fail.", "WARNING")

def get_item_details(self, item_name: str) -> str:
"""
Given a resource name, query New Relic entitySearch and return account name.
Returns: account_name or "NA" (not found) or "ERROR" (API failure)
"""
if not item_name:
return "NA"

# Return from cache if already looked up
if item_name in self.account_cache:
return self.account_cache[item_name]

if not self.nr_api_key:
return "ERROR"

# Build GraphQL query
query = f"""
{{
actor {{
entitySearch(queryBuilder: {{name: "{item_name}", domain: INFRA}}) {{
results {{
entities {{
account {{
name
}}
}}
}}
}}
}}
}}
"""

payload = {
"query": query,
"variables": ""
}

headers = {
"Content-Type": "application/json",
"API-Key": self.nr_api_key
}

try:
response = requests.post(
NR_API_URL,
headers=headers,
json=payload,
timeout=30,
verify=SSL_VERIFY
)
response.raise_for_status()

data = response.json()
entities = data["data"]["actor"]["entitySearch"]["results"]["entities"]

if entities:
account_name = entities[0]["account"]["name"]
else:
account_name = "NA"

except Exception as e:
logger.log(f"New Relic lookup failed for '{item_name}': {str(e)[:80]}", "WARN_NR")
account_name = "ERROR"

# Cache result
self.account_cache[item_name] = account_name
return account_name

def enrich_resources_with_nr_account(self, resources: List[Dict]) -> List[Dict]:
"""Enrich resources with New Relic account information"""
logger.log(f"Looking up New Relic accounts for {len(resources)} resources", "NR_LOOKUP")

for idx, resource in enumerate(resources):
item_name = resource.get('Resource Name', '')
if item_name:
account = self.get_item_details(item_name)
resource['New Relic Account'] = account
else:
resource['New Relic Account'] = "NA"

# Progress indicator
if (idx + 1) % 50 == 0:
logger.log(f" Processed {idx + 1}/{len(resources)} resources", "NR_PROGRESS")

logger.log(f"✓ New Relic account enrichment complete", "NR_LOOKUP")
logger.log(f" Cache hits: {len(self.account_cache)}", "NR_LOOKUP")
return resources

# ============================================================================
# CSV GENERATION
# ============================================================================

def generate_csv(data: List[Dict], columns: List[str], filename: Path) -> int:
"""Generate CSV file"""
logger.log(f"Generating {filename.name}...", "CSV_GEN")

try:
if not data:
logger.log("No data to write", "WARNING")
df = pd.DataFrame(columns=columns)
else:
df = pd.DataFrame(data, columns=columns)

df.to_csv(filename, index=False, encoding='utf-8')
logger.log(f"✓ Saved {len(df)} rows to {filename.name}", "CSV_GEN")
return len(df)

except Exception as e:
logger.log(f"Error: {str(e)[:100]}", "ERROR")
return 0

# ============================================================================
# MAIN
# ============================================================================

def main():
print("\n" + "=" * 80)
print("UNIFIED INTEGRATION - API Data Fetch + New Relic Account Lookup")
print("=" * 80 + "\n")

parser = argparse.ArgumentParser(description="Complete API Integration with NR Lookup")
parser.add_argument("app_code", help="Application code (e.g., APP01915)")
parser.add_argument("segment", help="Segment (e.g., ASIA)")
parser.add_argument("month", help="Month (e.g., Nov 2025)")

args = parser.parse_args()

app_code = args.app_code
segment = args.segment
month = args.month

logger.log(f"Starting: app={app_code}, segment={segment}, month={month}", "START")
print(f"Parameters: app_code={app_code}, segment={segment}, month={month}\n")

try:
# PHASE 1: FETCH FROM APIs
print("-" * 80)
print("PHASE 1: FETCH FROM APIs")
print("-" * 80 + "\n")

applications_data = fetch_applications_api()
mappings_data = fetch_mappings_api(app_code, segment, month)
apps_data = fetch_apps_api(app_code)

print(f"✓ Applications: {len(applications_data)}")
print(f"✓ Mappings: {len(mappings_data)}")
print(f"✓ Apps: {len(apps_data)}\n")

# PHASE 2: BUILD RESOURCE ID LOOKUP
print("-" * 80)
print("PHASE 2: BUILD RESOURCE ID LOOKUP")
print("-" * 80 + "\n")

resource_lookup = build_resource_service_lookup(apps_data)
print(f"✓ Resource lookup built: {len(resource_lookup)} resource IDs indexed\n")

# PHASE 3: EXTRACT DATA
print("-" * 80)
print("PHASE 3: EXTRACT DATA")
print("-" * 80 + "\n")

resources_data = extract_resources_from_mappings(mappings_data, resource_lookup)
services_data = extract_services_from_apps(apps_data)

print(f"✓ Resources: {len(resources_data)} (with service enrichment)")
print(f"✓ Services: {len(services_data)}\n")

# PHASE 4: NEW RELIC ACCOUNT LOOKUP
print("-" * 80)
print("PHASE 4: NEW RELIC ACCOUNT LOOKUP")
print("-" * 80 + "\n")

nr_lookup = NewRelicLookup()
resources_data = nr_lookup.enrich_resources_with_nr_account(resources_data)
print(f"✓ New Relic account enrichment complete\n")

# PHASE 5: GENERATE CSVs
print("-" * 80)
print("PHASE 5: GENERATE FINAL CSVs")
print("-" * 80 + "\n")

# app_services.csv - 7 COLUMNS
services_cols = [
'Resource Name', 'Resource Type', 'CI Number',
'Application Code', 'Environment', 'Parent CI Number',
'Process State'
]
services_file = CSV_DIR / f"app_services_{TIMESTAMP}.csv"
services_count = generate_csv(services_data, services_cols, services_file)

# app_resources_final.csv - 16 COLUMNS (with New Relic Account)
resources_cols = [
'Resource Name', 'Resource Type', 'CI Number', 'Business Application',
'Meter Category', 'App Code', 'App Name', 'App Cost Center',
'Segment', 'Sub Segment', 'Resource ID',
'app_service_name', 'app_service_ci_number',
'Resource Type- Class', 'Process State', 'New Relic Account'
]
resources_file = CSV_DIR / f"app_resources_{TIMESTAMP}_final.csv"
resources_count = generate_csv(resources_data, resources_cols, resources_file)

# SUMMARY
print("\n" + "=" * 80)
print("✅ COMPLETE")
print("=" * 80)

print(f"\n✓ Final CSV Files:")
print(f" {services_file.name}")
print(f" Rows: {services_count}, Columns: 7")
print(f"\n {resources_file.name}")
print(f" Rows: {resources_count}, Columns: 16 (including New Relic Account)")

print(f"\n✓ Enriched Fields:")
print(f" ✓ Meter Category: Extracted from resource ID path")
print(f" ✓ app_service_name: Matched from Apps API via resource ID")
print(f" ✓ app_service_ci_number: Matched from Apps API via resource ID")
print(f" ✓ Resource Type- Class: Service Resource Type from Apps API")
print(f" ✓ Process State (resources): Service Process State from Apps API")
print(f" ✓ Process State (services): From Apps API app/services JSON")
print(f" ✓ New Relic Account: Queried from New Relic API by Resource Name")

print(f"\n✓ Location: {CSV_DIR}")
print(f"✓ Log: {LOG_DIR / f'integration_{TIMESTAMP}.log'}\n")

logger.log(
f"SUCCESS: {resources_count} resources (16 cols with NR Account), "
f"{services_count} services (7 cols)",
"COMPLETE"
)
logger.save()

return 0

except Exception as e:
logger.log(f"ERROR: {type(e).__name__}: {str(e)}", "ERROR")
logger.save()
print(f"\n✗ FATAL ERROR: {e}")
traceback.print_exc()
return 1

if __name__ == "__main__":
sys.exit(main())
