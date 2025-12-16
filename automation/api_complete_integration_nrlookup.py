#!/usr/bin/env python3
"""
UNIFIED INTEGRATION SCRIPT - API Data Fetch + New Relic Account Lookup
api_complete_integration_nrlookup.py

ADDED:
- New column: Infrastructure
- Logic:
    If New Relic Account in (MLF-PREPROD, MLF-PROD) → Infrastructure = Yes
    Else → Infrastructure = No
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

# Base paths
SCRIPT_DIR = Path(__file__).resolve().parent
BASE_DIR = SCRIPT_DIR.parent  # Repository root

# Segment is passed from GitHub Actions (fallback for local runs)
SEGMENT = os.getenv("SEGMENT", "ASIA")

# CSV output directory will be created dynamically inside main()
CSV_DIR = None

# Keep logs OUTSIDE the repo (prevents extra files in GitHub)
LOG_DIR = Path("/tmp/sk_logs")
LOG_DIR.mkdir(parents=True, exist_ok=True)

# API configuration
API_BASE = "https://application-resource-mapping.platform-insights.dev.cac.corp.aks.manulife.com/api/v1"

API_ENDPOINTS = {
    "applications": f"{API_BASE}/application-resources/applications",
    "mappings": f"{API_BASE}/application-resources/mappings",
    "apps": f"{API_BASE}/apps/"
}

# New Relic configuration
NR_API_URL = "https://api.newrelic.com/graphql"
NR_API_KEY = os.getenv("NR_API_KEY")

# Request settings
SSL_VERIFY = False
TIMEOUT = 60

# Timestamp for output files
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
        return ''
    except Exception as e:
        logger.log(f"Meter category parse error: {str(e)}", "ERROR")
        return ''

def normalize_resource_id(resource_id: str) -> str:
    return resource_id.lower().strip() if resource_id else ''

def find_first_key(obj: Any, target_key: str) -> Optional[Any]:
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
# ============================================================================
# RESOURCE ↔ SERVICE LOOKUP
# ============================================================================

def build_resource_service_lookup(apps_data: List[Dict]) -> Dict[str, Dict[str, str]]:
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
                    lookup[normalize_resource_id(resource_id)] = {
                        'app_service_name': app_service_name,
                        'app_service_ci_number': app_service_ci_number,
                        'Resource Type- Class': app_service_type,
                        'Process State': process_state
                    }

    logger.log(f"Built resource lookup with {len(lookup)} entries", "LOOKUP")
    return lookup

# ============================================================================
# API FETCHING
# ============================================================================

def fetch_applications_api() -> List[Dict]:
    logger.log("Fetching Applications API", "FETCH")
    try:
        r = requests.get(
            API_ENDPOINTS["applications"],
            params={"format": "json"},
            timeout=TIMEOUT,
            verify=SSL_VERIFY
        )
        r.raise_for_status()
        data = r.json()
        return data if isinstance(data, list) else [data]
    except Exception as e:
        logger.log(f"Applications API error: {e}", "ERROR")
        return []

def fetch_mappings_api(app_code: str, segment: str, month: str) -> List[Dict]:
    logger.log("Fetching Mappings API", "FETCH")
    try:
        r = requests.get(
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
        r.raise_for_status()
        data = r.json()
        return data if isinstance(data, list) else [data]
    except Exception as e:
        logger.log(f"Mappings API error: {e}", "ERROR")
        return []

def fetch_apps_api(app_code: str) -> List[Dict]:
    logger.log("Fetching Apps API", "FETCH")
    try:
        r = requests.get(
            API_ENDPOINTS["apps"],
            params={
                "mfc_app_code": app_code,
                "format": "json",
                "include_resource": "true"
            },
            timeout=TIMEOUT,
            verify=SSL_VERIFY
        )
        r.raise_for_status()
        data = r.json()
        return data if isinstance(data, list) else [data]
    except Exception as e:
        logger.log(f"Apps API error: {e}", "ERROR")
        return []

# ============================================================================
# DATA EXTRACTION
# ============================================================================

def extract_resources_from_mappings(
    mappings_data: List[Dict],
    resource_lookup: Dict[str, Dict[str, str]]
) -> List[Dict]:

    resources: List[Dict[str, Any]] = []

    for m in mappings_data:
        resource_id = m.get('path_end_resource_id') or ''
        norm_id = normalize_resource_id(resource_id)

        svc = resource_lookup.get(norm_id, {})

        resources.append({
            'Resource Name': m.get('path_end_name', ''),
            'Resource Type': m.get('path_end_sys_class', ''),
            'CI Number': m.get('path_end_ci_number', ''),
            'Business Application': m.get('app_ci_number', ''),
            'Meter Category': extract_meter_category(resource_id),
            'App Code': m.get('app_code', ''),
            'App Name': m.get('app_name', ''),
            'App Cost Center': m.get('app_cost_center', ''),
            'Segment': m.get('segment', ''),
            'Sub Segment': m.get('sub_segment', ''),
            'Resource ID': resource_id,
            'app_service_name': svc.get('app_service_name', ''),
            'app_service_ci_number': svc.get('app_service_ci_number', ''),
            'Resource Type- Class': svc.get('Resource Type- Class', ''),
            'Process State': svc.get('Process State', '')
        })

    logger.log(f"Extracted {len(resources)} resources", "EXTRACT")
    return resources

def extract_services_from_apps(apps_data: List[Dict]) -> List[Dict]:
    services: List[Dict[str, Any]] = []

    for app in apps_data:
        app_code = app.get('mfc_app_code', '')
        parent_ci = app.get('apm_app_id', '')
        app_state = find_first_key(app, 'process_state') or ''

        for svc in app.get('app_services', []):
            svc_state = find_first_key(svc, 'process_state') or app_state

            services.append({
                'Resource Name': svc.get('app_service_name', ''),
                'Resource Type': svc.get('app_service_sys_class_name', ''),
                'CI Number': svc.get('app_service_ci_number', ''),
                'Application Code': app_code,
                'Environment': svc.get('mfc_env', ''),
                'Parent CI Number': parent_ci,
                'Process State': svc_state
            })

    logger.log(f"Extracted {len(services)} services", "EXTRACT")
    return services
# ============================================================================
# NEW RELIC ACCOUNT LOOKUP + INFRASTRUCTURE LOGIC
# ============================================================================

class NewRelicLookup:
    def __init__(self):
        self.cache: Dict[str, str] = {}
        self.nr_api_key = NR_API_KEY

        if not self.nr_api_key:
            logger.log("NR_API_KEY not set. NR lookups disabled.", "WARNING")

    def get_account_name(self, resource_name: str) -> str:
        if not resource_name:
            return "NA"

        if resource_name in self.cache:
            return self.cache[resource_name]

        if not self.nr_api_key:
            return "ERROR"

        query = f"""
        {{
          actor {{
            entitySearch(queryBuilder: {{ name: "{resource_name}", domain: INFRA }}) {{
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

        try:
            r = requests.post(
                NR_API_URL,
                headers={
                    "Content-Type": "application/json",
                    "API-Key": self.nr_api_key
                },
                json={"query": query},
                timeout=30,
                verify=SSL_VERIFY
            )
            r.raise_for_status()
            entities = r.json()["data"]["actor"]["entitySearch"]["results"]["entities"]
            account = entities[0]["account"]["name"] if entities else "NA"

        except Exception as e:
            logger.log(f"NR lookup failed for {resource_name}: {e}", "NR_WARN")
            account = "ERROR"

        self.cache[resource_name] = account
        return account

    def enrich_resources(self, resources: List[Dict]) -> List[Dict]:
        logger.log("Starting New Relic enrichment", "NR")

        for i, r in enumerate(resources, start=1):
            nr_account = self.get_account_name(r.get('Resource Name', ''))
            r['New Relic Account'] = nr_account

            # 🔥 Infrastructure column logic
            if nr_account in ("MLF-PREPROD", "MLF-PROD"):
                r['Infrastructure'] = "Yes"
            else:
                r['Infrastructure'] = "No"

            if i % 50 == 0:
                logger.log(f"NR processed {i}/{len(resources)}", "NR")

        logger.log("New Relic enrichment complete", "NR")
        return resources
# ============================================================================
# CSV GENERATION
# ============================================================================

def generate_csv(data: List[Dict], columns: List[str], filename: Path) -> int:
    logger.log(f"Writing {filename.name}", "CSV")
    df = pd.DataFrame(data, columns=columns)
    df.to_csv(filename, index=False, encoding="utf-8")
    logger.log(f"Saved {len(df)} rows", "CSV")
    return len(df)

# ============================================================================
# MAIN
# ============================================================================

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("app_code")
    parser.add_argument("segment")
    parser.add_argument("month")
    args = parser.parse_args()

    logger.log(f"Run started for {args.app_code}", "START")

    # Fetch APIs
    apps = fetch_apps_api(args.app_code)
    mappings = fetch_mappings_api(args.app_code, args.segment, args.month)
        # ------------------------------------------------------------
    # Derive App Name & APM number dynamically from API (SAFE)
    # ------------------------------------------------------------
    app_name = ""
    apm_number = ""

    if mappings and isinstance(mappings, list):
        first = mappings[0]
        app_name = (first.get("app_name") or "").strip()
        apm_number = (first.get("app_ci_number") or "").strip()

    # Fallbacks (never fail)
    if not app_name:
        app_name = args.app_code

    if not apm_number:
        apm_number = "APM000000"

    # Build output folder name dynamically
    folder_name = f"{apm_number} - {args.app_code} - {app_name}"

    global CSV_DIR
    CSV_DIR = BASE_DIR / SEGMENT / folder_name
    CSV_DIR.mkdir(parents=True, exist_ok=True)

    # Build lookup
    lookup = build_resource_service_lookup(apps)

    # Extract data
    resources = extract_resources_from_mappings(mappings, lookup)
    services = extract_services_from_apps(apps)

    # NR + Infrastructure enrichment
    nr = NewRelicLookup()
    resources = nr.enrich_resources(resources)

    # CSVs
    services_cols = [
        'Resource Name', 'Resource Type', 'CI Number',
        'Application Code', 'Environment',
        'Parent CI Number', 'Process State'
    ]

    resources_cols = [
        'Resource Name', 'Resource Type', 'CI Number',
        'Business Application', 'Meter Category',
        'App Code', 'App Name', 'App Cost Center',
        'Segment', 'Sub Segment', 'Resource ID',
        'app_service_name', 'app_service_ci_number',
        'Resource Type- Class', 'Process State',
        'New Relic Account', 'Infrastructure'
    ]

    services_file = CSV_DIR / f"app_services_{TIMESTAMP}.csv"
    resources_file = CSV_DIR / f"app_resources_{TIMESTAMP}_final.csv"

    generate_csv(services, services_cols, services_file)
    generate_csv(resources, resources_cols, resources_file)

    logger.log("Run completed successfully", "COMPLETE")
    logger.save()

    print("\n✅ CSVs generated:")
    print(f" - {services_file.name}")
    print(f" - {resources_file.name}")

if __name__ == "__main__":
    sys.exit(main())
