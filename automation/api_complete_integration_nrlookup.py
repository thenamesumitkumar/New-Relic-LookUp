#!/usr/bin/env python3
"""
UNIFIED INTEGRATION SCRIPT
API Data Fetch + Service Mapping + New Relic Lookup

- Dynamic output folder name from APIs (APM + APP + App Name)
- Two CSV outputs per app
- Logs kept outside repo
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

# ---------------------------------------------------------------------------
# SSL CONFIG
# ---------------------------------------------------------------------------
try:
    import urllib3
    urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
except Exception:
    pass

try:
    requests.packages.urllib3.disable_warnings()
except Exception:
    pass

# ============================================================================
# CONFIGURATION  ✅ SAFE / NO HARD-CODING
# ============================================================================

# Base paths
SCRIPT_DIR = Path(__file__).resolve().parent
BASE_DIR = SCRIPT_DIR.parent  # repo root

# Segment injected from GitHub Actions (or local fallback)
SEGMENT = os.getenv("SEGMENT", "ASIA")

# CSV directory will be created dynamically inside main()
CSV_DIR = None

# Logs must NOT go into repo
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

# HTTP settings
SSL_VERIFY = False
TIMEOUT = 60

# Timestamp (used in CSV names – you said this is OK)
TIMESTAMP = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
# ============================================================================
# LOGGER
# ============================================================================

class Logger:
    def __init__(self, log_file: Path):
        self.log_file = log_file
        self.messages: List[str] = []

    def log(self, message: str, level: str = "INFO"):
        ts = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]
        formatted = f"[{ts}] [{level:8s}] {message}"
        print(formatted)
        self.messages.append(formatted)

    def save(self):
        try:
            with open(self.log_file, "w", encoding="utf-8") as f:
                f.write("\n".join(self.messages))
        except Exception as e:
            print(f"✗ Failed to save log: {e}")


# Initialize logger (log file name is static per run)
logger = Logger(LOG_DIR / "integration.log")

# ============================================================================
# UTILITY FUNCTIONS
# ============================================================================

def normalize_resource_id(resource_id: Optional[str]) -> str:
    """Normalize resource ID for matching"""
    if not resource_id:
        return ""
    return resource_id.lower().strip()


def extract_meter_category(full_path: Optional[str]) -> str:
    """
    Extract meter category from Azure-style resource path.
    Example:
      /subscriptions/.../providers/Microsoft.Compute/virtualMachines/vm1
      -> Microsoft.Compute/virtualMachines
    """
    if not full_path or not isinstance(full_path, str):
        return ""

    try:
        path_upper = full_path.upper()
        idx = path_upper.find("/PROVIDERS/")
        if idx == -1:
            return ""

        start = idx + len("/PROVIDERS/")
        remaining = full_path[start:]
        parts = remaining.split("/")

        if len(parts) >= 2:
            return f"{parts[0]}/{parts[1]}"
        elif len(parts) == 1:
            return parts[0]
        return ""
    except Exception as e:
        logger.log(f"Meter category parse error: {e}", "ERROR")
        return ""


def find_first_key(obj: Any, target_key: str) -> Optional[Any]:
    """
    Recursively search for the first occurrence of target_key
    in nested dict/list structures.
    """
    if isinstance(obj, dict):
        for k, v in obj.items():
            if k == target_key:
                return v
            found = find_first_key(v, target_key)
            if found is not None:
                return found
    elif isinstance(obj, list):
        for item in obj:
            found = find_first_key(item, target_key)
            if found is not None:
                return found
    return None
# ============================================================================
# API FETCH FUNCTIONS
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
# RESOURCE ↔ SERVICE LOOKUP (SAFE, NO SIDE EFFECTS)
# ============================================================================

def build_resource_service_lookup(apps_data: List[Dict]) -> Dict[str, Dict[str, str]]:
    """
    Build lookup:
      normalized_resource_id -> {
          app_service_name,
          app_service_ci_number,
          Resource Type- Class,
          Process State
      }
    """
    lookup: Dict[str, Dict[str, str]] = {}

    for app in apps_data:
        app_process_state = find_first_key(app, "process_state") or ""
        services = app.get("app_services") or []

        for svc in services:
            svc_name = svc.get("app_service_name", "")
            svc_ci = svc.get("app_service_ci_number", "")
            svc_type = svc.get("app_service_sys_class_name", "")

            svc_state = find_first_key(svc, "process_state")
            process_state = svc_state or app_process_state or ""

            resources = svc.get("resources") or []
            if isinstance(resources, dict):
                resources = list(resources.values())

            for res in resources:
                res_id = res.get("resource_id") or res.get("path_end_resource_id") or ""
                if not res_id:
                    continue

                lookup[normalize_resource_id(res_id)] = {
                    "app_service_name": svc_name,
                    "app_service_ci_number": svc_ci,
                    "Resource Type- Class": svc_type,
                    "Process State": process_state
                }

    logger.log(f"Resource-service lookup built ({len(lookup)} entries)", "LOOKUP")
    return lookup
# ============================================================================
# DATA EXTRACTION
# ============================================================================

def extract_resources_from_mappings(
    mappings_data: List[Dict],
    resource_lookup: Dict[str, Dict[str, str]]
) -> List[Dict[str, Any]]:
    """
    Build app_resources rows enriched with service info
    """
    resources: List[Dict[str, Any]] = []

    for m in mappings_data:
        resource_id = m.get("path_end_resource_id") or ""
        svc = resource_lookup.get(normalize_resource_id(resource_id), {})

        resources.append({
            "Resource Name": m.get("path_end_name", ""),
            "Resource Type": m.get("path_end_sys_class", ""),
            "CI Number": m.get("path_end_ci_number", ""),
            "Business Application": m.get("app_ci_number", ""),
            "Meter Category": extract_meter_category(resource_id),
            "App Code": m.get("app_code", ""),
            "App Name": m.get("app_name", ""),
            "App Cost Center": m.get("app_cost_center", ""),
            "Segment": m.get("segment", ""),
            "Sub Segment": m.get("sub_segment", ""),
            "Resource ID": resource_id,
            "app_service_name": svc.get("app_service_name", ""),
            "app_service_ci_number": svc.get("app_service_ci_number", ""),
            "Resource Type- Class": svc.get("Resource Type- Class", ""),
            "Process State": svc.get("Process State", "")
        })

    logger.log(f"Extracted {len(resources)} resources", "EXTRACT")
    return resources


def extract_services_from_apps(apps_data: List[Dict]) -> List[Dict[str, Any]]:
    """
    Build app_services rows
    """
    services: List[Dict[str, Any]] = []

    for app in apps_data:
        app_code = app.get("mfc_app_code", "")
        parent_ci = app.get("apm_app_id", "")
        app_state = find_first_key(app, "process_state") or ""

        for svc in app.get("app_services", []):
            svc_state = find_first_key(svc, "process_state") or app_state

            services.append({
                "Resource Name": svc.get("app_service_name", ""),
                "Resource Type": svc.get("app_service_sys_class_name", ""),
                "CI Number": svc.get("app_service_ci_number", ""),
                "Application Code": app_code,
                "Environment": svc.get("mfc_env", ""),
                "Parent CI Number": parent_ci,
                "Process State": svc_state
            })

    logger.log(f"Extracted {len(services)} services", "EXTRACT")
    return services


# ============================================================================
# NEW RELIC LOOKUP (IN-MEMORY, CI/CD SAFE)
# ============================================================================

class NewRelicLookup:
    def __init__(self):
        self.cache: Dict[str, str] = {}
        self.api_key = NR_API_KEY

        if not self.api_key:
            logger.log("NR_API_KEY not set – NR enrichment skipped", "WARN")

    def get_account_name(self, resource_name: str) -> str:
        if not resource_name:
            return "NA"

        if resource_name in self.cache:
            return self.cache[resource_name]

        if not self.api_key:
            self.cache[resource_name] = "NA"
            return "NA"

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
                    "API-Key": self.api_key
                },
                json={"query": query},
                timeout=30,
                verify=SSL_VERIFY
            )
            r.raise_for_status()

            entities = (
                r.json()
                .get("data", {})
                .get("actor", {})
                .get("entitySearch", {})
                .get("results", {})
                .get("entities", [])
            )

            account = entities[0]["account"]["name"] if entities else "NA"

        except Exception as e:
            logger.log(f"NR lookup failed for {resource_name}: {e}", "NR_WARN")
            account = "ERROR"

        self.cache[resource_name] = account
        return account

    def enrich_resources(self, resources: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        logger.log("Starting New Relic enrichment", "NR")

        for idx, r in enumerate(resources, start=1):
            nr_account = self.get_account_name(r.get("Resource Name", ""))
            r["New Relic Account"] = nr_account

            if nr_account in ("MLF-PREPROD", "MLF-PROD"):
                r["Infrastructure"] = "Yes"
            else:
                r["Infrastructure"] = "No"

            if idx % 50 == 0:
                logger.log(f"NR processed {idx}/{len(resources)}", "NR")

        logger.log("New Relic enrichment completed", "NR")
        return resources
