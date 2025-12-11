#!/usr/bin/env python3
"""
UNIFIED INTEGRATION SCRIPT - API Data Fetch + New Relic Account Lookup

Usage:
  python api_complete_integration_nrlookup.py APP01915 ASIA "Nov 2025"

Directory layout created:

  output/
    <SEGMENT>/
      <APP_CODE>-<APP_NAME>/
        app_services.csv      (7 columns)
        app_resources.csv     (16 columns, incl. New Relic Account)
        integration_YYYYMMDD_HHMMSS.log
"""

import sys
import os
import datetime
import argparse
import traceback
from pathlib import Path
from typing import Dict, List, Any, Optional

import requests
import pandas as pd

# -----------------------------------------------------------------------------
# SSL CONFIG
# -----------------------------------------------------------------------------
try:
    import urllib3

    urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
except Exception:
    pass

try:
    requests.packages.urllib3.disable_warnings()  # type: ignore[attr-defined]
except Exception:
    pass

# -----------------------------------------------------------------------------
# CONFIGURATION
# -----------------------------------------------------------------------------

API_BASE = (
    "https://application-resource-mapping.platform-insights.dev.cac.corp.aks.manulife.com/api/v1"
)

API_ENDPOINTS = {
    "applications": f"{API_BASE}/application-resources/applications",
    "mappings": f"{API_BASE}/application-resources/mappings",
    "apps": f"{API_BASE}/apps/",
}

NR_API_URL = "https://api.newrelic.com/graphql"
NR_API_KEY = os.getenv("NR_API_KEY")

SSL_VERIFY = False
TIMEOUT = 60

SCRIPT_DIR = Path(__file__).resolve().parent
BASE_DIR = SCRIPT_DIR.parent
OUTPUT_ROOT = BASE_DIR / "output"

TIMESTAMP = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")


# -----------------------------------------------------------------------------
# LOGGER
# -----------------------------------------------------------------------------

class Logger:
    def __init__(self, log_file: Path):
        self.log_file = log_file
        self.messages: List[str] = []

    def log(self, message: str, level: str = "INFO") -> None:
        ts = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]
        formatted = f"[{ts}] [{level:8s}] {message}"
        print(formatted)
        self.messages.append(formatted)

    def save(self) -> None:
        try:
            self.log_file.parent.mkdir(parents=True, exist_ok=True)
            with open(self.log_file, "w", encoding="utf-8") as f:
                f.write("\n".join(self.messages))
        except Exception as e:
            print(f"✗ Failed to save log: {e}")


# Temporary logger, will be reassigned in main() once app/segment are known
logger = Logger(OUTPUT_ROOT / "logs" / f"integration_{TIMESTAMP}.log")


# -----------------------------------------------------------------------------
# UTILITY FUNCTIONS
# -----------------------------------------------------------------------------

def extract_meter_category(full_path: str) -> str:
    """
    Extract meter category from full resource path.

    Input: /SUBSCRIPTIONS/.../PROVIDERS/MICROSOFT.COMPUTE/DISKS/OSDISK-...
    Output: MICROSOFT.COMPUTE/DISKS
    """
    if not full_path or not isinstance(full_path, str):
        return ""

    try:
        path_upper = full_path.upper()
        providers_idx = path_upper.find("/PROVIDERS/")

        if providers_idx == -1:
            return ""

        start_idx = providers_idx + len("/PROVIDERS/")
        remaining = full_path[start_idx:]
        parts = remaining.split("/")

        if len(parts) >= 2:
            return f"{parts[0]}/{parts[1]}"
        if len(parts) == 1:
            return parts[0]
        return ""
    except Exception as e:
        logger.log(f"Error parsing meter category: {str(e)[:50]}", "ERROR")
        return ""


def normalize_resource_id(resource_id: str) -> str:
    """Normalize resource ID for comparison (lowercase, trim whitespace)."""
    if not resource_id:
        return ""
    return resource_id.lower().strip()


def find_first_key(obj: Any, target_key: str) -> Optional[Any]:
    """
    Recursively search for the first occurrence of target_key
    inside a nested dict/list structure.
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


def build_resource_service_lookup(apps_ List[Dict]) -> Dict[str, Dict[str, str]]:
    """
    Build lookup table:

      normalized resource_id -> {
         app_service_name,
         app_service_ci_number,
         Resource Type- Class,
         Process State
      }
    """
    lookup: Dict[str, Dict[str, str]] = {}

    for app in apps_
        app_process_state = find_first_key(app, "process_state") or ""
        services = app.get("app_services") or []

        for service in services:
            app_service_name = service.get("app_service_name", "")
            app_service_ci_number = service.get("app_service_ci_number", "")
            app_service_type = service.get("app_service_sys_class_name", "")

            service_process_state = find_first_key(service, "process_state")
            process_state = service_process_state or app_process_state or ""

            resources = service.get("resources") or []
            if isinstance(resources, dict):
                resources = list(resources.values())

            for resource in resources:
                resource_id = (
                    resource.get("resource_id")
                    or resource.get("path_end_resource_id")
                    or ""
                )
                if resource_id:
                    normalized_id = normalize_resource_id(resource_id)
                    lookup[normalized_id] = {
                        "app_service_name": app_service_name,
                        "app_service_ci_number": app_service_ci_number,
                        "Resource Type- Class": app_service_type,
                        "Process State": process_state,
                    }

    logger.log(f"Built resource lookup: {len(lookup)} resource IDs indexed", "LOOKUP")
    return lookup


# -----------------------------------------------------------------------------
# API FETCHING
# -----------------------------------------------------------------------------

def fetch_applications_api() -> List[Dict]:
    logger.log("Fetching API #1: Applications", "FETCH")
    try:
        response = requests.get(
            API_ENDPOINTS["applications"],
            params={"format": "json"},
            timeout=TIMEOUT,
            verify=SSL_VERIFY,
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
    logger.log(f"Fetching API #2: Mappings (app={app_code}, seg={segment})", "FETCH")
    try:
        response = requests.get(
            API_ENDPOINTS["mappings"],
            params={
                "app_code": app_code,
                "segment": segment,
                "month": month,
                "format": "json",
            },
            timeout=TIMEOUT,
            verify=SSL_VERIFY,
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
    logger.log(f"Fetching API #3: Apps (app={app_code})", "FETCH")
    try:
        response = requests.get(
            API_ENDPOINTS["apps"],
            params={
                "mfc_app_code": app_code,
                "format": "json",
                "include_resource": "true",
            },
            timeout=TIMEOUT,
            verify=SSL_VERIFY,
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


# -----------------------------------------------------------------------------
# DATA EXTRACTION
# -----------------------------------------------------------------------------

def extract_resources_from_mappings(
    mappings_ List[Dict],
    resource_lookup: Dict[str, Dict[str, str]],
) -> List[Dict]:
    logger.log(f"Extracting {len(mappings_data)} resources from Mappings API", "EXTRACT")

    resources: List[Dict[str, Any]] = []
    matched_count = 0
    unmatched_count = 0

    for mapping in mappings_
        ci_number = mapping.get("path_end_ci_number") or ""
        resource_id = mapping.get("path_end_resource_id") or ""
        full_path = resource_id
        meter_category = extract_meter_category(full_path)

        app_service_name = ""
        app_service_ci_number = ""
        app_service_resource_type = ""
        app_service_process_state = ""

        if resource_id:
            normalized_id = normalize_resource_id(resource_id)
            svc_info = resource_lookup.get(normalized_id)
            if svc_info:
                app_service_name = svc_info.get("app_service_name", "")
                app_service_ci_number = svc_info.get("app_service_ci_number", "")
                app_service_resource_type = svc_info.get("Resource Type- Class", "")
                app_service_process_state = svc_info.get("Process State", "")
                matched_count += 1
            else:
                unmatched_count += 1
        else:
            unmatched_count += 1

        resource = {
            "Resource Name": mapping.get("path_end_name") or "",
            "Resource Type": mapping.get("path_end_sys_class") or "",
            "CI Number": ci_number,
            "Business Application": mapping.get("app_ci_number") or "",
            "Meter Category": meter_category,
            "App Code": mapping.get("app_code") or "",
            "App Name": mapping.get("app_name") or "",
            "App Cost Center": mapping.get("app_cost_center") or "",
            "Segment": mapping.get("segment") or "",
            "Sub Segment": mapping.get("sub_segment") or "",
            "Resource ID": mapping.get("path_end_resource_id") or "",
            "app_service_name": app_service_name,
            "app_service_ci_number": app_service_ci_number,
            "Resource Type- Class": app_service_resource_type,
            "Process State": app_service_process_state,
        }
        resources.append(resource)

    logger.log(f"✓ Extracted {len(resources)} resources", "EXTRACT")
    logger.log(
        f"  Matched with service: {matched_count}, Unmatched: {unmatched_count}",
        "EXTRACT",
    )

    return resources


def extract_services_from_apps(apps_ List[Dict]) -> List[Dict]:
    logger.log(f"Extracting services from {len(apps_data)} app(s)", "EXTRACT")

    all_services: List[Dict[str, Any]] = []

    for app in apps_
        app_code = app.get("mfc_app_code") or ""
        app_ci_number = app.get("apm_app_id") or ""
        app_process_state = find_first_key(app, "process_state") or ""
        services = app.get("app_services") or []

        logger.log(f"  App {app_code}: Processing {len(services)} services", "EXTRACT")

        for service in services:
            service_process_state = find_first_key(service, "process_state")
            process_state = service_process_state or app_process_state or ""

            svc = {
                "Resource Name": service.get("app_service_name") or "",
                "Resource Type": service.get("app_service_sys_class_name") or "",
                "CI Number": service.get("app_service_ci_number") or "",
                "Application Code": app_code,
                "Environment": service.get("mfc_env") or "",
                "Parent CI Number": app_ci_number or "",
                "Process State": process_state,
            }
            all_services.append(svc)

    logger.log(f"✓ Extracted {len(all_services)} services", "EXTRACT")
    return all_services


# -----------------------------------------------------------------------------
# NEW RELIC ACCOUNT LOOKUP
# -----------------------------------------------------------------------------

class NewRelicLookup:
    """Handles New Relic API queries for account information."""

    def __init__(self) -> None:
        self.account_cache: Dict[str, str] = {}
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

        if item_name in self.account_cache:
            return self.account_cache[item_name]

        if not self.nr_api_key:
            return "ERROR"

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

        payload = {"query": query, "variables": ""}
        headers = {"Content-Type": "application/json", "API-Key": self.nr_api_key}

        try:
            response = requests.post(
                NR_API_URL,

