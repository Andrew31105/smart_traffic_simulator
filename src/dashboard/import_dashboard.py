"""
import_dashboard.py - Import Kibana Dashboard qua Import API (_import).

Sử dụng Kibana Saved Objects _import API (NDJSON format)
để import visualizations và dashboard đúng cách.
"""

import json
import sys
import os
import tempfile

import requests

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..')))
from src.utils.logger_utils import get_logger

logger = get_logger("kibana_import", log_to_file=True)

KIBANA_URL = os.getenv("KIBANA_URL", "http://localhost:5601")
OBJECTS_FILE = os.path.join(os.path.dirname(__file__), "kibana_objects.json")


def build_ndjson():
    """Build NDJSON content from kibana_objects.json for the Import API."""
    with open(OBJECTS_FILE, "r", encoding="utf-8") as f:
        objects = json.load(f)

    lines = []

    # 1. Index Pattern (as index-pattern type for import)
    ip = objects["index_pattern"]
    ip_obj = {
        "id": ip["id"],
        "type": "index-pattern",
        "attributes": {
            "title": ip["attributes"]["title"],
            "timeFieldName": ip["attributes"]["timeFieldName"],
            "fields": ip["attributes"].get("fields", "[]"),
            "fieldFormatMap": ip["attributes"].get("fieldFormatMap", "{}"),
        },
        "references": [],
    }
    lines.append(json.dumps(ip_obj, ensure_ascii=False))

    # 2. Visualizations
    for viz in objects["visualizations"]:
        attributes = dict(viz["attributes"])

        # visState phải là JSON string
        if isinstance(attributes.get("visState"), dict):
            attributes["visState"] = json.dumps(attributes["visState"])

        # searchSourceJSON phải là JSON string
        meta = attributes.get("kibanaSavedObjectMeta", {})
        if isinstance(meta, dict):
            ss = meta.get("searchSourceJSON")
            if isinstance(ss, dict):
                meta["searchSourceJSON"] = json.dumps(ss)
            attributes["kibanaSavedObjectMeta"] = meta

        viz_obj = {
            "id": viz["id"],
            "type": "visualization",
            "attributes": attributes,
            "references": viz.get("references", []),
        }
        lines.append(json.dumps(viz_obj, ensure_ascii=False))

    # 3. Dashboard
    dash = objects["dashboard"]
    dash_attrs = dict(dash["attributes"])

    # panelsJSON phải là JSON string
    if isinstance(dash_attrs.get("panelsJSON"), list):
        dash_attrs["panelsJSON"] = json.dumps(dash_attrs["panelsJSON"])

    # Dashboard cần kibanaSavedObjectMeta.searchSourceJSON hợp lệ
    dash_meta = dash_attrs.get("kibanaSavedObjectMeta", {})
    if not isinstance(dash_meta, dict):
        dash_meta = {}
    dash_ss = dash_meta.get("searchSourceJSON")
    if not dash_ss:
        dash_meta["searchSourceJSON"] = json.dumps(
            {"query": {"language": "kuery", "query": ""}, "filter": []}
        )
    elif isinstance(dash_ss, dict):
        dash_meta["searchSourceJSON"] = json.dumps(dash_ss)
    dash_attrs["kibanaSavedObjectMeta"] = dash_meta

    # refreshInterval bỏ ra khỏi attributes (Kibana tự xử lý qua timeRestore)
    dash_attrs.pop("refreshInterval", None)

    dash_obj = {
        "id": dash["id"],
        "type": "dashboard",
        "attributes": dash_attrs,
        "references": dash.get("references", []),
    }
    lines.append(json.dumps(dash_obj, ensure_ascii=False))

    return "\n".join(lines)


def import_to_kibana(ndjson_content: str):
    """Import NDJSON content to Kibana via Import API."""
    url = f"{KIBANA_URL}/api/saved_objects/_import?overwrite=true"

    # Import API cần multipart/form-data, file có extension .ndjson
    with tempfile.NamedTemporaryFile(
        mode="w", suffix=".ndjson", delete=False, encoding="utf-8"
    ) as tmp:
        tmp.write(ndjson_content)
        tmp_path = tmp.name

    try:
        with open(tmp_path, "rb") as f:
            resp = requests.post(
                url,
                headers={"kbn-xsrf": "true"},
                files={"file": ("export.ndjson", f, "application/ndjson")},
                timeout=30,
            )

        result = resp.json()
        logger.info(f"Import status: {resp.status_code}")
        logger.info(json.dumps(result, indent=2, ensure_ascii=False))

        if resp.status_code == 200 and result.get("success"):
            logger.info("✅ Import thành công!")
            count = result.get("successCount", 0)
            logger.info(f"   Đã import {count} objects.")
            return True
        else:
            logger.error(f"❌ Import thất bại!")
            errors = result.get("errors", [])
            for err in errors:
                logger.error(f"   - {err.get('id')}: {err.get('error', {}).get('message', 'unknown')}")
            return False
    finally:
        os.unlink(tmp_path)


def main():
    logger.info("🚦 Smart Traffic Analytics — Kibana Dashboard Import")
    logger.info(f"   Kibana URL: {KIBANA_URL}")

    # Build NDJSON
    logger.info("📦 Building NDJSON...")
    ndjson = build_ndjson()
    logger.info(f"   {ndjson.count(chr(10)) + 1} objects to import")

    # Import
    logger.info("📤 Importing to Kibana...")
    ok = import_to_kibana(ndjson)

    if ok:
        logger.info(f"🔗 Dashboard: {KIBANA_URL}/app/dashboards#/view/smart-traffic-dashboard")

    sys.exit(0 if ok else 1)


if __name__ == "__main__":
    main()
