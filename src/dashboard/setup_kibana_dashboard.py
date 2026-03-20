
import json
import sys
import os
import argparse
import time

import requests


# Thêm project root vào sys.path
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..')))
from src.utils.logger_utils import get_logger


logger = get_logger("kibana_dashboard_setup", log_to_file=True)


# =========================================================
# Đường dẫn file JSON chứa định nghĩa Kibana objects
# =========================================================
OBJECTS_FILE = os.path.join(os.path.dirname(__file__), "kibana_objects.json")


# =========================================================
# Kibana API helpers
# =========================================================
class KibanaDashboardSetup:
    """
    Tạo Dashboard trên Kibana tự động qua Saved Objects API.

    Flow:
      1. Kiểm tra Kibana sẵn sàng
      2. Xóa saved objects cũ (nếu có)
      3. Tạo Data View (Index Pattern)
      4. Tạo từng Visualization
      5. Tạo Dashboard tổng hợp
    """

    def __init__(self, kibana_url: str = "http://localhost:5601"):
        self.kibana_url = kibana_url.rstrip("/")
        self.api_base = f"{self.kibana_url}/api"
        self.headers = {
            "kbn-xsrf": "true",
            "Content-Type": "application/json",
        }

        # Load object definitions
        with open(OBJECTS_FILE, "r", encoding="utf-8") as f:
            self.objects = json.load(f)

        logger.info(f"Kibana URL: {self.kibana_url}")

    # ---------------------------------------------------------
    # Kiểm tra Kibana sẵn sàng
    # ---------------------------------------------------------
    def wait_for_kibana(self, timeout: int = 120):
        """Đợi Kibana khởi động xong (tối đa timeout giây)."""
        logger.info(f"Đợi Kibana sẵn sàng (timeout={timeout}s)...")
        start = time.time()

        while time.time() - start < timeout:
            try:
                resp = requests.get(
                    f"{self.api_base}/status",
                    headers=self.headers,
                    timeout=5,
                )
                if resp.status_code == 200:
                    data = resp.json()
                    status = data.get("status", {}).get("overall", {}).get("level", "")
                    if status == "available":
                        logger.info("✅ Kibana sẵn sàng!")
                        return True
                    logger.info(f"Kibana status: {status}, đợi thêm...")
            except requests.ConnectionError:
                logger.debug("Kibana chưa sẵn sàng, thử lại...")
            except Exception as e:
                logger.debug(f"Lỗi kết nối Kibana: {e}")

            time.sleep(3)

        logger.error(f"Kibana không sẵn sàng sau {timeout}s!")
        return False

    # ---------------------------------------------------------
    # Xóa saved object cũ
    # ---------------------------------------------------------
    def delete_saved_object(self, obj_type: str, obj_id: str):
        """Xóa một saved object (bỏ qua nếu không tồn tại)."""
        try:
            resp = requests.delete(
                f"{self.api_base}/saved_objects/{obj_type}/{obj_id}",
                headers=self.headers,
                timeout=10,
            )
            if resp.status_code == 200:
                logger.info(f"  🗑️  Đã xóa {obj_type}/{obj_id}")
            elif resp.status_code == 404:
                logger.debug(f"  ℹ️  {obj_type}/{obj_id} không tồn tại, bỏ qua.")
            else:
                logger.warning(
                    f"  ⚠️  Lỗi xóa {obj_type}/{obj_id}: {resp.status_code}"
                )
        except Exception as e:
            logger.warning(f"  ⚠️  Lỗi xóa {obj_type}/{obj_id}: {e}")

    def delete_all_old_objects(self):
        """Xóa tất cả saved objects cũ (dashboard, visualizations)."""
        logger.info("🗑️  Xóa saved objects cũ...")

        # Xóa dashboard trước
        dash = self.objects["dashboard"]
        self.delete_saved_object("dashboard", dash["id"])

        # Xóa từng visualization
        for viz in self.objects["visualizations"]:
            self.delete_saved_object("visualization", viz["id"])

        logger.info("  ✅ Đã xóa xong saved objects cũ.")

    # ---------------------------------------------------------
    # Tạo Data View (Index Pattern)
    # ---------------------------------------------------------
    def create_data_view(self):
        """Tạo Data View (Index Pattern) cho traffic-realtime."""
        ip = self.objects["index_pattern"]
        logger.info(f"Tạo Data View: {ip['attributes']['title']}...")

        # Sử dụng Data Views API (Kibana 8.x)
        payload = {
            "data_view": {
                "id": ip["id"],
                "title": ip["attributes"]["title"],
                "timeFieldName": ip["attributes"]["timeFieldName"],
                "fieldFormats": json.loads(
                    ip["attributes"].get("fieldFormatMap", "{}")
                ),
            }
        }

        # Thử tạo mới
        resp = requests.post(
            f"{self.api_base}/data_views/data_view",
            headers=self.headers,
            json=payload,
            timeout=10,
        )

        if resp.status_code == 200:
            logger.info(f"  ✅ Data View '{ip['attributes']['title']}' đã tạo.")
            return True
        elif resp.status_code in (400, 409) and "Duplicate" in resp.text:
            logger.info(f"  ℹ️  Data View '{ip['attributes']['title']}' đã tồn tại.")
            return True
        else:
            logger.error(
                f"  ❌ Lỗi tạo Data View: {resp.status_code} - {resp.text}"
            )
            return False

    # ---------------------------------------------------------
    # Tạo Visualization
    # ---------------------------------------------------------
    def create_visualization(self, viz: dict):
        """Tạo một Visualization qua Saved Objects API."""
        viz_id = viz["id"]
        title = viz["attributes"]["title"]
        logger.info(f"  Tạo visualization: {title}...")

        # Chuẩn bị attributes - chuyển visState thành JSON string
        attributes = dict(viz["attributes"])
        if isinstance(attributes.get("visState"), dict):
            attributes["visState"] = json.dumps(attributes["visState"])

        # Chuyển searchSourceJSON thành string nếu là dict
        meta = dict(attributes.get("kibanaSavedObjectMeta", {}))
        search_source = meta.get("searchSourceJSON")
        if isinstance(search_source, dict):
            meta["searchSourceJSON"] = json.dumps(search_source)
        elif isinstance(search_source, str):
            # Đã là string → giữ nguyên, chỉ validate JSON hợp lệ
            try:
                json.loads(search_source)
            except json.JSONDecodeError:
                logger.warning(f"    searchSourceJSON không hợp lệ cho {title}")
        attributes["kibanaSavedObjectMeta"] = meta

        payload = {
            "attributes": attributes,
            "references": viz.get("references", []),
        }

        resp = requests.post(
            f"{self.api_base}/saved_objects/visualization/{viz_id}",
            headers=self.headers,
            json=payload,
            timeout=10,
        )

        if resp.status_code == 200:
            logger.info(f"    {title}")
            return True
        elif resp.status_code == 409:
            # Đã tồn tại → cập nhật
            resp = requests.put(
                f"{self.api_base}/saved_objects/visualization/{viz_id}",
                headers=self.headers,
                json=payload,
                timeout=10,
            )
            if resp.status_code == 200:
                logger.info(f"    {title} (đã cập nhật)")
                return True
            else:
                logger.error(f"    Lỗi cập nhật: {resp.status_code} - {resp.text}")
                return False
        else:
            logger.error(f"    Lỗi: {resp.status_code} - {resp.text}")
            return False

    # ---------------------------------------------------------
    # Tạo Dashboard
    # ---------------------------------------------------------
    def create_dashboard(self):
        """Tạo Dashboard tổng hợp."""
        dash = self.objects["dashboard"]
        dash_id = dash["id"]
        title = dash["attributes"]["title"]
        logger.info(f"Tạo Dashboard: {title}...")

        # Chuyển panelsJSON thành string nếu cần
        attributes = dict(dash["attributes"])
        if isinstance(attributes.get("panelsJSON"), list):
            attributes["panelsJSON"] = json.dumps(attributes["panelsJSON"])

        # Dashboard bắt buộc có searchSourceJSON hợp lệ để tránh lỗi render
        meta = dict(attributes.get("kibanaSavedObjectMeta", {}))
        search_source = meta.get("searchSourceJSON")
        if not search_source:
            meta["searchSourceJSON"] = json.dumps(
                {"query": {"language": "kuery", "query": ""}, "filter": []}
            )
        elif isinstance(search_source, dict):
            meta["searchSourceJSON"] = json.dumps(search_source)
        elif isinstance(search_source, str):
            try:
                json.loads(search_source)
            except json.JSONDecodeError:
                logger.warning(
                    "  Dashboard searchSourceJSON không hợp lệ, dùng mặc định."
                )
                meta["searchSourceJSON"] = json.dumps(
                    {"query": {"language": "kuery", "query": ""}, "filter": []}
                )
        attributes["kibanaSavedObjectMeta"] = meta

        payload = {
            "attributes": attributes,
            "references": dash.get("references", []),
        }

        resp = requests.post(
            f"{self.api_base}/saved_objects/dashboard/{dash_id}",
            headers=self.headers,
            json=payload,
            timeout=10,
        )

        if resp.status_code == 200:
            logger.info(f"  ✅ Dashboard '{title}' đã tạo!")
            return True
        elif resp.status_code == 409:
            resp = requests.put(
                f"{self.api_base}/saved_objects/dashboard/{dash_id}",
                headers=self.headers,
                json=payload,
                timeout=10,
            )
            if resp.status_code == 200:
                logger.info(f"  Dashboard '{title}' đã cập nhật!")
                return True
            else:
                logger.error(f"   Lỗi cập nhật Dashboard: {resp.status_code}")
                return False
        else:
            logger.error(f"  Lỗi tạo Dashboard: {resp.status_code} - {resp.text}")
            return False

    # ---------------------------------------------------------
    # Main setup
    # ---------------------------------------------------------
    def setup(self):
        """
        Chạy toàn bộ quy trình setup Dashboard.

        Returns:
            bool: True nếu setup thành công hoàn toàn.
        """
        logger.info("=" * 60)
        logger.info("🚦 BẮT ĐẦU SETUP KIBANA DASHBOARD")
        logger.info("=" * 60)

        # 1. Đợi Kibana sẵn sàng
        if not self.wait_for_kibana():
            return False

        # 2. Xóa saved objects cũ (tránh lỗi format cũ)
        self.delete_all_old_objects()

        # 3. Tạo Data View
        success = True
        if not self.create_data_view():
            success = False

        # 4. Tạo Visualizations
        viz_count = 0
        for viz in self.objects["visualizations"]:
            if self.create_visualization(viz):
                viz_count += 1
            else:
                success = False

        logger.info(
            f"Đã tạo {viz_count}/{len(self.objects['visualizations'])} visualizations."
        )

        # 5. Tạo Dashboard
        if not self.create_dashboard():
            success = False

        # 6. Tổng kết
        logger.info("=" * 60)
        if success:
            logger.info("✅ SETUP HOÀN TẤT!")
            logger.info(
                f"   Mở Dashboard tại: "
                f"{self.kibana_url}/app/dashboards#/view/smart-traffic-dashboard"
            )
        else:
            logger.warning("⚠️  SETUP HOÀN TẤT VỚI MỘT SỐ LỖI (xem log ở trên)")

        logger.info("=" * 60)
        return success


# =========================================================
# CLI Entrypoint
# =========================================================
def parse_args():
    parser = argparse.ArgumentParser(
        description="Setup Kibana Dashboard cho Smart Traffic Analytics"
    )
    parser.add_argument(
        "--kibana-url",
        default=os.getenv("KIBANA_URL", "http://localhost:5601"),
        help="URL của Kibana (default: http://localhost:5601)",
    )
    return parser.parse_args()


if __name__ == "__main__":
    args = parse_args()

    logger.info("🚦 Smart Traffic Analytics — Kibana Dashboard Setup")
    logger.info(f"   Kibana URL: {args.kibana_url}")

    setup = KibanaDashboardSetup(kibana_url=args.kibana_url)
    ok = setup.setup()
    sys.exit(0 if ok else 1)
