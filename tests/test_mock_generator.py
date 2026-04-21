"""
Test các hàm thuần túy của MockGenerator.
Không cần Docker, Kafka, hay MinIO — chạy được ngay bằng: pytest tests/
"""
import sys
import os
from unittest.mock import MagicMock

# Thêm root vào path để Python tìm được module ingest
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

# Mock MinioClient trước khi import MockGenerator (tránh lỗi kết nối)
sys.modules['minio_client'] = MagicMock()
# Cũng mock ingest.utils.minio_client vì facebook/mock.py import từ đó
sys.modules['ingest.utils.minio_client'] = MagicMock()

from ingest.facebook.mock import MockGenerator  # noqa: E402

# Tạo 1 instance dùng chung cho tất cả test
gen = MockGenerator(None, None, None)


# ── Nhóm 1: ID & tên ──────────────────────────────────────────────────────────

def test_deterministic_id_co_prefix():
    """ID phải bắt đầu bằng prefix truyền vào."""
    result = gen._get_deterministic_id("test_seed", prefix="cam")
    assert result.startswith("cam_")


def test_deterministic_id_on_dinh():
    """Cùng seed phải luôn ra cùng ID (deterministic)."""
    id1 = gen._get_deterministic_id("same_seed", "ad")
    id2 = gen._get_deterministic_id("same_seed", "ad")
    assert id1 == id2


def test_deterministic_id_khac_nhau_khi_khac_seed():
    result1 = gen._get_deterministic_id("seed_A", "ad")
    result2 = gen._get_deterministic_id("seed_B", "ad")
    assert result1 != result2


def test_random_name_tra_ve_string():
    name = gen._get_random_name("campaign", "some_seed")
    assert isinstance(name, str)
    assert len(name) > 0


def test_random_name_on_dinh():
    """Cùng category + seed → cùng tên."""
    name1 = gen._get_random_name("adset", "fixed_seed")
    name2 = gen._get_random_name("adset", "fixed_seed")
    assert name1 == name2


# ── Nhóm 2: Ngày tháng ────────────────────────────────────────────────────────

def test_get_dates_dung_so_ngay():
    dates = gen._get_dates_in_range("2026-01-01", "2026-01-07")
    assert len(dates) == 7


def test_get_dates_bao_gom_ngay_bat_dau_va_ket_thuc():
    dates = gen._get_dates_in_range("2026-03-01", "2026-03-03")
    assert dates[0] == "2026-03-01"
    assert dates[-1] == "2026-03-03"


def test_get_dates_mot_ngay():
    dates = gen._get_dates_in_range("2026-05-01", "2026-05-01")
    assert len(dates) == 1


# ── Nhóm 3: Seasonality (hệ số mùa vụ) ───────────────────────────────────────

def test_seasonality_cuoi_tuan_cao_hon_ngay_thuong():
    # 2026-01-03 là thứ 7 (weekend), 2026-01-05 là thứ 2 (weekday)
    weekend = gen._get_seasonality_multiplier("2026-01-03")
    weekday = gen._get_seasonality_multiplier("2026-01-05")
    assert weekend > weekday


def test_seasonality_ngay_thuong_bang_1():
    weekday = gen._get_seasonality_multiplier("2026-01-05")
    assert weekday == 1.0


def test_seasonality_tet_rat_cao():
    tet = gen._get_seasonality_multiplier("2026-01-20")  # tháng 1, gần Tết
    assert tet > 1.0


# ── Nhóm 4: Targeting bias ────────────────────────────────────────────────────

def test_bias_phu_nu():
    bias = gen._analyze_targeting_bias("Phụ nữ - Quan tâm sức khỏe")
    assert bias.get("gender") == "female"


def test_bias_nam_gioi():
    bias = gen._analyze_targeting_bias("Nam giới - Phụ kiện xe hơi")
    assert bias.get("gender") == "male"


def test_bias_khong_co_khi_trung_tinh():
    bias = gen._analyze_targeting_bias("Broad Targeting - Toàn quốc")
    assert "gender" not in bias


def test_bias_ceo_age_range():
    bias = gen._analyze_targeting_bias("Tệp đối tượng Quản lý / CEO")
    assert "age_range" in bias


# ── Nhóm 5: Sum metrics ───────────────────────────────────────────────────────

def test_sum_metrics_cong_dung():
    target = {"spend": 100.0, "impressions": 1000}
    source = {"spend": 50.0, "impressions": 500}
    gen._sum_metrics(target, source)
    assert target["spend"] == 150.0
    assert target["impressions"] == 1500


def test_sum_metrics_key_khong_co_trong_target():
    target = {}
    source = {"clicks": 200}
    gen._sum_metrics(target, source)
    assert target["clicks"] == 200
