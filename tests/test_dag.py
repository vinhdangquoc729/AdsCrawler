"""
Kiểm tra Airflow DAG không có lỗi import.
Yêu cầu: pip install apache-airflow
"""
import pytest

airflow = pytest.importorskip(
    "airflow",
    reason="Cần cài apache-airflow: pip install apache-airflow"
)


def test_dag_khong_co_loi_import():
    """DAG phải load được, không có import error."""
    from airflow.models import DagBag
    db = DagBag(
        dag_folder="airflow/dags",
        include_examples=False,
    )
    assert db.import_errors == {}, f"DAG bị lỗi import: {db.import_errors}"


def test_dag_marketing_pipeline_ton_tai():
    """DAG marketing_data_pipeline phải tồn tại."""
    from airflow.models import DagBag
    db = DagBag(
        dag_folder="airflow/dags",
        include_examples=False,
    )
    assert "marketing_data_pipeline" in db.dags
