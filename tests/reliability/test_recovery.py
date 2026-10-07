from __future__ import annotations

from reliability.recovery import canonical_hash, metric_value


def test_canonical_hash_is_order_independent_for_mapping_keys() -> None:
    assert canonical_hash({"a": 1, "b": [2, 3]}) == canonical_hash(
        {"b": [2, 3], "a": 1}
    )


def test_canonical_hash_changes_when_business_identity_changes() -> None:
    assert canonical_hash({"events": ["a"]}) != canonical_hash({"events": ["a", "b"]})


def test_metric_value_reads_prometheus_gauge() -> None:
    text = """# TYPE citibike_serving_database_up gauge
citibike_serving_database_up 0
"""
    assert metric_value(text, "citibike_serving_database_up") == 0
    assert metric_value(text, "missing") is None
