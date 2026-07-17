"""Unit tests for SNMP throughput helpers."""

from collector import MAX_COUNTER64, calc_mbps, delta_with_rollover


def test_delta_no_rollover():
    assert delta_with_rollover(1000, 400) == 600


def test_delta_equal():
    assert delta_with_rollover(50, 50) == 0


def test_delta_rollover():
    prev = MAX_COUNTER64 - 10
    cur = 5
    # (2^64 - prev) + cur = 10 + 5
    assert delta_with_rollover(cur, prev) == 15


def test_calc_mbps():
    # 125_000_000 bytes in 1 second = 1 Gbps = 1000 Mbps
    assert abs(calc_mbps(125_000_000, 1.0) - 1000.0) < 1e-9


def test_calc_mbps_zero_delta():
    assert calc_mbps(1000, 0) == 0.0
    assert calc_mbps(1000, -1) == 0.0
