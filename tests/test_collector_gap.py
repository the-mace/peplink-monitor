"""Collector gap handling: host downtime must not invent poll transitions."""

from collector import poll_state_is_stale


def test_fresh_sample_not_stale():
    # 5-minute cadence; sample 5 minutes ago is still trusted.
    assert not poll_state_is_stale(1000, 1000 + 300, 300)


def test_just_beyond_two_intervals_is_stale():
    # Default max_intervals=2 → gap > 600s is stale.
    assert poll_state_is_stale(1000, 1000 + 601, 300)
    assert not poll_state_is_stale(1000, 1000 + 600, 300)


def test_none_last_seen_not_stale():
    # First-ever observation has no prior sample to compare.
    assert not poll_state_is_stale(None, 5000, 300)


def test_host_reboot_gap_is_stale():
    # Multi-hour host reboot must not compare pre-gap LED to live LED.
    assert poll_state_is_stale(1_000_000, 1_000_000 + 7200, 300)
