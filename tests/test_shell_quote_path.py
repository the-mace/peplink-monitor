"""Remote path quoting must preserve tilde expansion under SSH."""

from cli import shell_quote_path


def test_tilde_home_converted():
    # shlex.quote only adds quotes when needed; plain path chars stay bare.
    out = shell_quote_path("~/Documents/Code/peplink-monitor")
    assert out.startswith("$HOME/")
    assert "peplink-monitor" in out
    assert not out.startswith("'~/")  # must not quote the tilde away


def test_tilde_only():
    assert shell_quote_path("~") == "$HOME"


def test_absolute_quoted():
    assert shell_quote_path("/Users/rob/Documents/Code/peplink-monitor") == (
        "/Users/rob/Documents/Code/peplink-monitor"
    ) or shell_quote_path("/Users/rob/Documents/Code/peplink-monitor").startswith("'")


def test_relative_quoted():
    assert shell_quote_path("data/monitor.db") == "data/monitor.db" or (
        shell_quote_path("data/monitor.db") == "'data/monitor.db'"
    )
