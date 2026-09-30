import importlib.util
from pathlib import Path
import pandas as pd
from datetime import datetime, timezone

P=Path(__file__).resolve().parents[1]/"scripts"/"build_dashboard.py"
spec=importlib.util.spec_from_file_location("dash",P)
dash=importlib.util.module_from_spec(spec); spec.loader.exec_module(dash)

def make(prices, year=2026):
    dates=pd.date_range(f"{year}-01-02", periods=len(prices), freq="B", tz="UTC")
    return {"df":pd.DataFrame({"date":dates,"close":prices})}

def test_one_day_uses_last_two_bars_not_chart_range_start():
    m=dash.history_metrics(make([100,101,102,103,104,105,106,107]),"MU")
    assert round(m["1d"],6)==round((107/106-1)*100,6)

def test_ytd_uses_last_prior_year_close_when_available():
    dates=pd.to_datetime(["2025-12-30","2025-12-31","2026-01-02","2026-01-05"],utc=True)
    h={"df":pd.DataFrame({"date":dates,"close":[98,100,101,110]})}
    m=dash.history_metrics(h,"SPY")
    assert round(m["ytd"],6)==10.0

def test_index_sanity_guard_blocks_impossible_daily_print():
    m=dash.history_metrics(make([100,101,102,103,104,105,106,140]),"^GSPC")
    assert m["1d"] is None
    assert m["quality_issue"]

def test_large_single_stock_move_can_still_pass():
    m=dash.history_metrics(make([100,101,102,103,104,105,106,80]),"FICO")
    assert m["1d"] is not None
