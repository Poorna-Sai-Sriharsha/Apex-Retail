import os

# 1. 1-to-1 mappings
with open('tests/test_coverage_bonus_tracker.py', 'r', encoding='utf-8') as f:
    tracker_code = f.read()
    
with open('tests/test_pipeline.py', 'a', encoding='utf-8') as f:
    f.write("\n\n# --- Merged from test_coverage_bonus_tracker.py ---\n")
    f.write(tracker_code)

with open('tests/test_coverage_bonus_main.py', 'r', encoding='utf-8') as f:
    main_code = f.read()

with open('tests/test_edge_cases.py', 'a', encoding='utf-8') as f:
    f.write("\n\n# --- Merged from test_coverage_bonus_main.py ---\n")
    f.write(main_code)

with open('tests/test_coverage_push.py', 'r', encoding='utf-8') as f:
    push_code = f.read()

with open('tests/test_edge_cases.py', 'a', encoding='utf-8') as f:
    f.write("\n\n# --- Merged from test_coverage_push.py ---\n")
    f.write(push_code)

# 2. test_coverage_boost.py split
with open('tests/test_coverage_boost.py', 'r', encoding='utf-8') as f:
    lines = f.readlines()

def get_lines(start, end):
    return "".join(lines[start-1:end])

# Setup metrics + metrics + funnel + camera metrics
metrics_code = get_lines(1, 148) + get_lines(247, 290) + get_lines(472, 481)
with open('tests/test_metrics.py', 'a', encoding='utf-8') as f:
    f.write("\n\n# --- Merged from test_coverage_boost.py ---\n")
    f.write(metrics_code)

# Anomaly tests (needs setup_anomaly_data)
anom_code = "import pytest\nfrom httpx import AsyncClient\nfrom datetime import datetime, timedelta, timezone\nfrom sqlalchemy.ext.asyncio import AsyncSession\nfrom app.models import EventORM\nfrom tests.conftest import make_session_events, make_event\n\n"
anom_code += get_lines(151, 245)
with open('tests/test_anomalies.py', 'a', encoding='utf-8') as f:
    f.write("\n\n# --- Merged from test_coverage_boost.py ---\n")
    f.write(anom_code)

# Edge cases / Health / Coverage
edge_code = get_lines(291, 388) + get_lines(461, 471)
with open('tests/test_edge_cases.py', 'a', encoding='utf-8') as f:
    f.write("\n\n# --- Merged from test_coverage_boost.py ---\n")
    f.write(edge_code)

# Pipeline (classifier/tracker)
pipeline_code = get_lines(389, 460)
with open('tests/test_pipeline.py', 'a', encoding='utf-8') as f:
    f.write("\n\n# --- Merged from test_coverage_boost.py ---\n")
    f.write(pipeline_code)

print("Merge completed successfully.")
