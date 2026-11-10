import json
from pathlib import Path

from mcp_server.logs import LogLine, render, select_useful

FIX = Path(__file__).parent / "fixtures"


def _content():
    return json.loads((FIX / "airflow_log_http503.json").read_text())["content"]


def test_render_real_structured_log():
    src = ["x"] * 82 + ["    resp.raise_for_status()"] + ["x"] * 10
    lines = render(_content(), src)
    texts = [ln.text for ln in lines]
    assert [ln.n for ln in lines] == list(range(1, len(lines) + 1))
    assert not any("Log message source details" in t or t.endswith(".log") for t in texts)
    assert "Traceback (most recent call last):" in texts
    assert any(t.startswith("HTTPError: 503 Server Error") for t in texts)
    i = next(i for i, t in enumerate(texts) if "cobaye_ventes.py\", line 83" in t)
    assert texts[i + 1].strip() == "resp.raise_for_status()"   # ligne de code du DAG ajoutée
    assert any("frames internes d'Airflow masquées" in t for t in texts)
    assert any("[cobaye_ventes] (cobaye_ventes.py:82)" in t for t in texts)


def test_select_useful_keeps_original_numbers_and_reports_gaps():
    lines = [LogLine(i, f"INFO ligne {i}", "info") for i in range(1, 301)]
    lines[199] = LogLine(200, "ERROR boom", "error")
    kept, omitted = select_useful(lines, before=5, after=2, tail=3)
    ns = [ln.n for ln in kept]
    assert ns == list(range(195, 203)) + [298, 299, 300]
    assert omitted == [[1, 194], [203, 297]]


def test_select_useful_without_error_keeps_the_end():
    lines = [LogLine(i, f"INFO {i}", "info") for i in range(1, 501)]
    kept, _ = select_useful(lines, max_lines=50)
    assert kept[0].n == 451 and kept[-1].n == 500
