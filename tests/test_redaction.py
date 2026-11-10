import json
from pathlib import Path

from mcp_server.logs import render
from mcp_server.redaction import MASK, Redactor

FIX = Path(__file__).parent / "fixtures"
CANARIES = ["fk_live_9c2e5a7f1b3d4e6a8c0b2d4f6a8e0c1d", "cnry-pg-3f9a1c7e2b", "cnry-src-5b1e0c9d7a"]


def test_real_airflow_log_has_unmasked_key_and_we_mask_it():
    content = json.loads((FIX / "airflow_log_http503.json").read_text())["content"]
    raw = "\n".join(ln.text for ln in render(content))
    assert CANARIES[0] in raw  # Airflow lui-même ne masque pas cette clé : le test a du sens
    red = Redactor()
    masked = red.redact(raw)
    assert CANARIES[0] not in masked
    assert red.total >= 1


def test_forms_of_secrets():
    r = Redactor()
    cases = {
        "postgresql://etl_writer:cnry-pg-3f9a1c7e2b@entrepot:5432/dwh": "postgresql://etl_writer:" + MASK + "@entrepot:5432/dwh",
        "url?date=2026-03-02&api_key=fk_live_9c2e5a7f1b3d4e6a8c0b2d4f6a8e0c1d&x=1": f"url?date=2026-03-02&api_key={MASK}&x=1",
        "Authorization: Bearer abcdefghijkl123": f"Authorization: Bearer {MASK}",
        '"password": "motdepasse"': f'"password": "{MASK}"',
        "Clé API refusée : fk_live_9c2e5a7f1b3d4e6a8c0b2d4f6a8e0c1d": f"Clé API refusée : {MASK}",
    }
    for raw, expected in cases.items():
        assert r.redact(raw) == expected, raw


def test_no_false_positives_on_airflow_noise():
    r = Redactor()
    for text in [
        'File "/opt/afenv/lib/python3.13/site-packages/airflow/sdk/execution_time/task_runner.py", line 1579',
        "ti_id=01a1c6d0-75ca-7554-972a-17731037c9f6",
        "run manual__2026-03-02T06:00:00+00:00_ab12",
        "KeyError: 'amount'",
        "ValueError: invalid literal for int() with base 10: '12.50'",
        'password authentication failed for user "etl_writer"',
        "invalid_api_key",
    ]:
        assert r.redact(text) == text, text
    assert r.total == 0


def test_source_code_mode_keeps_code_references():
    src = 'api_key = os.environ["FOURNISSEUR_API_KEY"]\nparams={"date": ds, "api_key": api_key}\nDSN = "postgresql://u:cnry-src-5b1e0c9d7a@h/db"'
    out = Redactor(source_code=True).redact(src)
    assert 'api_key = os.environ["FOURNISSEUR_API_KEY"]' in out
    assert '"api_key": api_key' in out
    assert "cnry-src-5b1e0c9d7a" not in out
