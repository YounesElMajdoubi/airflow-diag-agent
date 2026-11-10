import pytest

from agent.schemas import Citation, Diagnostic
from agent.validation import DiffError, apply_unified_diff, check_citations, check_diff

SRC = "def f(o, rates):\n    rate = rates.get(o['currency'])\n    return round(o['amount'] * taux, 2)\n"


def _diag(*cits):
    return Diagnostic(cause="cause suffisamment longue", explication="explication assez longue",
                      citations=[Citation(ligne=n, extrait=e) for n, e in cits])


def test_citations():
    lines = {12: "NameError: name 'taux' is not defined", 13: "===== Post Execute ====="}
    assert check_citations(_diag((12, "name 'taux' is not  defined")), lines) == []
    assert "ne fait pas partie" in check_citations(_diag((99, "taux")), lines)[0]
    assert "ne figure pas" in check_citations(_diag((13, "NameError")), lines)[0]


def test_diff_applies_even_with_wrong_line_numbers():
    diff = """--- a/cobaye_ventes.py
+++ b/cobaye_ventes.py
@@ -40,2 +40,2 @@
     rate = rates.get(o['currency'])
-    return round(o['amount'] * taux, 2)
+    return round(o['amount'] * rate, 2)
"""
    assert "* rate, 2)" in apply_unified_diff(SRC, diff)
    assert check_diff(diff, SRC) == (True, [])


def test_diff_with_wrong_context_is_rejected():
    bad = "@@ -1,1 +1,1 @@\n-    return round(o['montant'] * taux, 2)\n+    return 0\n"
    with pytest.raises(DiffError):
        apply_unified_diff(SRC, bad)
    ok, problems = check_diff(bad, SRC)
    assert ok is False and problems


def test_no_diff():
    assert check_diff(None, SRC) == (None, [])
