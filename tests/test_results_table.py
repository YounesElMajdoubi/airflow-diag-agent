"""Format du tableau du README, avec un faux client MLflow (aucun chiffre réel ici)."""

from types import SimpleNamespace as NS

from eval.make_results_table import build_table, wilson


class StubClient:
    def __init__(self, runs, checks=()):
        self.runs, self.checks = runs, list(checks)

    def get_experiment_by_name(self, name):
        return NS(experiment_id="1")

    def search_runs(self, ids, filter_string, order_by, max_results):
        return self.checks if "verification_manuelle" in filter_string else self.runs


def _run(rid, cfg, strat, **metrics):
    return NS(info=NS(run_id=rid), data=NS(tags={"configuration": cfg, "strategie": strat}, metrics=metrics))


def test_wilson_known_value():
    lo, hi = wilson(0.8, 30)
    assert round(lo, 3) == 0.627 and round(hi, 3) == 0.905
    assert wilson(None, 30) is None


def test_table_keeps_latest_run_per_configuration_and_orders_strategies():
    runs = [  # déjà triés du plus récent au plus ancien, comme le renvoie MLflow
        _run("bbbbbbbb1", "routage-x", "routage", exactitude=0.9, n=30),
        _run("aaaaaaaa2", "gros-x", "gros", exactitude=0.7, n=30, cout_par_diagnostic_usd=0.0123, fuites_secrets=0),
        _run("aaaaaaaa1", "gros-x", "gros", exactitude=0.1, n=30),  # plus ancien : ignoré
    ]
    table = build_table(StubClient(runs))
    lines = [ln for ln in table.splitlines() if ln.startswith("| gros") or ln.startswith("| routage")]
    assert len(lines) == 2 and lines[0].startswith("| gros") and lines[1].startswith("| routage")
    assert "70 % [52–83]" in lines[0] and "0.01230 $" in lines[0] and "`aaaaaaaa`" in lines[0]
    assert "pas encore faite" in table


def test_empty_experiment_says_so():
    assert "Aucune campagne réelle" in build_table(StubClient([]))
