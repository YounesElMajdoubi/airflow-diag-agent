"""Génère le tableau de résultats du README À PARTIR DE MLFLOW (jamais à la main).

Règles :
- seules les campagnes complètes (jeu = all), terminées, avec de vrais modèles (fake = false) ;
- pour chaque configuration, la campagne la plus récente ;
- chaque ligne renvoie à son run MLflow (traçabilité) ;
- s'il n'y a aucune campagne réelle, le README le dit au lieu d'afficher des chiffres.

Usage : python -m eval.make_results_table   (réécrit la section entre les marqueurs du README)
"""

from __future__ import annotations

import os
import re
from datetime import datetime, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
README = ROOT / "README.md"
START, END = "<!-- RESULTATS:DEBUT -->", "<!-- RESULTATS:FIN -->"
ORDER = {"gros": 0, "petit": 1, "routage": 2}


def _f(v, pct=False, money=False, digits=2):
    if v is None:
        return "—"
    if pct:
        return f"{100 * v:.0f} %"
    if money:
        return f"{v:.5f} $"
    return f"{v:.{digits}f}"


def wilson(p: float | None, n: int, z: float = 1.96) -> tuple[float, float] | None:
    """IC à 95 % de Wilson pour une proportion (meilleur que p ± 1,96·√(p(1-p)/n) quand n est petit)."""
    if p is None or n == 0:
        return None
    den = 1 + z * z / n
    centre = (p + z * z / (2 * n)) / den
    half = z * ((p * (1 - p) / n + z * z / (4 * n * n)) ** 0.5) / den
    return max(0.0, centre - half), min(1.0, centre + half)


def build_table(client, experiment_name: str = "diagnostic-airflow") -> str:
    exp = client.get_experiment_by_name(experiment_name)
    if exp is None:
        return "_Aucune campagne enregistrée pour l'instant : lancer `make eval` (voir plus bas)._"
    runs = client.search_runs(
        [exp.experiment_id],
        filter_string="tags.type = 'campagne' and tags.fake = 'false' and tags.jeu = 'all' and attributes.status = 'FINISHED'",
        order_by=["attributes.start_time DESC"], max_results=500,
    )
    latest = {}
    for r in runs:
        latest.setdefault(r.data.tags["configuration"], r)
    if not latest:
        return "_Aucune campagne réelle complète pour l'instant : lancer `make eval` (voir plus bas)._"
    rows = sorted(latest.values(), key=lambda r: (ORDER.get(r.data.tags["strategie"], 9), r.data.tags["configuration"]))
    out = ["| Stratégie | Configuration | Exactitude | Justesse diag. (juge) | Escalades correctes | Coût / diagnostic | Durée moy. | Fuites | n | Run MLflow |",
           "|---|---|---|---|---|---|---|---|---|---|"]
    for r in rows:
        m, t = r.data.metrics, r.data.tags
        ci = wilson(m.get("exactitude"), int(m.get("n", 0)))
        acc = _f(m.get("exactitude"), pct=True) + (f" [{100 * ci[0]:.0f}–{100 * ci[1]:.0f}]" if ci else "")
        out.append(f"| {t['strategie']} | {t['configuration']} | {acc} | "
                   f"{_f(m.get('justesse_diagnostic'), pct=True)} | {_f(m.get('escalades_correctes'), pct=True)} | "
                   f"{_f(m.get('cout_par_diagnostic_usd'), money=True)} | {_f(m.get('duree_moy_s'), digits=1)} s | "
                   f"{int(m.get('fuites_secrets', 0))} | {int(m.get('n', 0))} | `{r.info.run_id[:8]}` |")
    checks = client.search_runs([exp.experiment_id], filter_string="tags.type = 'verification_manuelle' and tags.fake = 'false'",
                                order_by=["attributes.start_time DESC"], max_results=1)
    if checks:
        m = checks[0].data.metrics
        out += ["", f"Vérification manuelle du juge ({int(m.get('n_diagnostic', 0))} diagnostics notés à l'aveugle) : "
                    f"accord exact {_f(m.get('accord_exact_diagnostic'), pct=True)}, "
                    f"kappa pondéré {_f(m.get('kappa_pondere_diagnostic'))} (run `{checks[0].info.run_id[:8]}`)."]
    else:
        out += ["", "_Vérification manuelle du juge : pas encore faite (`make verif-juge`)._"]
    out += ["", "Exactitude : entre crochets, intervalle de confiance à 95 % (Wilson). Avec 30 pannes, "
                "deux configurations dont les intervalles se chevauchent largement ne sont pas départagées."]
    out += ["", f"_Tableau généré depuis MLflow le {datetime.now(timezone.utc):%Y-%m-%d %H:%M} UTC "
                "par `eval/make_results_table.py` ; ne pas modifier à la main._"]
    return "\n".join(out)


def update_readme(table: str) -> None:
    text = README.read_text(encoding="utf-8")
    if START not in text or END not in text:
        raise SystemExit(f"Marqueurs {START} / {END} absents du README")
    new = re.sub(re.escape(START) + ".*?" + re.escape(END), f"{START}\n{table}\n{END}", text, flags=re.S)
    README.write_text(new, encoding="utf-8")


def main() -> None:
    import mlflow
    from mlflow.tracking import MlflowClient

    mlflow.set_tracking_uri(os.environ.get("MLFLOW_TRACKING_URI", "http://localhost:5000"))
    table = build_table(MlflowClient())
    update_readme(table)
    print(table)


if __name__ == "__main__":
    main()
