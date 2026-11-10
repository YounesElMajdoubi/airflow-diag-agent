"""Vérification à la main des notes du LLM juge, sur un échantillon.

1. `sample` tire un échantillon aléatoire (graine fixe) de diagnostics depuis des runs MLflow
   et écrit une feuille à remplir SANS les notes du juge (notation à l'aveugle) ;
   les notes du juge sont mises de côté dans un fichier clé.
2. Tu remplis les colonnes note_humaine_* (0, 1 ou 2) en suivant eval/grille_juge.md.
3. `accord` compare tes notes à celles du juge : accord exact et kappa de Cohen
   pondéré (quadratique, adapté à une échelle ordinale 0-1-2), puis l'enregistre dans MLflow.

Usage :
    python -m eval.manual_check sample --runs <run_id> [<run_id> ...] --n 15
    python -m eval.manual_check accord eval/verification/feuille.csv
"""

from __future__ import annotations

import argparse
import csv
import json
import os
import random
from pathlib import Path

from faults.catalog import load_scenarios

OUT_DIR = Path(__file__).resolve().parent / "verification"
FIELDS = ["id", "scenario", "cause_reelle", "diagnostic_agent", "correction_agent",
          "note_humaine_diagnostic", "note_humaine_correction", "commentaire"]


def weighted_kappa(a: list[int], b: list[int], k: int = 3) -> float | None:
    """Kappa de Cohen à poids quadratiques pour deux notateurs sur une échelle 0..k-1."""
    n = len(a)
    if n == 0:
        return None
    obs = [[0.0] * k for _ in range(k)]
    for x, y in zip(a, b):
        obs[x][y] += 1 / n
    pa = [sum(obs[i]) for i in range(k)]
    pb = [sum(obs[i][j] for i in range(k)) for j in range(k)]
    w = [[(i - j) ** 2 / (k - 1) ** 2 for j in range(k)] for i in range(k)]
    num = sum(w[i][j] * obs[i][j] for i in range(k) for j in range(k))
    den = sum(w[i][j] * pa[i] * pb[j] for i in range(k) for j in range(k))
    return round(1 - num / den, 4) if den else None


def _mlflow():
    import mlflow
    mlflow.set_tracking_uri(os.environ.get("MLFLOW_TRACKING_URI", "http://localhost:5000"))
    return mlflow


def sample(run_ids: list[str], n: int, seed: int) -> None:
    mlflow = _mlflow()
    causes = {s.id: s.cause_reelle for s in load_scenarios()}
    pool = []
    client = mlflow.tracking.MlflowClient()
    fake_runs = {rid for rid in run_ids if client.get_run(rid).data.tags.get("fake") != "false"}
    for rid in run_ids:
        path = mlflow.artifacts.download_artifacts(run_id=rid, artifact_path="resultats.jsonl")
        reports_dir = mlflow.artifacts.download_artifacts(run_id=rid, artifact_path="rapports")
        for line in Path(path).read_text(encoding="utf-8").splitlines():
            r = json.loads(line)
            rp = Path(reports_dir) / f"{r['scenario']}.json"
            if r.get("note_diagnostic") is None or not rp.exists():
                continue
            rap = json.loads(rp.read_text())
            pool.append({"run": rid, "r": r, "rapport": rap})
    rng = random.Random(seed)
    chosen = rng.sample(pool, min(n, len(pool)))
    OUT_DIR.mkdir(exist_ok=True)
    sheet, key = OUT_DIR / "feuille.csv", OUT_DIR / "cle_juge.json"
    with sheet.open("w", newline="", encoding="utf-8") as fh:
        w = csv.DictWriter(fh, FIELDS)
        w.writeheader()
        keys = {}
        for i, c in enumerate(chosen, 1):
            d, f = c["rapport"].get("diagnostic") or {}, c["rapport"].get("correction") or {}
            w.writerow({"id": i, "scenario": c["r"]["scenario"], "cause_reelle": causes[c["r"]["scenario"]],
                        "diagnostic_agent": f"{d.get('cause', '')} | {d.get('explication', '')}",
                        "correction_agent": f"{f.get('resume', '')} | " + " / ".join(f.get("etapes", [])),
                        "note_humaine_diagnostic": "", "note_humaine_correction": "", "commentaire": ""})
            keys[str(i)] = {"run": c["run"], "note_diagnostic": c["r"]["note_diagnostic"],
                            "note_correction": c["r"]["note_correction"], "fake": c["run"] in fake_runs}
    key.write_text(json.dumps(keys, indent=1))
    print(f"{len(chosen)} diagnostics à noter dans {sheet} (notes du juge mises de côté dans {key.name})")


def agreement(sheet: Path) -> None:
    keys = json.loads((sheet.parent / "cle_juge.json").read_text())
    pairs = {"diagnostic": ([], []), "correction": ([], [])}
    with sheet.open(encoding="utf-8") as fh:
        for row in csv.DictReader(fh):
            for crit in pairs:
                h = row[f"note_humaine_{crit}"].strip()
                if h:
                    pairs[crit][0].append(int(h))
                    pairs[crit][1].append(int(keys[row["id"]][f"note_{crit}"]))
    res = {}
    for crit, (hum, jug) in pairs.items():
        if not hum:
            continue
        res[f"accord_exact_{crit}"] = round(sum(x == y for x, y in zip(hum, jug)) / len(hum), 4)
        res[f"kappa_pondere_{crit}"] = weighted_kappa(hum, jug)
        res[f"n_{crit}"] = len(hum)
        res[f"ecart_moyen_{crit}"] = round(sum(y - x for x, y in zip(hum, jug)) / len(hum), 4)  # >0 : juge plus généreux
    if not res:
        raise SystemExit("Aucune note humaine remplie dans la feuille.")
    print(json.dumps(res, indent=1, ensure_ascii=False))
    mlflow = _mlflow()
    mlflow.set_experiment("diagnostic-airflow")
    with mlflow.start_run(run_name="verification-manuelle-juge"):
        mlflow.set_tags({"type": "verification_manuelle",
                         "fake": str(any(v.get("fake", True) for v in keys.values())).lower()})
        mlflow.log_param("runs_echantillonnes", ",".join(sorted({v["run"] for v in keys.values()})))
        mlflow.log_metrics({k: float(v) for k, v in res.items() if v is not None})
        mlflow.log_artifact(str(sheet))


def main() -> None:
    p = argparse.ArgumentParser()
    sub = p.add_subparsers(dest="cmd", required=True)
    s = sub.add_parser("sample")
    s.add_argument("--runs", nargs="+", required=True)
    s.add_argument("--n", type=int, default=15)
    s.add_argument("--seed", type=int, default=2026)
    a = sub.add_parser("accord")
    a.add_argument("sheet", type=Path)
    args = p.parse_args()
    if args.cmd == "sample":
        sample(args.runs, args.n, args.seed)
    else:
        agreement(args.sheet)


if __name__ == "__main__":
    main()
