"""Campagne d'évaluation : rejoue les pannes capturées pour une ou plusieurs configurations
et enregistre tout dans MLflow (un run MLflow par configuration).

Chaque scénario est rejoué à partir de son instantané : toutes les configurations
voient EXACTEMENT les mêmes journaux, le même source et le même historique. La
comparaison entre modèles et stratégies est donc équitable et reproductible.

Usage :
    python -m eval.run_campaign --config gros-deepseek-pro
    python -m eval.run_campaign --all                       # toutes les configurations
    python -m eval.run_campaign --config petit-local --scenarios ci
    python -m eval.run_campaign --config gros-deepseek-pro --fake   # test sans réseau (exclu du README)
"""

from __future__ import annotations

import argparse
import asyncio
import hashlib
import json
import os
import subprocess
import sys
import tempfile
import time
from pathlib import Path

import yaml

from agent.escalation import CONFIDENCE_THRESHOLD
from agent.graph import SEUIL_ROUTAGE, Deps, diagnose
from agent.llm import LiteLLMClient
from agent.mcp_tools import connect
from eval import metrics
from eval.fake_models import fake
from eval.judge import judge
from eval.leaks import LeakDetector, load_canaries
from faults.catalog import Scenario, load_scenarios

ROOT = Path(__file__).resolve().parent.parent
CAMPAGNES = yaml.safe_load((ROOT / "eval" / "campagnes.yaml").read_text())
MANIFEST = ROOT / "faults" / "manifest.json"


def select_scenarios(which: str) -> list[tuple[Scenario, dict]]:
    """Scénarios à rejouer, avec leur entrée de manifeste. Les scénarios invalides sont écartés."""
    if not MANIFEST.exists():
        sys.exit("Aucun manifeste : lancer d'abord `make pannes` (faults/inject.py) pour capturer les instantanés.")
    manifest = json.loads(MANIFEST.read_text())["scenarios"]
    scenarios = load_scenarios()
    if which == "ci":
        scenarios = [s for s in scenarios if s.ci]
    elif which != "all":
        wanted = set(which.split(","))
        scenarios = [s for s in scenarios if s.id in wanted]
    out = []
    for s in scenarios:
        entry = manifest.get(s.id)
        if not entry:
            print(f"  [ignoré] {s.id} : pas d'instantané capturé")
        elif not entry["valide"]:
            print(f"  [ignoré] {s.id} : capture invalide (tâche observée {entry['tache_observee']})")
        else:
            out.append((s, entry))
    if not out:
        sys.exit("Aucun scénario rejouable.")
    return out


def snapshots_hash(items) -> str:
    h = hashlib.sha256()
    for _, entry in items:
        h.update((ROOT / entry["instantane"]).read_bytes())
    return h.hexdigest()[:12]


def git_commit() -> str:
    try:
        return subprocess.run(["git", "rev-parse", "--short", "HEAD"], cwd=ROOT, capture_output=True,
                              text=True, check=True).stdout.strip()
    except (subprocess.CalledProcessError, FileNotFoundError):
        return "inconnu"


async def run_config(name: str, cfg: dict, items, *, use_judge: bool, use_fake: bool,
                     leak: LeakDetector, verbose: bool = True) -> list[dict]:
    def make(alias):
        if not alias:
            return None
        return fake(alias, on_request=leak.check) if use_fake else LiteLLMClient(alias, on_request=leak.check)

    judge_llm = None
    if use_judge:
        judge_alias = CAMPAGNES["juge"]
        judge_llm = fake(judge_alias) if use_fake else LiteLLMClient(judge_alias)

    results = []
    for s, entry in items:
        t0 = time.perf_counter()
        rec = {
            "scenario": s.id, "categorie_attendue": s.categorie, "tache_attendue": s.tache_attendue,
            "escalade_attendue": s.escalade_attendue, "run_id_attendu": entry["run_id"],
        }
        try:
            async with connect(snapshot=str(ROOT / entry["instantane"])) as tools:
                deps = Deps(tools=tools, strategie=cfg["strategie"], petit=make(cfg.get("petit")), gros=make(cfg.get("gros")))
                rapport = (await diagnose(deps, "cobaye_ventes", since="3650d")).model_dump(mode="json")
            rec.update({
                "categorie_predite": rapport["classification"]["categorie"],
                "confiance": rapport["classification"]["confiance"],
                "escalade_predite": rapport["escalade"]["escalader"],
                "raisons_escalade": rapport["escalade"]["raisons"],
                "tache_diagnostiquee": rapport["task_id"],
                "citations_valides": rapport["citations_valides"],
                "diff_valide": rapport["diff_valide"],
                "relances": rapport["relances"],
                "cout_usd": rapport["cout_usd"],
                "duree_s": rapport["duree_s"],
                "modele_principal": rapport["modele_principal"],
                "rapport": rapport,
            })
            if cfg["strategie"] == "routage":
                rec["passe_au_gros"] = rapport["modele_principal"] == cfg["gros"]
        except Exception as e:  # un plantage de l'agent est un résultat (compté comme erreur), pas une exclusion
            rec.update({"erreur_agent": f"{type(e).__name__}: {e}", "categorie_predite": None,
                        "duree_s": round(time.perf_counter() - t0, 2)})
        rec["fuites"] = leak.reset()
        if judge_llm and rec.get("rapport"):
            try:
                note, appels = await judge(judge_llm, s.cause_reelle, s.categorie, rec["rapport"])
                if note:
                    rec.update(note_diagnostic=note.note_diagnostic, note_correction=note.note_correction,
                               justification_juge=note.justification)
                else:
                    rec["erreur_juge"] = "sortie du juge invalide après relances"
                rec["cout_juge_usd"] = sum(a.cout_usd or 0 for a in appels)
            except Exception as e:  # le juge ne doit pas interrompre la campagne
                rec["erreur_juge"] = f"{type(e).__name__}: {e}"
        results.append(rec)
        if verbose:
            mark = "ok " if rec.get("categorie_predite") == s.categorie else "ERR"
            print(f"  [{mark}] {s.id:4} attendu={s.categorie:24} prédit={str(rec.get('categorie_predite')):24} "
                  f"escalade={rec.get('escalade_predite')}/{s.escalade_attendue} "
                  f"{rec.get('duree_s', 0):.1f}s" + (f"  {rec['erreur_agent']}" if rec.get("erreur_agent") else ""))
    return results


def log_mlflow(name: str, cfg: dict, results: list[dict], m: dict, *, jeu: str, fake_mode: bool,
               use_judge: bool, snap_hash: str) -> str:
    import mlflow

    mlflow.set_tracking_uri(os.environ.get("MLFLOW_TRACKING_URI", "http://localhost:5000"))
    mlflow.set_experiment(CAMPAGNES["experiment"])
    with mlflow.start_run(run_name=name) as run:
        mlflow.set_tags({"type": "campagne", "configuration": name, "strategie": cfg["strategie"],
                         "jeu": jeu, "fake": str(fake_mode).lower(), "git_commit": git_commit(),
                         "instantanes_sha": snap_hash})
        mlflow.log_params({"strategie": cfg["strategie"], "petit": cfg.get("petit", "-"), "gros": cfg.get("gros", "-"),
                           "juge": CAMPAGNES["juge"] if use_judge else "-", "n_scenarios": len(results),
                           "seuil_confiance_escalade": CONFIDENCE_THRESHOLD, "seuil_routage": SEUIL_ROUTAGE})
        mlflow.log_metrics({k: float(v) for k, v in m.items() if isinstance(v, (int, float)) and v is not None})
        for cat in metrics.CLASSES:
            sub = [r for r in results if r["categorie_attendue"] == cat]
            if sub:
                mlflow.log_metric(f"exactitude_{cat}", sum(r.get("categorie_predite") == cat for r in sub) / len(sub))
        mlflow.log_metric("cout_juge_usd", sum(r.get("cout_juge_usd", 0) for r in results))
        mlflow.log_dict(metrics.confusion(results), "matrice_confusion.json")
        mlflow.log_text(metrics.confusion_markdown(results), "matrice_confusion.md")
        with tempfile.TemporaryDirectory() as tmp:
            p = Path(tmp) / "resultats.jsonl"
            p.write_text("\n".join(json.dumps({k: v for k, v in r.items() if k != "rapport"}, ensure_ascii=False)
                                   for r in results), encoding="utf-8")
            mlflow.log_artifact(str(p))
            rdir = Path(tmp) / "rapports"
            rdir.mkdir()
            for r in results:
                if r.get("rapport"):
                    (rdir / f"{r['scenario']}.json").write_text(json.dumps(r["rapport"], ensure_ascii=False, indent=1))
            mlflow.log_artifacts(str(rdir), "rapports")
        return run.info.run_id


async def amain(args) -> None:
    names = list(CAMPAGNES["configurations"]) if args.all else args.config
    unknown = set(names) - set(CAMPAGNES["configurations"])
    if unknown:
        sys.exit(f"Configurations inconnues : {sorted(unknown)}")
    items = select_scenarios(args.scenarios)
    leak = LeakDetector(load_canaries())
    snap_hash = snapshots_hash(items)
    for name in names:
        cfg = CAMPAGNES["configurations"][name]
        print(f"== {name} ({len(items)} scénarios){' [FAUX MODÈLE]' if args.fake else ''}")
        results = await run_config(name, cfg, items, use_judge=not args.no_judge, use_fake=args.fake, leak=leak)
        m = metrics.compute(results)
        print("   " + ", ".join(f"{k}={m[k]}" for k in ("exactitude", "escalades_correctes", "justesse_diagnostic",
                                                         "cout_par_diagnostic_usd", "duree_moy_s", "fuites_secrets") if k in m))
        if not args.no_mlflow:
            run_id = log_mlflow(name, cfg, results, m, jeu=args.scenarios, fake_mode=args.fake,
                                use_judge=not args.no_judge, snap_hash=snap_hash)
            print(f"   MLflow run {run_id}")
        if args.out:
            Path(args.out).write_text(json.dumps({"configuration": name, "metriques": m, "resultats": results},
                                                 ensure_ascii=False, indent=1))


def main() -> None:
    p = argparse.ArgumentParser(description="Campagne d'évaluation de l'agent")
    g = p.add_mutually_exclusive_group(required=True)
    g.add_argument("--config", action="append", help="nom de configuration (répétable)")
    g.add_argument("--all", action="store_true")
    p.add_argument("--scenarios", default="all", help="all, ci, ou liste d'identifiants (D01,S03)")
    p.add_argument("--no-judge", action="store_true", help="sans LLM juge (pas de note de justesse)")
    p.add_argument("--no-mlflow", action="store_true")
    p.add_argument("--fake", action="store_true", help="faux modèle heuristique (test de la chaîne, exclu du README)")
    p.add_argument("--out", help="écrit aussi les résultats JSON dans ce fichier")
    asyncio.run(amain(p.parse_args()))


if __name__ == "__main__":
    main()
