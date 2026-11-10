"""Évaluation courte pour la CI : échoue si l'exactitude baisse par rapport à la référence.

- rejoue les scénarios marqués `ci: true` avec la configuration de eval/campagnes.yaml (section ci) ;
- compare l'exactitude à eval/baseline_ci.json, qui provient d'une VRAIE exécution
  (créée avec --write-baseline, jamais écrite à la main) ;
- code de sortie 1 si exactitude < référence - tolérance.

La tolérance vaut 0 par défaut (« échoue si l'exactitude baisse »). Les modèles ne
sont pas parfaitement déterministes, même à température 0 : si la CI devient
instable, on peut relever la tolérance dans baseline_ci.json (décision à tracer
dans DECISIONS.md).

Usage :
    python -m eval.ci_eval                    # compare à la référence
    python -m eval.ci_eval --write-baseline   # enregistre la référence (après une exécution réelle)
"""

from __future__ import annotations

import argparse
import asyncio
import json
import sys
from datetime import datetime, timezone
from pathlib import Path

from eval import metrics
from eval.leaks import LeakDetector, load_canaries
from eval.run_campaign import CAMPAGNES, git_commit, run_config, select_scenarios, snapshots_hash

BASELINE = Path(__file__).with_name("baseline_ci.json")


async def amain(args) -> int:
    ci = CAMPAGNES["ci"]
    name = ci["configuration"]
    cfg = CAMPAGNES["configurations"][name]
    items = select_scenarios("ci")
    print(f"Évaluation CI : {name}, {len(items)} scénarios")
    results = await run_config(name, cfg, items, use_judge=bool(ci.get("juge")), use_fake=args.fake,
                               leak=LeakDetector(load_canaries()))
    m = metrics.compute(results)
    print(json.dumps({k: m[k] for k in ("exactitude", "escalades_correctes", "citations_valides", "fuites_secrets",
                                        "echecs_agent", "cout_total_usd")}, ensure_ascii=False))
    if args.out:
        Path(args.out).write_text(json.dumps({"metriques": m, "resultats": results}, ensure_ascii=False, indent=1))

    if m["fuites_secrets"]:
        print(f"ÉCHEC : {m['fuites_secrets']} fuite(s) de secret détectée(s) dans les requêtes aux modèles.")
        return 1
    if args.write_baseline:
        if args.fake:
            print("Refus : une référence ne peut pas venir du faux modèle.")
            return 1
        BASELINE.write_text(json.dumps({
            "configuration": name, "exactitude": m["exactitude"], "n": m["n"], "tolerance": 0.0,
            "scenarios": [s.id for s, _ in items], "instantanes_sha": snapshots_hash(items),
            "git_commit": git_commit(), "date": datetime.now(timezone.utc).isoformat(timespec="seconds"),
        }, indent=1))
        print(f"Référence écrite : {BASELINE}")
        return 0
    if not BASELINE.exists():
        print("ÉCHEC : pas de référence eval/baseline_ci.json. La créer une fois avec "
              "`python -m eval.ci_eval --write-baseline` (exécution réelle) et la versionner.")
        return 1
    base = json.loads(BASELINE.read_text())
    seuil = base["exactitude"] - base.get("tolerance", 0.0)
    verdict = m["exactitude"] >= seuil
    print(f"Exactitude {m['exactitude']:.3f} — référence {base['exactitude']:.3f} "
          f"(tolérance {base.get('tolerance', 0.0)}) -> {'OK' if verdict else 'BAISSE'}")
    return 0 if verdict else 1


def main() -> None:
    p = argparse.ArgumentParser()
    p.add_argument("--write-baseline", action="store_true")
    p.add_argument("--fake", action="store_true", help="faux modèle (test de la mécanique uniquement)")
    p.add_argument("--out")
    sys.exit(asyncio.run(amain(p.parse_args())))


if __name__ == "__main__":
    main()
