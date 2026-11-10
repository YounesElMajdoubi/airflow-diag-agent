"""Ligne de commande de l'agent.

Exemples :
    # diagnostiquer le dernier échec du DAG (Airflow réel, serveur MCP lancé en stdio)
    python -m agent.cli --dag cobaye_ventes --strategie gros --gros deepseek-pro

    # via le serveur MCP du docker-compose
    python -m agent.cli --dag cobaye_ventes --mcp-url http://localhost:8765/mcp --strategie routage \
        --petit mistral-small --gros deepseek-pro

    # sur un instantané enregistré (sans Airflow)
    python -m agent.cli --snapshot faults/snapshots/U01.json --strategie petit --petit local
"""

from __future__ import annotations

import argparse
import asyncio
import sys

from agent.graph import Deps, NoFailureFound, diagnose
from agent.llm import LiteLLMClient
from agent.mcp_tools import ToolCallError, connect
from agent.schemas import RapportDiagnostic


def render_markdown(r: RapportDiagnostic) -> str:
    c, d, f, e = r.classification, r.diagnostic, r.correction, r.escalade
    lines = [
        f"# Diagnostic — {r.dag_id} / {r.task_id}",
        f"Run `{r.run_id}`, tentative {r.tentative} · stratégie **{r.strategie}** (modèle principal : {r.modele_principal})",
        "",
        f"**Catégorie :** {c.categorie.value} (confiance {c.confiance:.2f}) — {c.justification}",
        "",
        "## Cause",
        d.cause if d else "_aucun diagnostic valide_",
    ]
    if d:
        lines += ["", d.explication, "", "**Lignes du journal citées** " + ("(vérifiées)" if r.citations_valides else "(NON vérifiées)") + " :"]
        lines += [f"- L{x.ligne} : `{x.extrait}`" for x in d.citations]
    lines += ["", "## Correction proposée (jamais appliquée)"]
    if f:
        lines += [f"{f.resume} — nature : {f.nature}, touche des données : {'oui' if f.touche_donnees else 'non'}", ""]
        lines += [f"{i}. {s}" for i, s in enumerate(f.etapes, 1)]
        if f.diff:
            lines += ["", "```diff", f.diff.strip("\n"), "```", f"Diff {'applicable' if r.diff_valide else 'NON applicable'} au source du DAG."]
    else:
        lines.append("_aucune correction valide_")
    lines += ["", "## Escalade", ("**Oui** — " + " ; ".join(e.raisons)) if e.escalader else "Non : correction à la portée de l'équipe data."]
    cost = f"{r.cout_usd:.5f} $" if r.cout_usd is not None else "inconnu"
    lines += ["", f"_Récidive : {r.recidive} · {len(r.appels)} appels LLM · {r.relances} relance(s) · coût {cost} · {r.duree_s:.1f} s_"]
    return "\n".join(lines)


async def amain(args) -> int:
    def llm(alias):
        return LiteLLMClient(alias) if alias else None

    async with connect(url=args.mcp_url, snapshot=args.snapshot) as tools:
        deps = Deps(tools=tools, strategie=args.strategie, petit=llm(args.petit), gros=llm(args.gros))
        try:
            rapport = await diagnose(deps, args.dag, run_id=args.run_id, since=args.since)
        except NoFailureFound as e:
            print(f"Rien à diagnostiquer : {e}")
            return 0
        except ToolCallError as e:
            print(f"Erreur d'accès MCP : {e}", file=sys.stderr)
            return 2
    print(rapport.model_dump_json(indent=2) if args.json else render_markdown(rapport))
    if args.out:
        with open(args.out, "w", encoding="utf-8") as fh:
            fh.write(rapport.model_dump_json(indent=2))
    return 0


def main() -> None:
    p = argparse.ArgumentParser(description="Agent de diagnostic de pipelines Airflow")
    p.add_argument("--dag", default="cobaye_ventes")
    p.add_argument("--run-id")
    p.add_argument("--since", default="7d")
    p.add_argument("--strategie", choices=["gros", "petit", "routage"], default="gros")
    p.add_argument("--petit", help="alias LiteLLM du petit modèle (ex. mistral-small, local)")
    p.add_argument("--gros", help="alias LiteLLM du gros modèle (ex. deepseek-pro)")
    src = p.add_mutually_exclusive_group()
    src.add_argument("--mcp-url", help="URL d'un serveur MCP déjà lancé (sinon : stdio)")
    src.add_argument("--snapshot", help="instantané à rejouer (mode sans Airflow)")
    p.add_argument("--json", action="store_true", help="affiche le rapport JSON brut")
    p.add_argument("--out", help="écrit aussi le rapport JSON dans ce fichier")
    args = p.parse_args()
    sys.exit(asyncio.run(amain(args)))


if __name__ == "__main__":
    main()
