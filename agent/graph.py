"""Agent de diagnostic : graphe LangGraph en 5 étapes (+ un aiguillage pour le routage).

    START
      │
    collecter_indices        (aucun LLM : appels MCP)
      │
    classer                  (LLM « classifieur » : petit modèle en routage)
      │──────────────┐  aiguillage : cas difficile ET stratégie « routage » ?
      │              ▼
      │        reclasser_gros   (le gros modèle reprend la classification)
      │              │
    diagnostiquer ◄──┘       (LLM principal : cite les lignes du journal)
      │
    proposer_correction      (LLM principal : diff jamais appliqué)
      │
    decider_escalade         (aucun LLM : règle explicite) -> rapport Pydantic
      │
     END

Pourquoi LangGraph plutôt qu'une simple suite d'appels : l'état est typé et
partagé, chaque étape est un nœud testable isolément, et l'aiguillage du
routage est une arête conditionnelle visible dans le graphe (pas un if caché).

Stratégies :
  gros    : toutes les étapes LLM avec le gros modèle ;
  petit   : toutes avec le petit modèle ;
  routage : le petit classe ; si la classification est difficile (sortie invalide,
            « indetermine » ou confiance < SEUIL_ROUTAGE), le gros reprend et fait le reste.
"""

from __future__ import annotations

import operator
import time
from dataclasses import dataclass
from typing import Annotated, Any, Literal, TypedDict

from langgraph.graph import END, START, StateGraph

from agent import prompts
from agent.escalation import decide
from agent.llm import LLM
from agent.mcp_tools import MCPTools, ToolCallError
from agent.schemas import (AppelLLM, Categorie, Classification, Correction, Diagnostic,
                           RapportDiagnostic)
from agent.structured import call_structured
from agent.validation import check_citations, check_diff

SEUIL_ROUTAGE = 0.75
Strategie = Literal["gros", "petit", "routage"]


class NoFailureFound(RuntimeError):
    pass


class DiagState(TypedDict, total=False):
    dag_id: str
    run_id: str | None
    since: str
    t0: float
    indices: dict[str, Any]
    classification: dict | None
    classification_petit: dict | None
    modele_principal: str
    diagnostic: dict | None
    citations_valides: bool
    correction: dict | None
    diff_valide: bool | None
    etapes_ratees: Annotated[list[str], operator.add]
    appels: Annotated[list[dict], operator.add]
    erreurs: Annotated[list[str], operator.add]
    relances: Annotated[int, operator.add]
    rapport: dict


@dataclass
class Deps:
    tools: MCPTools
    strategie: Strategie
    petit: LLM | None = None
    gros: LLM | None = None

    def __post_init__(self):
        if self.strategie in ("petit", "routage") and not self.petit:
            raise ValueError(f"la stratégie {self.strategie} demande un petit modèle")
        if self.strategie in ("gros", "routage") and not self.gros:
            raise ValueError(f"la stratégie {self.strategie} demande un gros modèle")

    def llm(self, role: str) -> LLM:
        return self.gros if role == "gros" else self.petit


def _recidive(entries: list[dict], run_id: str) -> int:
    """Échecs consécutifs de la tâche en remontant le temps depuis le run diagnostiqué (inclus)."""
    idx = next((i for i, e in enumerate(entries) if e["run_id"] == run_id), None)
    if idx is None:
        return 1
    n = 0
    for e in entries[idx:]:
        if e["etat"] not in ("failed", "success"):
            continue  # run sans exécution de cette tâche (ex. bloquée en amont)
        if e["etat"] != "failed":
            break
        n += 1
    return max(n, 1)


def build_graph(deps: Deps):
    tools = deps.tools

    # ------------------------------------------------------------ 1. collecter
    async def collecter_indices(state: DiagState) -> dict:
        dag_id = state["dag_id"]
        failed = await tools.call("list_failed_runs", {"dag_id": dag_id, "since": state.get("since", "7d")})
        runs = failed["executions"]
        if state.get("run_id"):
            runs = [r for r in runs if r["run_id"] == state["run_id"]]
        runs = [r for r in runs if r["taches_en_echec"]]
        if not runs:
            raise NoFailureFound(f"aucune exécution en échec trouvée pour {dag_id}")
        run = {**runs[0], "dag_id": dag_id}  # la plus récente
        task = run["taches_en_echec"][0]     # la première tâche tombée = la plus en amont
        journal = await tools.call("get_task_logs", {"dag_id": dag_id, "run_id": run["run_id"], "task_id": task["task_id"]})
        historique = await tools.call("get_task_history", {"dag_id": dag_id, "task_id": task["task_id"]})
        source = await tools.call("get_dag_source", {"dag_id": dag_id, "run_id": run["run_id"]})
        indices = {
            "run": run, "tache": task, "journal": journal, "historique": historique, "source": source,
            "autres_echecs": [t["task_id"] for t in run["taches_en_echec"][1:]],
            "recidive": _recidive(historique["entrees"], run["run_id"]),
        }
        return {"run_id": run["run_id"], "indices": indices}

    # --------------------------------------------------------------- 2. classer
    async def _classify(state: DiagState, role: str) -> dict:
        res = await call_structured(deps.llm(role), f"classer[{role}]", prompts.CLASSIFY_SYSTEM,
                                    prompts.classify_prompt(state["indices"]), Classification)
        out = {"appels": [a.model_dump() for a in res.appels], "erreurs": res.erreurs, "relances": res.relances,
               "classification": res.value.model_dump(mode="json") if res.value else None}
        if not res.value:
            out["etapes_ratees"] = [f"classer[{role}]"]
        return out

    async def classer(state: DiagState) -> dict:
        role = "gros" if deps.strategie == "gros" else "petit"
        out = await _classify(state, role)
        out["modele_principal"] = role
        if deps.strategie == "routage":
            out["classification_petit"] = out["classification"]
        return out

    def aiguillage(state: DiagState) -> str:
        if deps.strategie != "routage":
            return "diagnostiquer"
        c = state.get("classification")
        difficile = (c is None or c["categorie"] == Categorie.indetermine.value or c["confiance"] < SEUIL_ROUTAGE)
        return "reclasser_gros" if difficile else "diagnostiquer"

    async def reclasser_gros(state: DiagState) -> dict:
        out = await _classify(state, "gros")
        out["modele_principal"] = "gros"
        return out

    # --------------------------------------------------------- 3. diagnostiquer
    async def diagnostiquer(state: DiagState) -> dict:
        ind = state["indices"]
        lines = {ln["n"]: ln["texte"] for ln in ind["journal"]["lignes"]}
        cat = (state.get("classification") or {}).get("categorie", "indetermine")
        res = await call_structured(
            deps.llm(state["modele_principal"]), "diagnostiquer",
            prompts.DIAGNOSE_SYSTEM.format(categorie=cat),
            prompts.diagnose_prompt(ind, state.get("classification") or {}), Diagnostic,
            validate=lambda d: check_citations(d, lines),
        )
        diag = res.value or res.last_value
        out = {"appels": [a.model_dump() for a in res.appels], "erreurs": res.erreurs, "relances": res.relances,
               "diagnostic": diag.model_dump() if diag else None, "citations_valides": res.value is not None}
        if diag is None:
            out["etapes_ratees"] = ["diagnostiquer"]
        return out

    # --------------------------------------------------- 4. proposer_correction
    async def proposer_correction(state: DiagState) -> dict:
        ind = state["indices"]
        source = ind["source"]["source"]
        diag = state.get("diagnostic") or {}

        def validate(c: Correction) -> list[str]:
            if c.nature == "code" and not c.diff:
                return ["nature = code mais aucun diff n'est fourni"]
            return check_diff(c.diff, source)[1]

        res = await call_structured(
            deps.llm(state["modele_principal"]), "proposer_correction",
            prompts.FIX_SYSTEM.format(categorie=(state.get("classification") or {}).get("categorie", "indetermine"),
                                      cause=diag.get("cause", "inconnue")),
            prompts.fix_prompt(ind, state.get("diagnostic")), Correction, validate=validate,
        )
        corr = res.value or res.last_value
        out = {"appels": [a.model_dump() for a in res.appels], "erreurs": res.erreurs, "relances": res.relances,
               "correction": corr.model_dump() if corr else None,
               "diff_valide": check_diff(corr.diff, source)[0] if corr else None}
        if corr is None:
            out["etapes_ratees"] = ["proposer_correction"]
        return out

    # ------------------------------------------------------ 5. decider_escalade
    async def decider_escalade(state: DiagState) -> dict:
        ind = state["indices"]
        classif = (Classification.model_validate(state["classification"]) if state.get("classification")
                   else Classification(categorie=Categorie.indetermine, confiance=0.0,
                                       justification="aucune classification valide produite"))
        corr = Correction.model_validate(state["correction"]) if state.get("correction") else None
        esc = decide(classif, corr, ind["recidive"], state.get("citations_valides", False),
                     state.get("etapes_ratees", []))
        appels = [AppelLLM.model_validate(a) for a in state.get("appels", [])]
        costs = [a.cout_usd for a in appels if a.cout_usd is not None]
        modele = deps.llm(state["modele_principal"]).alias
        rapport = RapportDiagnostic(
            dag_id=state["dag_id"], run_id=state["run_id"], task_id=ind["tache"]["task_id"],
            tentative=ind["journal"]["tentative"], strategie=deps.strategie, modele_principal=modele,
            classification=classif,
            diagnostic=Diagnostic.model_validate(state["diagnostic"]) if state.get("diagnostic") else None,
            correction=corr, escalade=esc, recidive=ind["recidive"],
            citations_valides=state.get("citations_valides", False), diff_valide=state.get("diff_valide"),
            relances=state.get("relances", 0), appels=appels,
            cout_usd=round(sum(costs), 6) if costs else None,
            duree_s=round(time.perf_counter() - state["t0"], 2), erreurs=state.get("erreurs", []),
        )
        return {"rapport": rapport.model_dump(mode="json")}

    g = StateGraph(DiagState)
    g.add_node("collecter_indices", collecter_indices)
    g.add_node("classer", classer)
    g.add_node("reclasser_gros", reclasser_gros)
    g.add_node("diagnostiquer", diagnostiquer)
    g.add_node("proposer_correction", proposer_correction)
    g.add_node("decider_escalade", decider_escalade)
    g.add_edge(START, "collecter_indices")
    g.add_edge("collecter_indices", "classer")
    g.add_conditional_edges("classer", aiguillage, ["diagnostiquer", "reclasser_gros"])
    g.add_edge("reclasser_gros", "diagnostiquer")
    g.add_edge("diagnostiquer", "proposer_correction")
    g.add_edge("proposer_correction", "decider_escalade")
    g.add_edge("decider_escalade", END)
    return g.compile()


async def diagnose(deps: Deps, dag_id: str, run_id: str | None = None, since: str = "7d") -> RapportDiagnostic:
    graph = build_graph(deps)
    state = await graph.ainvoke({"dag_id": dag_id, "run_id": run_id, "since": since, "t0": time.perf_counter(),
                                 "etapes_ratees": [], "appels": [], "erreurs": [], "relances": 0})
    return RapportDiagnostic.model_validate(state["rapport"])


__all__ = ["Deps", "build_graph", "diagnose", "NoFailureFound", "ToolCallError", "SEUIL_ROUTAGE"]
