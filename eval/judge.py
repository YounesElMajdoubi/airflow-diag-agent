"""LLM juge : note la justesse du diagnostic et de la correction avec une grille écrite.

Précautions contre les biais :
- le juge ne voit ni le modèle ni la stratégie (évaluation à l'aveugle) ;
- la grille (eval/grille_juge.md) est fixe et versionnée ;
- température 0 ; un alias LiteLLM dédié (« juge ») pour séparer son coût ;
- ses notes sont VÉRIFIÉES À LA MAIN sur un échantillon (eval/manual_check.py),
  et l'accord humain/juge est publié (kappa pondéré).
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import Literal

from pydantic import BaseModel, ConfigDict, Field

from agent.llm import LLM
from agent.schemas import AppelLLM
from agent.structured import call_structured

GRILLE = (Path(__file__).with_name("grille_juge.md")).read_text(encoding="utf-8")


class NoteJuge(BaseModel):
    model_config = ConfigDict(extra="forbid")
    note_diagnostic: Literal[0, 1, 2]
    note_correction: Literal[0, 1, 2]
    justification: str = Field(min_length=5, max_length=600)


SYSTEM = ("Tu es un évaluateur rigoureux de diagnostics d'incidents de pipelines de données. "
          "Applique strictement la grille suivante.\n\n" + GRILLE)


def judge_input(cause_reelle: str, categorie: str, rapport: dict) -> str:
    diag = rapport.get("diagnostic") or {}
    corr = rapport.get("correction") or {}
    return json.dumps({
        "cause_reelle": cause_reelle,
        "categorie_attendue": categorie,
        "diagnostic_agent": {"cause": diag.get("cause"), "explication": diag.get("explication")},
        "correction_agent": {"resume": corr.get("resume"), "etapes": corr.get("etapes"), "diff": corr.get("diff")},
    }, ensure_ascii=False, indent=1)


async def judge(llm: LLM, cause_reelle: str, categorie: str, rapport: dict) -> tuple[NoteJuge | None, list[AppelLLM]]:
    if not rapport.get("diagnostic"):
        return NoteJuge(note_diagnostic=0, note_correction=0, justification="aucun diagnostic produit"), []
    res = await call_structured(llm, "juge", SYSTEM, judge_input(cause_reelle, categorie, rapport), NoteJuge)
    return res.value, res.appels
