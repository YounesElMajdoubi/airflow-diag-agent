"""Schémas Pydantic des sorties de l'agent.

Chaque étape LLM doit produire un JSON conforme à l'un de ces schémas. Pydantic
valide types, bornes et longueurs ; agent/structured.py relance le modèle avec
le message d'erreur si la sortie est invalide. Le rapport final est lui aussi
un modèle Pydantic : c'est le « contrat » de sortie de l'agent.
"""

from __future__ import annotations

from enum import Enum
from typing import Literal

from pydantic import BaseModel, ConfigDict, Field


class Categorie(str, Enum):
    donnee_manquante = "donnee_manquante"
    schema_modifie = "schema_modifie"
    delai_depasse = "delai_depasse"
    erreur_code = "erreur_code"
    dependance_indisponible = "dependance_indisponible"
    droits_insuffisants = "droits_insuffisants"
    indetermine = "indetermine"  # échappatoire : sans elle, le modèle force chaque panne dans une case


class _Strict(BaseModel):
    model_config = ConfigDict(extra="forbid")


class Classification(_Strict):
    categorie: Categorie
    confiance: float = Field(ge=0, le=1, description="probabilité estimée que la catégorie soit correcte")
    justification: str = Field(min_length=10, max_length=800)


class Citation(_Strict):
    ligne: int = Field(ge=1, description="numéro de ligne n du journal")
    extrait: str = Field(min_length=3, max_length=400, description="texte recopié de cette ligne")


class Diagnostic(_Strict):
    cause: str = Field(min_length=10, max_length=600, description="cause racine en une ou deux phrases")
    citations: list[Citation] = Field(min_length=1, max_length=6)
    explication: str = Field(min_length=10, max_length=1500)


class Correction(_Strict):
    resume: str = Field(min_length=10, max_length=500)
    nature: Literal["code", "configuration", "donnees", "infrastructure", "acces"]
    touche_donnees: bool = Field(description="la correction exige-t-elle de modifier, redéposer ou migrer des données ?")
    diff: str | None = Field(default=None, description="diff unifié sur le source du DAG, ou null")
    etapes: list[str] = Field(min_length=1, max_length=6)


class Escalade(_Strict):
    escalader: bool
    raisons: list[str]


class AppelLLM(BaseModel):
    etape: str
    modele: str
    tokens_entree: int = 0
    tokens_sortie: int = 0
    cout_usd: float | None = None
    duree_s: float = 0.0
    valide: bool = True


class RapportDiagnostic(BaseModel):
    """Sortie finale de l'agent."""

    dag_id: str
    run_id: str
    task_id: str
    tentative: int
    strategie: str
    modele_principal: str
    classification: Classification
    diagnostic: Diagnostic | None
    correction: Correction | None
    escalade: Escalade
    recidive: int = Field(description="échecs consécutifs de la tâche, ce run compris")
    citations_valides: bool
    diff_valide: bool | None
    relances: int
    appels: list[AppelLLM]
    cout_usd: float | None
    duree_s: float
    erreurs: list[str] = []
