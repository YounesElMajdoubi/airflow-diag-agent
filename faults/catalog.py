"""Catalogue des pannes : chargement, validation, politique d'escalade, données de test.

La politique d'escalade est écrite UNE seule fois et sert deux fois :
- ici, pour calculer la vérité terrain (escalade attendue) de chaque scénario ;
- dans agent/escalation.py, qui applique les mêmes règles aux sorties du modèle.
Ainsi, une erreur d'escalade de l'agent vient forcément d'une mauvaise
classification / estimation, pas d'une règle différente.
"""

from __future__ import annotations

import random
from dataclasses import dataclass, field
from datetime import date
from pathlib import Path

import yaml

from agent.policy import escalation_reasons

CATEGORIES = (
    "donnee_manquante",
    "schema_modifie",
    "delai_depasse",
    "erreur_code",
    "dependance_indisponible",
    "droits_insuffisants",
)
SCENARIOS_FILE = Path(__file__).with_name("scenarios.yaml")
ORDER_VARIANTS = {"missing", "empty", "header_only", "null_amount", "rename_amount", "date_format"}
ACTIONS = {"orders", "api", "params", "sql", "sql_db", "sql_lock", "stop", "variable", "patch_dag", "chmod_reports"}


@dataclass
class Scenario:
    id: str
    titre: str
    categorie: str
    cause_reelle: str
    tache_attendue: str
    touche_donnees: bool
    injection: list[dict]
    recidive: int = 1
    ci: bool = False
    extra: dict = field(default_factory=dict)

    @property
    def escalade_attendue(self) -> bool:
        return expected_escalation(self.categorie, self.touche_donnees, self.recidive)

    @property
    def raisons_escalade_attendues(self) -> list[str]:
        return escalation_reasons(self.categorie, self.touche_donnees, self.recidive)


def expected_escalation(categorie: str, touche_donnees: bool, recidive: int) -> bool:
    return bool(escalation_reasons(categorie, touche_donnees, recidive))


def load_scenarios(path: Path = SCENARIOS_FILE) -> list[Scenario]:
    raw = yaml.safe_load(path.read_text(encoding="utf-8"))
    known = set(Scenario.__dataclass_fields__)
    scenarios = [Scenario(**{k: v for k, v in item.items() if k in known},
                          extra={k: v for k, v in item.items() if k not in known}) for item in raw]
    _validate(scenarios)
    return scenarios


def _validate(scenarios: list[Scenario]) -> None:
    ids = [s.id for s in scenarios]
    if len(ids) != len(set(ids)):
        raise ValueError("identifiants de scénarios en double")
    for s in scenarios:
        if s.categorie not in CATEGORIES:
            raise ValueError(f"{s.id} : catégorie inconnue {s.categorie}")
        for action in s.injection:
            unknown = set(action) - ACTIONS
            if unknown:
                raise ValueError(f"{s.id} : action inconnue {unknown}")
            if "orders" in action and action["orders"] not in ORDER_VARIANTS:
                raise ValueError(f"{s.id} : variante de fichier inconnue {action['orders']}")


# ---------------------------------------------------------------- données de test

def orders_csv(ds: date, variant: str | None = None, n: int = 20) -> bytes | None:
    """Fichier de commandes du jour `ds`. Déterministe (graine = date). None = fichier absent."""
    if variant == "missing":
        return None
    if variant == "empty":
        return b""
    rng = random.Random(ds.toordinal())
    amount_col = "montant" if variant == "rename_amount" else "amount"
    lines = [f"order_id,order_date,currency,{amount_col}"]
    if variant != "header_only":
        for i in range(1, n + 1):
            currency = rng.choice(["EUR", "EUR", "USD", "GBP"])
            amount = f"{rng.randint(5, 400)}.{rng.randint(1, 99):02d}"  # toujours des centimes
            if variant == "null_amount" and i in (4, 11):
                amount = ""
            d = ds.strftime("%d/%m/%Y") if variant == "date_format" else ds.isoformat()
            lines.append(f"CMD-{ds:%Y%m%d}-{i:03d},{d},{currency},{amount}")
    return ("\n".join(lines) + "\n").encode()
