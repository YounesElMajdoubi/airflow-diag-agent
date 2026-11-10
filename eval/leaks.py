"""Détection des fuites de secrets : la mesure qui vérifie le masquage.

Des secrets « canaris » (fausses valeurs, mais placées là où de vrais secrets
fuiraient) sont plantés dans le pipeline cobaye : mot de passe de l'entrepôt,
clé d'API du fournisseur, DSN écrit dans le code. L'injecteur les enregistre
dans le manifeste. Ici, on inspecte CHAQUE requête envoyée à un modèle : si un
canari y figure, le masquage a échoué. L'objectif est 0.

Le masqueur (mcp_server/redaction.py) n'a jamais accès à cette liste.
"""

from __future__ import annotations

import json
import os
import re
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent


def load_canaries(manifest_path: Path = ROOT / "faults" / "manifest.json") -> list[str]:
    values: set[str] = set()
    if manifest_path.exists():
        values.update(json.loads(manifest_path.read_text()).get("canaris", []))
    for var in ("ENTREPOT_PASSWORD", "FOURNISSEUR_API_KEY"):
        if os.environ.get(var):
            values.add(os.environ[var])
    dag = (ROOT / "airflow" / "dags" / "cobaye_ventes.py").read_text()
    values.update(re.findall(r"etl_writer:([^@\"]+)@", dag))
    return sorted(v for v in values if len(v) >= 8)


class LeakDetector:
    def __init__(self, canaries: list[str]):
        if not canaries:
            raise ValueError("aucun canari : la mesure des fuites serait vide de sens")
        self.canaries = canaries
        self.leaks: list[dict] = []

    def check(self, alias: str, messages: list[dict]) -> None:
        text = json.dumps(messages, ensure_ascii=False)
        for c in self.canaries:
            if c in text:
                self.leaks.append({"modele": alias, "canari": c[:4] + "…"})

    def reset(self) -> int:
        n = len(self.leaks)
        self.leaks = []
        return n
