"""Faux modèle heuristique, pour TESTER la chaîne d'évaluation sans réseau ni clé d'API.

Il répond au format attendu à partir de mots-clés du journal. Les campagnes qui
l'utilisent sont étiquetées fake=true dans MLflow et sont REFUSÉES par
eval/make_results_table.py : aucun chiffre du README ne peut en venir.
"""

from __future__ import annotations

import json
import re

from agent.llm import FakeLLM

RULES = [
    (r"AirflowTaskTimeout|Timeout|timed out|statement timeout", "delai_depasse"),
    (r"\b40[13]\b|permission denied|PermissionError|not permitted to log in", "droits_insuffisants"),
    (r"\b50[23]\b|ConnectionError|could not translate host|Connection refused|not currently accepting|reset by peer|RemoteDisconnected", "dependance_indisponible"),
    (r"NameError|AttributeError|not all arguments converted|invalid literal for int", "erreur_code"),
    (r"FileNotFoundError|vide|sans aucune commande|Montant manquant|Aucun taux", "donnee_manquante"),
    (r"KeyError|does not match format|string indices|UndefinedColumn|does not exist", "schema_modifie"),
]


def _error_line(user: str) -> tuple[int, str]:
    lines = re.findall(r"^L(\d+): (.*)$", user, re.M)
    for n, text in reversed(lines):
        if re.search(r"Error|Exception|Timeout|denied|FATAL", text):
            return int(n), text
    n, text = lines[-1]
    return int(n), text


def heuristic_responder(messages: list[dict]) -> str:
    system, user = messages[0]["content"], messages[1]["content"]
    if "évaluateur" in system:  # juge
        return json.dumps({"note_diagnostic": 1, "note_correction": 1, "justification": "note factice de test"})
    n, text = _error_line(user)
    if "CLASSER" in system:
        cat = next((c for pat, c in RULES if re.search(pat, text, re.I)), "indetermine")
        return json.dumps({"categorie": cat, "confiance": 0.8 if cat != "indetermine" else 0.3,
                           "justification": f"Mot-clé repéré dans la ligne {n}."})
    if "DIAGNOSTIQUER" in system:
        return json.dumps({"cause": f"Erreur repérée dans le journal : {text[:200]}",
                           "citations": [{"ligne": n, "extrait": text[:80]}],
                           "explication": "Réponse heuristique de test (faux modèle)."})
    if "PROPOSER" in system:
        touche = bool(re.search(r"FileNotFoundError|vide|sans aucune|Montant manquant|UndefinedColumn", text))
        return json.dumps({"resume": "Correction heuristique de test (faux modèle).", "nature": "infrastructure",
                           "touche_donnees": touche, "diff": None, "etapes": ["Analyser l'erreur citée."]})
    raise ValueError("prompt inconnu du faux modèle")


def fake(alias: str, on_request=None) -> FakeLLM:
    return FakeLLM(f"faux-{alias}", heuristic_responder, on_request=on_request)
