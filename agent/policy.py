"""Politique d'escalade commune à l'agent et à la vérité terrain (faults/catalog.py).

Une seule définition : si l'agent se trompe d'escalade, c'est qu'il s'est trompé
de catégorie, de « touche des données » ou de récidive, jamais de règle.
"""

ALWAYS_ESCALATE = {"dependance_indisponible", "droits_insuffisants"}  # correction hors du code du DAG
RECURRENCE_THRESHOLD = 3


def escalation_reasons(categorie: str, touche_donnees: bool, recidive: int) -> list[str]:
    reasons = []
    if categorie in ALWAYS_ESCALATE:
        reasons.append(f"catégorie {categorie} : correction hors du code du DAG")
    if touche_donnees:
        reasons.append("la correction touche des données")
    if recidive >= RECURRENCE_THRESHOLD:
        reasons.append(f"panne répétée ({recidive} échecs consécutifs)")
    return reasons
