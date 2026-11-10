"""Décision d'escalade : une RÈGLE explicite, pas un jugement libre du modèle.

Le modèle fournit des éléments (catégorie, confiance, « la correction touche des
données ? ») ; le code décide. Avantages : la décision est explicable, testable,
et identique quel que soit le modèle. La partie « vérité terrain » de la règle est
partagée avec faults/catalog.py (agent/policy.py), pour que l'évaluation soit juste.

Escalade vers un humain si au moins une raison :
  - catégorie dependance_indisponible ou droits_insuffisants (correction hors du DAG) ;
  - la correction touche des données ;
  - la panne se répète (≥ 3 échecs consécutifs de la tâche) ;
  - l'agent n'est pas sûr : catégorie « indetermine », confiance < seuil,
    citations invalides, ou une étape n'a pas produit de sortie valide.
"""

from __future__ import annotations

from agent.policy import escalation_reasons
from agent.schemas import Categorie, Classification, Correction, Escalade

CONFIDENCE_THRESHOLD = 0.6


def decide(classification: Classification, correction: Correction | None, recidive: int,
           citations_valides: bool, etapes_ratees: list[str],
           seuil_confiance: float = CONFIDENCE_THRESHOLD) -> Escalade:
    cat = classification.categorie
    raisons = escalation_reasons(
        cat.value if cat != Categorie.indetermine else "indetermine",
        bool(correction and correction.touche_donnees),
        recidive,
    )
    if cat == Categorie.indetermine:
        raisons.append("catégorie indéterminée")
    if classification.confiance < seuil_confiance:
        raisons.append(f"confiance faible ({classification.confiance:.2f} < {seuil_confiance})")
    if not citations_valides:
        raisons.append("diagnostic non étayé par des citations valides du journal")
    for etape in etapes_ratees:
        raisons.append(f"sortie invalide à l'étape {etape}")
    return Escalade(escalader=bool(raisons), raisons=raisons)
