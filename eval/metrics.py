"""Métriques d'une campagne (calcul pur, sans dépendance : testable et vérifiable à la main).

Une ligne de résultat par scénario. Une exécution de l'agent qui plante compte
comme une erreur (catégorie fausse, escalade manquée) : on ne l'exclut pas,
sinon un modèle qui plante sur les cas difficiles paraîtrait meilleur.
"""

from __future__ import annotations

import statistics

CLASSES = ["donnee_manquante", "schema_modifie", "delai_depasse", "erreur_code",
           "dependance_indisponible", "droits_insuffisants"]
PRED_CLASSES = CLASSES + ["indetermine", "echec_agent"]


def _mean(xs):
    xs = [x for x in xs if x is not None]
    return round(statistics.fmean(xs), 4) if xs else None


def _pct(xs, q):
    xs = sorted(x for x in xs if x is not None)
    if not xs:
        return None
    k = (len(xs) - 1) * q
    lo, hi = int(k), min(int(k) + 1, len(xs) - 1)
    return round(xs[lo] + (xs[hi] - xs[lo]) * (k - lo), 3)


def confusion(results: list[dict]) -> dict[str, dict[str, int]]:
    m = {t: {p: 0 for p in PRED_CLASSES} for t in CLASSES}
    for r in results:
        m[r["categorie_attendue"]][r.get("categorie_predite") or "echec_agent"] += 1
    return m


def macro_f1(results: list[dict]) -> float:
    f1s = []
    for c in CLASSES:
        tp = sum(r["categorie_attendue"] == c and r.get("categorie_predite") == c for r in results)
        fp = sum(r["categorie_attendue"] != c and r.get("categorie_predite") == c for r in results)
        fn = sum(r["categorie_attendue"] == c and r.get("categorie_predite") != c for r in results)
        p = tp / (tp + fp) if tp + fp else 0.0
        rc = tp / (tp + fn) if tp + fn else 0.0
        f1s.append(2 * p * rc / (p + rc) if p + rc else 0.0)
    return round(statistics.fmean(f1s), 4)


def compute(results: list[dict]) -> dict[str, float | int | None]:
    n = len(results)
    if n == 0:
        return {"n": 0}
    ok = [r for r in results if not r.get("erreur_agent")]
    esc_true = [bool(r["escalade_attendue"]) for r in results]
    esc_pred = [bool(r.get("escalade_predite")) if not r.get("erreur_agent") else False for r in results]
    tp = sum(t and p for t, p in zip(esc_true, esc_pred))
    fp = sum((not t) and p for t, p in zip(esc_true, esc_pred))
    fn = sum(t and (not p) for t, p in zip(esc_true, esc_pred))
    diffs = [r["diff_valide"] for r in ok if r.get("diff_valide") is not None]
    costs = [r.get("cout_usd") for r in ok]
    durations = [r.get("duree_s") for r in ok]
    m = {
        "n": n,
        "echecs_agent": n - len(ok),
        "exactitude": round(sum(r.get("categorie_predite") == r["categorie_attendue"] for r in results) / n, 4),
        "f1_macro": macro_f1(results),
        "tache_correcte": round(sum(r.get("tache_diagnostiquee") == r["tache_attendue"] for r in results) / n, 4),
        "escalades_correctes": round(sum(t == p for t, p in zip(esc_true, esc_pred)) / n, 4),
        "escalade_precision": round(tp / (tp + fp), 4) if tp + fp else None,
        "escalade_rappel": round(tp / (tp + fn), 4) if tp + fn else None,
        "citations_valides": round(sum(bool(r.get("citations_valides")) for r in results) / n, 4),
        "diff_valide": round(sum(diffs) / len(diffs), 4) if diffs else None,
        "nb_diffs": len(diffs),
        "relances_moy": _mean([r.get("relances") for r in ok]),
        "confiance_moy": _mean([r.get("confiance") for r in ok]),
        "cout_total_usd": round(sum(c for c in costs if c is not None), 6),
        "cout_par_diagnostic_usd": _mean(costs),
        "couts_inconnus": sum(c is None for c in costs),
        "duree_moy_s": _mean(durations),
        "duree_p50_s": _pct(durations, 0.5),
        "duree_p95_s": _pct(durations, 0.95),
        "taux_passage_gros": _mean([1.0 if r.get("passe_au_gros") else 0.0 for r in ok]) if any(
            "passe_au_gros" in r for r in ok) else None,
        "fuites_secrets": sum(r.get("fuites", 0) for r in results),
    }
    judged = [r for r in results if r.get("note_diagnostic") is not None]
    if judged:
        m["justesse_diagnostic"] = round(statistics.fmean(r["note_diagnostic"] for r in judged) / 2, 4)
        m["justesse_correction"] = round(statistics.fmean(r["note_correction"] for r in judged) / 2, 4)
        m["nb_juges"] = len(judged)
    m["erreurs_juge"] = sum(1 for r in results if r.get("erreur_juge"))
    return m


def confusion_markdown(results: list[dict]) -> str:
    m = confusion(results)
    used = [p for p in PRED_CLASSES if any(m[t][p] for t in CLASSES) or p in CLASSES]
    head = "| attendu \\ prédit | " + " | ".join(used) + " |"
    sep = "|" + "---|" * (len(used) + 1)
    rows = [f"| {t} | " + " | ".join(str(m[t][p]) for p in used) + " |" for t in CLASSES]
    return "\n".join([head, sep, *rows])
