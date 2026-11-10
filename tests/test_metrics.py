from eval.manual_check import weighted_kappa
from eval.metrics import compute, confusion


def _r(sid, true, pred, esc_t, esc_p, cost=0.01, **kw):
    return {"scenario": sid, "categorie_attendue": true, "categorie_predite": pred, "tache_attendue": "t",
            "tache_diagnostiquee": "t", "escalade_attendue": esc_t, "escalade_predite": esc_p,
            "citations_valides": True, "cout_usd": cost, "duree_s": 2.0, "relances": 0, **kw}


def test_compute_counts_agent_crash_as_error():
    res = [
        _r("a", "erreur_code", "erreur_code", False, False),
        _r("b", "droits_insuffisants", "droits_insuffisants", True, True),
        _r("c", "delai_depasse", "erreur_code", False, True),
        {"scenario": "d", "categorie_attendue": "donnee_manquante", "categorie_predite": None, "tache_attendue": "t",
         "escalade_attendue": True, "erreur_agent": "boom", "duree_s": 1.0},
    ]
    m = compute(res)
    assert m["n"] == 4 and m["echecs_agent"] == 1
    assert m["exactitude"] == 0.5
    assert m["escalades_correctes"] == 0.5          # a, b justes ; c faux positif ; d manqué
    assert m["escalade_precision"] == 0.5 and m["escalade_rappel"] == 0.5
    assert m["cout_par_diagnostic_usd"] == 0.01
    assert confusion(res)["donnee_manquante"]["echec_agent"] == 1


def test_weighted_kappa():
    assert weighted_kappa([0, 1, 2, 2], [0, 1, 2, 2]) == 1.0
    assert weighted_kappa([0, 2], [2, 0]) < 0
    assert weighted_kappa([], []) is None
