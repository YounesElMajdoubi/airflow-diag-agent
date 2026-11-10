from collections import Counter
from datetime import date
from pathlib import Path

from agent.escalation import decide
from agent.schemas import Categorie, Classification, Correction
from faults.catalog import CATEGORIES, load_scenarios, orders_csv

DAG = (Path(__file__).resolve().parent.parent / "airflow" / "dags" / "cobaye_ventes.py").read_text()


def test_thirty_scenarios_five_per_category():
    s = load_scenarios()
    assert len(s) == 30
    assert Counter(x.categorie for x in s) == {c: 5 for c in CATEGORIES}
    assert {x.categorie for x in s if x.ci} == set(CATEGORIES)


def test_code_patches_match_the_dag_exactly_once():
    for s in load_scenarios():
        for action in s.injection:
            for p in action.get("patch_dag", []):
                assert DAG.count(p["find"]) == 1, (s.id, p["find"])


def test_dag_source_does_not_give_hints_to_the_agent():
    # le source est montré à l'agent : ni interrupteur de panne, ni mention du banc de test
    import re
    for word in ("scenario", "fault", "panne", "chaos", "canari", "canary", "volontaire", "cobaye", "inject"):
        assert not re.search(rf"\b{word}", DAG.lower().replace("cobaye_ventes", "")), word


def test_expected_escalation_policy():
    s = {x.id: x for x in load_scenarios()}
    assert s["D01"].escalade_attendue and s["U01"].escalade_attendue and s["P03"].escalade_attendue
    assert s["S05"].escalade_attendue and s["T05"].escalade_attendue
    assert not s["C01"].escalade_attendue and not s["T01"].escalade_attendue and not s["S01"].escalade_attendue
    assert sum(x.escalade_attendue for x in s.values()) == 17


def test_agent_rule_matches_ground_truth_when_inputs_are_right():
    for sc in load_scenarios():
        c = Classification(categorie=Categorie(sc.categorie), confiance=0.9, justification="justification test")
        corr = Correction(resume="résumé de test", nature="code", touche_donnees=sc.touche_donnees, etapes=["x"])
        assert decide(c, corr, sc.recidive, True, []).escalader == sc.escalade_attendue, sc.id


def test_low_confidence_or_unverified_escalates():
    c = Classification(categorie=Categorie.erreur_code, confiance=0.4, justification="justification test")
    assert decide(c, None, 1, True, []).escalader
    c.confiance = 0.9
    assert decide(c, None, 1, False, []).escalader
    assert not decide(c, None, 1, True, []).escalader


def test_orders_variants():
    d = date(2026, 3, 1)
    assert orders_csv(d, "missing") is None
    assert orders_csv(d, "empty") == b""
    assert orders_csv(d, "header_only").decode().strip() == "order_id,order_date,currency,amount"
    normal = orders_csv(d).decode().splitlines()
    assert len(normal) == 21 and all("." in ln.split(",")[3] for ln in normal[1:])
    assert orders_csv(d) == orders_csv(d)  # déterministe
    assert "montant" in orders_csv(d, "rename_amount").decode().splitlines()[0]
    assert "01/03/2026" in orders_csv(d, "date_format").decode()
    assert ",," not in orders_csv(d).decode() and "EUR,\n" in orders_csv(d, "null_amount").decode()
