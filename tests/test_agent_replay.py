"""Tests de bout en bout SANS réseau : vrais instantanés Airflow (capturés par faults/inject.py)
rejoués par le vrai serveur MCP (stdio), agent LangGraph complet, faux modèles scriptés."""

import asyncio
import json
import re
from pathlib import Path

import pytest

from agent.graph import Deps, diagnose
from agent.llm import FakeLLM
from agent.mcp_tools import connect
from eval.leaks import LeakDetector

SNAP = Path(__file__).parent / "fixtures" / "snapshots"
CANARIES = ["fk_live_9c2e5a7f1b3d4e6a8c0b2d4f6a8e0c1d", "cnry-pg-3f9a1c7e2b", "cnry-src-5b1e0c9d7a"]


def run(coro):
    return asyncio.run(coro)


def _error_line(user):
    for n, text in reversed(re.findall(r"^L(\d+): (.*)$", user, re.M)):
        if "Error" in text:
            return int(n), text
    raise AssertionError("pas de ligne d'erreur dans le prompt")


def scripted(categorie, confiance=0.9, bad_first=False, touche=False):
    state = {"calls": 0}

    def responder(messages):
        state["calls"] += 1
        system, user = messages[0]["content"], messages[1]["content"]
        if bad_first and len(messages) == 2 and "CLASSER" in system:
            return "désolé, voici ma réponse : catégorie = erreur"   # JSON invalide -> relance attendue
        if "CLASSER" in system:
            return json.dumps({"categorie": categorie, "confiance": confiance, "justification": "indices du journal"})
        if "DIAGNOSTIQUER" in system:
            n, text = _error_line(user)
            return json.dumps({"cause": "cause identifiée dans le journal", "citations": [{"ligne": n, "extrait": text[:50]}],
                               "explication": "explication de test"})
        return json.dumps({"resume": "correction de test", "nature": "acces", "touche_donnees": touche,
                           "diff": None, "etapes": ["étape"]})
    return responder


def test_replay_tools_never_leak_canaries():
    async def go():
        leak = LeakDetector(CANARIES)
        for snap in sorted(SNAP.glob("*.json")):
            async with connect(snapshot=str(snap)) as tools:
                runs = await tools.call("list_failed_runs", {"dag_id": "cobaye_ventes", "since": "3650d"})
                run = runs["executions"][0]
                task = run["taches_en_echec"][0]["task_id"]
                outs = [runs,
                        await tools.call("get_task_logs", {"dag_id": "cobaye_ventes", "run_id": run["run_id"], "task_id": task}),
                        await tools.call("get_dag_source", {"dag_id": "cobaye_ventes", "run_id": run["run_id"]}),
                        await tools.call("get_task_history", {"dag_id": "cobaye_ventes", "task_id": task})]
                leak.check("outils", [{"content": json.dumps(o)} for o in outs])
        return leak.leaks
    assert run(go()) == []


def test_snapshots_contain_raw_canaries():
    """Sinon le test précédent ne prouverait rien."""
    raw = "".join(p.read_text() for p in SNAP.glob("*.json"))
    assert any(c in raw for c in CANARIES)


def test_full_agent_with_retry_on_invalid_json():
    async def go():
        async with connect(snapshot=str(SNAP / "U01.json")) as tools:
            llm = FakeLLM("faux", scripted("dependance_indisponible", bad_first=True))
            return await diagnose(Deps(tools=tools, strategie="gros", gros=llm), "cobaye_ventes", since="3650d")
    r = run(go())
    assert r.task_id == "fetch_rates"
    assert r.classification.categorie.value == "dependance_indisponible"
    assert r.relances == 1 and len(r.appels) == 4 and r.appels[0].valide is False
    assert r.citations_valides
    assert r.escalade.escalader and "dependance_indisponible" in r.escalade.raisons[0]


def test_routing_sends_hard_cases_to_big_model():
    async def go():
        async with connect(snapshot=str(SNAP / "C01.json")) as tools:
            petit = FakeLLM("petit", scripted("indetermine", confiance=0.3))
            gros = FakeLLM("gros", scripted("erreur_code", confiance=0.9))
            r = await diagnose(Deps(tools=tools, strategie="routage", petit=petit, gros=gros), "cobaye_ventes", since="3650d")
            return r, petit, gros
    r, petit, gros = run(go())
    assert r.modele_principal == "gros"
    assert len(petit.calls) == 1 and len(gros.calls) == 3   # le gros reclasse puis diagnostique et corrige
    assert r.classification.categorie.value == "erreur_code"
    assert not r.escalade.escalader


def test_routing_keeps_easy_cases_on_small_model():
    async def go():
        async with connect(snapshot=str(SNAP / "D01.json")) as tools:
            petit = FakeLLM("petit", scripted("donnee_manquante", confiance=0.95, touche=True))
            gros = FakeLLM("gros", scripted("donnee_manquante"))
            r = await diagnose(Deps(tools=tools, strategie="routage", petit=petit, gros=gros), "cobaye_ventes", since="3650d")
            return r, gros
    r, gros = run(go())
    assert r.modele_principal == "petit" and gros.calls == []
    assert r.escalade.escalader and "touche des données" in " ".join(r.escalade.raisons)


def test_invented_citation_is_caught():
    def liar(messages):
        system = messages[0]["content"]
        if "CLASSER" in system:
            return json.dumps({"categorie": "erreur_code", "confiance": 0.9, "justification": "indices du journal"})
        if "DIAGNOSTIQUER" in system:
            return json.dumps({"cause": "cause inventée de toutes pièces", "explication": "explication inventée",
                               "citations": [{"ligne": 9999, "extrait": "ligne qui n'existe pas"}]})
        return json.dumps({"resume": "correction de test", "nature": "acces", "touche_donnees": False,
                           "diff": None, "etapes": ["étape"]})

    async def go():
        async with connect(snapshot=str(SNAP / "C01.json")) as tools:
            return await diagnose(Deps(tools=tools, strategie="gros", gros=FakeLLM("x", liar)), "cobaye_ventes", since="3650d")
    r = run(go())
    assert r.citations_valides is False and r.relances == 2
    assert r.escalade.escalader and any("citations" in x for x in r.escalade.raisons)


@pytest.mark.parametrize("snap", sorted(p.stem for p in SNAP.glob("*.json")))
def test_cli_rejoue_chaque_instantane_de_test(snap):
    async def go():
        async with connect(snapshot=str(SNAP / f"{snap}.json")) as tools:
            from eval.fake_models import fake
            return await diagnose(Deps(tools=tools, strategie="petit", petit=fake("p")), "cobaye_ventes", since="3650d")
    r = run(go())
    assert r.diagnostic is not None and r.citations_valides
