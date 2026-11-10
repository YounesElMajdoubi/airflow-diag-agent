"""Injecteur de pannes : provoque chaque scénario, capture ce que l'agent verra, répare.

Pour chaque scénario :
  1. remise à zéro de l'environnement (API normale, droits, colonnes, services, DAG d'origine…) ;
  2. une exécution SAINE, qui doit réussir (sinon la remise à zéro est incomplète : on s'arrête) ;
     elle donne aussi un historique propre : l'échec suivant n'est pas compté comme une récidive ;
  3. injection de la panne, puis 1 exécution (ou `recidive` exécutions) qui doivent échouer ;
  4. vérification : la tâche en échec est-elle celle attendue ? (sinon scénario marqué invalide) ;
  5. capture d'un instantané : toutes les réponses BRUTES d'Airflow dont le serveur MCP a besoin
     pour ce run (journaux de chaque tentative, source, historiques) -> faults/snapshots/<id>.json ;
  6. le manifeste faults/manifest.json relie chaque scénario à son run réel.

L'instantané ne contient AUCUNE étiquette : la vérité terrain reste dans scenarios.yaml.

Usage :
    python -m faults.inject --env docker --all
    python -m faults.inject --env docker --only D01,U03
    python -m faults.inject --env local --only D01        # mode développement sans Docker
"""

from __future__ import annotations

import argparse
import asyncio
import json
import os
import secrets
import subprocess
import sys
import time
from datetime import date, datetime, timedelta, timezone
from pathlib import Path

import httpx
from mcp import Client

from faults.catalog import Scenario, load_scenarios, orders_csv
from faults.env import DockerEnv, Env, LocalEnv, wait_http
from mcp_server.airflow_api import LiveAirflow, RecordingAirflow
from mcp_server.server import create_server

ROOT = Path(__file__).resolve().parent.parent
DAG_ID = "cobaye_ventes"
TASKS = ["extract_orders", "fetch_rates", "transform", "load_warehouse", "publish_report"]
SNAP_DIR = ROOT / "faults" / "snapshots"
MANIFEST = ROOT / "faults" / "manifest.json"
STATE_DIR = ROOT / "faults" / ".state"
RESET_SQL = (ROOT / "services" / "entrepot" / "reset.sql").read_text()


def log(msg: str) -> None:
    print(f"[{datetime.now():%H:%M:%S}] {msg}", flush=True)


class AirflowAdmin:
    """Client Airflow AVEC droits d'écriture : réservé à l'injecteur (jamais à l'agent)."""

    def __init__(self, url: str, user: str, password: str):
        self.http = httpx.Client(base_url=url.rstrip("/"), timeout=60)
        tok = self.http.post("/auth/token", json={"username": user, "password": password})
        tok.raise_for_status()
        self.http.headers["Authorization"] = f"Bearer {tok.json()['access_token']}"

    def _get(self, path: str, **kw) -> httpx.Response:
        """GET tolérant : une erreur réseau passagère (machine chargée) ne doit pas faire échouer la campagne."""
        for attempt in range(5):
            try:
                return self._ok(self.http.get(path, **kw))
            except httpx.TransportError as e:
                if attempt == 4:
                    raise
                log(f"  (Airflow lent : {type(e).__name__}, nouvel essai dans {5 * (attempt + 1)} s)")
                time.sleep(5 * (attempt + 1))

    def _ok(self, r: httpx.Response) -> httpx.Response:
        if r.status_code >= 400:
            raise RuntimeError(f"Airflow {r.request.method} {r.request.url.path} -> {r.status_code} {r.text[:300]}")
        return r

    def version(self) -> str:
        return self._get("/api/v2/version").json().get("version", "?")

    def wait_dag(self, timeout: float = 180) -> None:
        deadline = time.time() + timeout
        while time.time() < deadline:
            r = self.http.get(f"/api/v2/dags/{DAG_ID}")
            if r.status_code == 200:
                if r.json().get("is_paused"):
                    self._ok(self.http.patch(f"/api/v2/dags/{DAG_ID}", json={"is_paused": False}))
                return
            time.sleep(2)
        raise RuntimeError(f"Le DAG {DAG_ID} n'apparaît pas dans Airflow (erreur d'import ?)")

    def last_logical_date(self) -> date | None:
        r = self._get(f"/api/v2/dags/{DAG_ID}/dagRuns", params={"order_by": "-logical_date", "limit": 1})
        runs = r.json()["dag_runs"]
        if not runs or not runs[0].get("logical_date"):
            return None
        return datetime.fromisoformat(runs[0]["logical_date"].replace("Z", "+00:00")).date()

    def source(self) -> str:
        return self._get(f"/api/v2/dagSources/{DAG_ID}").json()["content"]

    def wait_source(self, expected: str, timeout: float = 120) -> None:
        deadline = time.time() + timeout
        while time.time() < deadline:
            if self.source() == expected:
                return
            time.sleep(2)
        raise RuntimeError("Airflow n'a pas repris la nouvelle version du DAG à temps")

    def set_variable(self, key: str, value: str) -> None:
        r = self.http.patch(f"/api/v2/variables/{key}", json={"key": key, "value": value})
        if r.status_code == 404:
            r = self.http.post("/api/v2/variables", json={"key": key, "value": value})
        self._ok(r)

    def delete_variable(self, key: str) -> None:
        r = self.http.delete(f"/api/v2/variables/{key}")
        if r.status_code not in (204, 404):
            self._ok(r)

    def trigger(self, run_id: str, ds: date) -> None:
        body = {"dag_run_id": run_id, "logical_date": f"{ds.isoformat()}T06:00:00Z"}
        self._ok(self.http.post(f"/api/v2/dags/{DAG_ID}/dagRuns", json=body))

    def wait_run(self, run_id: str, timeout: float = 900) -> str:
        deadline = time.time() + timeout
        while time.time() < deadline:
            state = self._get(f"/api/v2/dags/{DAG_ID}/dagRuns/{run_id}").json()["state"]
            if state in ("success", "failed"):
                return state
            time.sleep(3)
        raise RuntimeError(f"Le run {run_id} n'est pas terminé après {timeout:.0f} s")

    def task_states(self, run_id: str) -> dict[str, str]:
        tis = self._get(f"/api/v2/dags/{DAG_ID}/dagRuns/{run_id}/taskInstances").json()["task_instances"]
        return {t["task_id"]: t["state"] for t in tis}

    def first_failed(self, run_id: str) -> str | None:
        tis = self._get(f"/api/v2/dags/{DAG_ID}/dagRuns/{run_id}/taskInstances").json()["task_instances"]
        failed = sorted((t for t in tis if t["state"] == "failed"), key=lambda t: t.get("end_date") or "")
        return failed[0]["task_id"] if failed else None


class Injector:
    def __init__(self, env: Env, admin: AirflowAdmin, fournisseur_url: str, reader: tuple[str, str, str]):
        self.env, self.admin = env, admin
        self.fournisseur_url = fournisseur_url
        self.reader = reader  # (url, user, password) du compte en lecture seule utilisé par le serveur MCP
        self.session = secrets.token_hex(2)
        STATE_DIR.mkdir(parents=True, exist_ok=True)
        # Sauvegarde du DAG d'origine, présente UNIQUEMENT pendant une panne « code ».
        # Si elle existe au démarrage, c'est qu'un arrêt brutal a laissé le DAG modifié : on le restaure.
        self.pristine_file = STATE_DIR / "cobaye_ventes.py.orig"
        if self.pristine_file.exists():
            env.dag_file.write_text(self.pristine_file.read_text())
            self.pristine_file.unlink()
        self.pristine = env.dag_file.read_text()
        last = admin.last_logical_date()
        self.next_ds = (last + timedelta(days=1)) if last else date(2026, 3, 1)
        self.lock_procs: list[subprocess.Popen] = []

    # ------------------------------------------------------------ outils simples
    def _api_mode(self, mode: str, params: dict | None = None) -> None:
        httpx.post(f"{self.fournisseur_url}/admin/mode", json={"mode": mode, "params": params or {}}, timeout=10).raise_for_status()

    def _new_ds(self) -> date:
        ds, self.next_ds = self.next_ds, self.next_ds + timedelta(days=1)
        return ds

    def _run_id(self, ds: date) -> str:
        # Identifiant neutre, comme un déclenchement manuel : il ne trahit pas le scénario.
        return f"manual__{ds.isoformat()}T06:00:00+00:00_{self.session}"

    # -------------------------------------------------------------- remise à zéro
    def reset(self) -> None:
        for p in self.lock_procs:
            p.kill()
        self.lock_procs.clear()
        self.env.start("entrepot")
        self.env.start("fournisseur")
        self._api_mode("normal")
        self.env.sql("ALTER DATABASE dwh ALLOW_CONNECTIONS true;", db="postgres")
        self.env.sql(RESET_SQL)
        self.admin.delete_variable("fournisseur_timeout_s")
        self.env.chmod_data("reports", "775")
        if self.env.dag_file.read_text() != self.pristine:
            self.env.dag_file.write_text(self.pristine)
        self.pristine_file.unlink(missing_ok=True)
        if self.admin.source() != self.pristine:
            self.admin.wait_source(self.pristine)

    # ------------------------------------------------------------------ panne
    def inject(self, s: Scenario, ds: date) -> None:
        variant = next((a["orders"] for a in s.injection if "orders" in a), None)
        self._write_orders(ds, variant)
        for a in s.injection:
            if "api" in a:
                self._api_mode(a["api"], a.get("params"))
            elif "sql" in a:
                self.env.sql(a["sql"] + ";", db=a.get("sql_db", "dwh"))
                if a.get("sql_db") == "postgres" and "ALLOW_CONNECTIONS false" in a["sql"]:
                    self.env.sql("SELECT pg_terminate_backend(pid) FROM pg_stat_activity WHERE datname = 'dwh';", db="postgres")
            elif "sql_lock" in a:
                sql = ("SET application_name = 'chaos-lock'; BEGIN; "
                       "LOCK TABLE ventes.fact_sales IN ACCESS EXCLUSIVE MODE; "
                       f"SELECT pg_sleep({int(a['sql_lock'])}); COMMIT;")
                self.lock_procs.append(self.env.sql_background(sql))
                time.sleep(3)
            elif "stop" in a:
                self.env.stop(a["stop"])
            elif "variable" in a:
                for k, v in a["variable"].items():
                    self.admin.set_variable(k, str(v))
            elif "chmod_reports" in a:
                self.env.chmod_data("reports", a["chmod_reports"])
            elif "patch_dag" in a:
                text = self.pristine
                for p in a["patch_dag"]:
                    if text.count(p["find"]) != 1:
                        raise RuntimeError(f"{s.id} : motif à remplacer introuvable ou ambigu : {p['find']!r}")
                    text = text.replace(p["find"], p["replace"])
                self.pristine_file.write_text(self.pristine)
                self.env.dag_file.write_text(text)
                self.admin.wait_source(text)
            # "orders" déjà traité

    def _write_orders(self, ds: date, variant: str | None) -> None:
        rel = f"incoming/orders_{ds.isoformat()}.csv"
        content = orders_csv(ds, variant)
        if content is None:
            self.env.delete_data(rel)
        else:
            self.env.write_data(rel, content)

    # -------------------------------------------------------------- scénario
    def healthy_run(self) -> str:
        ds = self._new_ds()
        self._write_orders(ds, None)
        run_id = self._run_id(ds)
        self.admin.trigger(run_id, ds)
        state = self.admin.wait_run(run_id)
        if state != "success":
            raise RuntimeError(f"Exécution saine {run_id} en échec ({self.admin.task_states(run_id)}) : "
                               "l'environnement n'est pas revenu à l'état nominal.")
        return run_id

    def run_scenario(self, s: Scenario, capture: bool = True) -> dict:
        t0 = time.time()
        log(f"{s.id} — {s.titre}")
        self.reset()
        healthy = self.healthy_run()
        log(f"  exécution saine OK ({healthy})")
        runs = []
        for k in range(s.recidive):
            ds = self._new_ds()
            self.inject(s, ds)
            run_id = self._run_id(ds)
            self.admin.trigger(run_id, ds)
            state = self.admin.wait_run(run_id)
            runs.append({"run_id": run_id, "date_logique": ds.isoformat(), "etat": state})
            log(f"  run {k + 1}/{s.recidive} : {state} ({run_id})")
        target = runs[-1]["run_id"]
        observed = self.admin.first_failed(target)
        valid = runs[-1]["etat"] == "failed" and observed == s.tache_attendue
        if not valid:
            log(f"  ATTENTION : attendu échec de {s.tache_attendue}, observé {observed} ({runs[-1]['etat']})")
        snap_file = SNAP_DIR / f"{s.id}.json"
        if capture:
            asyncio.run(self.capture(s, target, snap_file))
            log(f"  instantané : {snap_file.relative_to(ROOT)} ({time.time() - t0:.0f} s)")
        return {
            "run_id": target, "date_logique": runs[-1]["date_logique"], "runs": runs,
            "run_sain_precedent": healthy, "tache_observee": observed, "valide": valid,
            "instantane": str(snap_file.relative_to(ROOT)), "duree_injection_s": round(time.time() - t0, 1),
        }

    async def capture(self, s: Scenario, run_id: str, path: Path) -> None:
        """Rejoue tous les appels utiles via le serveur MCP, en enregistrant les réponses brutes."""
        url, user, pwd = self.reader
        rec = RecordingAirflow(LiveAirflow(url, user, pwd))
        async with Client(create_server(rec)) as c:
            await c.call_tool("list_failed_runs", {"dag_id": DAG_ID, "since": "3650d", "limit": 50})
            for ti in rec.list_task_instances(DAG_ID, run_id):
                for t in range(1, (ti.get("try_number") or 0) + 1):
                    await c.call_tool("get_task_logs", {"dag_id": DAG_ID, "run_id": run_id, "task_id": ti["task_id"], "try_number": t})
            await c.call_tool("get_dag_source", {"dag_id": DAG_ID, "run_id": run_id})
            await c.call_tool("get_dag_source", {"dag_id": DAG_ID})
            for task in TASKS:
                await c.call_tool("get_task_history", {"dag_id": DAG_ID, "task_id": task})
        # Monde isolé : dans le rejeu, seul le run de ce scénario apparaît comme « en échec ».
        key = f"list_dag_runs|{DAG_ID}|failed"
        rec.calls[key] = [r for r in rec.calls.get(key, []) if r["dag_run_id"] == run_id]
        # Les autres runs en échec ne sont pas visibles dans ce monde : leurs détails sont inutiles.
        for k in [k for k in rec.calls if k.startswith(("list_task_instances|", "get_dag_run|"))]:
            if not k.endswith(f"|{run_id}"):
                del rec.calls[k]
        snap = rec.snapshot({"dag_id": DAG_ID, "run_id": run_id, "airflow_version": self.admin.version(),
                             "format": 1})
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(json.dumps(snap, ensure_ascii=False, separators=(",", ":")), encoding="utf-8")


def canaries_from_env(previous: list[str]) -> list[str]:
    """Valeurs canari plantées dans le pipeline (fausses, par construction) : servent à mesurer les fuites."""
    values = set(previous)
    for var in ("ENTREPOT_PASSWORD", "FOURNISSEUR_API_KEY"):
        if os.environ.get(var):
            values.add(os.environ[var])
    dsn = os.environ.get("ENTREPOT_DSN", "")
    if "://" in dsn and "@" in dsn:
        values.add(dsn.split("://", 1)[1].split("@", 1)[0].split(":", 1)[-1])
    return sorted(values)


def build_env(kind: str) -> tuple[Env, str]:
    fournisseur = os.environ.get("FOURNISSEUR_ADMIN_URL", "http://localhost:8000")
    if kind == "docker":
        return DockerEnv(ROOT, fournisseur), fournisseur
    return LocalEnv(
        ROOT,
        data_dir=Path(os.environ["PIPELINE_DATA_DIR"]),
        pg_port=int(os.environ.get("ENTREPOT_PORT", "55432")),
        pg_data=Path(os.environ["LOCAL_PGDATA"]),
        pg_os_user=os.environ.get("LOCAL_PG_OS_USER"),
        fournisseur_url=fournisseur,
        state_dir=STATE_DIR,
    ), fournisseur


def main() -> None:
    p = argparse.ArgumentParser(description="Provoque les pannes étiquetées et capture les instantanés")
    p.add_argument("--env", choices=["docker", "local"], default="docker")
    g = p.add_mutually_exclusive_group(required=True)
    g.add_argument("--all", action="store_true")
    g.add_argument("--only", help="liste d'identifiants séparés par des virgules (ex. D01,U03)")
    g.add_argument("--reset", action="store_true", help="remet seulement l'environnement à l'état sain")
    p.add_argument("--keep", action="store_true", help="laisse la dernière panne en place (démo)")
    p.add_argument("--no-capture", action="store_true", help="ne capture pas d'instantané (démo)")
    args = p.parse_args()

    scenarios = load_scenarios()
    if args.only:
        wanted = set(args.only.split(","))
        unknown = wanted - {s.id for s in scenarios}
        if unknown:
            sys.exit(f"Scénarios inconnus : {sorted(unknown)}")
        scenarios = [s for s in scenarios if s.id in wanted]

    env, fournisseur = build_env(args.env)
    af_url = os.environ.get("AIRFLOW_API_URL", "http://localhost:8080")
    wait_http(f"{af_url}/api/v2/monitor/health", timeout=180)
    admin = AirflowAdmin(af_url, os.environ.get("AIRFLOW_ADMIN_USERNAME", "airflow"),
                         os.environ.get("AIRFLOW_ADMIN_PASSWORD", "airflow"))
    admin.wait_dag()
    reader = (af_url, os.environ.get("AIRFLOW_USERNAME", "diag_reader"),
              os.environ.get("AIRFLOW_PASSWORD") or os.environ.get("AIRFLOW_READER_PASSWORD", ""))
    inj = Injector(env, admin, fournisseur, reader)
    if args.reset:
        inj.reset()
        log("Environnement remis à l'état sain")
        return

    manifest = json.loads(MANIFEST.read_text()) if MANIFEST.exists() else {"scenarios": {}}
    manifest.update({"airflow_version": admin.version(), "environnement": args.env,
                     "canaris": canaries_from_env(manifest.get("canaris", []))})
    try:
        for s in scenarios:
            entry = inj.run_scenario(s, capture=not args.no_capture)
            if args.no_capture:
                log(f"  panne en place : run {entry['run_id']} (tâche {entry['tache_observee']})")
                continue
            manifest["scenarios"][s.id] = entry
            manifest["mis_a_jour_le"] = datetime.now(timezone.utc).isoformat(timespec="seconds")
            MANIFEST.write_text(json.dumps(manifest, ensure_ascii=False, indent=2, sort_keys=True))
    finally:
        if args.keep:
            log("--keep : la panne reste en place (make demo, puis `python -m faults.inject --reset` pour réparer)")
        else:
            log("Remise à zéro finale")
            inj.reset()
    invalid = [k for k, v in manifest["scenarios"].items() if not v["valide"]]
    log(f"Terminé : {len(manifest['scenarios'])} scénarios dans le manifeste, invalides : {invalid or 'aucun'}")


if __name__ == "__main__":
    main()
