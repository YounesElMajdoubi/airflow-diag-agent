"""Serveur MCP « airflow-diag » : 4 outils en LECTURE SEULE sur Airflow.

Construit avec le SDK Python officiel `mcp` (v2). Dans la v1 du SDK, la classe
s'appelait FastMCP ; elle a été renommée MCPServer en v2 (même principe : un
décorateur @tool qui transforme une fonction Python typée en outil MCP, avec
schéma JSON des paramètres généré à partir des annotations).

Garanties :
- aucun outil n'écrit dans Airflow (le client n'émet que des GET, et dans
  docker-compose le serveur utilise un compte Airflow « Viewer ») ;
- tout texte renvoyé (journaux, source du DAG) passe par le masqueur de secrets ;
- les journaux sont tronqués aux lignes utiles, avec les numéros d'origine.

Lancement :
    python -m mcp_server.server                        # stdio, backend live
    python -m mcp_server.server --transport streamable-http --port 8765
    python -m mcp_server.server --backend replay --snapshot faults/snapshots/D01.json
"""

from __future__ import annotations

import argparse
import logging
import os
import re
import statistics
from datetime import datetime, timedelta
from functools import lru_cache
from typing import Any

from mcp.server.mcpserver import MCPServer
from mcp.server.mcpserver.exceptions import ToolError
from mcp.server.transport_security import TransportSecuritySettings
from mcp.types import ToolAnnotations

from mcp_server.airflow_api import AirflowBackend, AirflowError, LiveAirflow, ReplayAirflow, parse_dt
from mcp_server.logs import render, select_useful
from mcp_server.redaction import Redactor

READ_ONLY = ToolAnnotations(readOnlyHint=True, destructiveHint=False, idempotentHint=True, openWorldHint=False)
FAILED_STATES = {"failed"}

INSTRUCTIONS = (
    "Outils de diagnostic Airflow en lecture seule. Démarche conseillée : "
    "list_failed_runs pour trouver l'exécution et la tâche en échec, puis get_task_logs, "
    "get_task_history et get_dag_source. Les secrets sont remplacés par [SECRET_MASQUÉ]. "
    "Les numéros de ligne des journaux sont ceux du journal complet d'origine."
)


def parse_since(since: str | None, now: datetime) -> datetime:
    """'30m', '24h', '7d' ou une date ISO -> datetime (UTC)."""
    if not since:
        return now - timedelta(days=7)
    m = re.fullmatch(r"\s*(\d+)\s*([mhd])\s*", since)
    if m:
        n, unit = int(m.group(1)), m.group(2)
        return now - {"m": timedelta(minutes=n), "h": timedelta(hours=n), "d": timedelta(days=n)}[unit]
    dt = parse_dt(since)
    if dt is None:
        raise ToolError(f"Paramètre since invalide : {since!r} (attendu : 24h, 7d ou date ISO)")
    return dt if dt.tzinfo else dt.replace(tzinfo=now.tzinfo)


def _num(x) -> float | None:
    return round(float(x), 2) if x is not None else None


logging.getLogger("httpx").setLevel(logging.WARNING)


def create_server(backend: AirflowBackend, *, lines_before: int = 15, max_lines: int = 150) -> MCPServer:
    server = MCPServer("airflow-diag", instructions=INSTRUCTIONS)

    def call(fn, *args, **kwargs):
        try:
            return fn(*args, **kwargs)
        except AirflowError as e:
            raise ToolError(str(e)) from e

    @lru_cache(maxsize=32)
    def source_lines(dag_id: str, version: int | None) -> tuple[str, ...]:
        try:
            return tuple(backend.get_dag_source(dag_id, version)["content"].splitlines())
        except AirflowError:
            return ()

    def run_version(dag_id: str, run_id: str) -> int | None:
        run = call(backend.get_dag_run, dag_id, run_id)
        versions = run.get("dag_versions") or []
        return versions[-1]["version_number"] if versions else None

    # ------------------------------------------------------------------ outils

    @server.tool(annotations=READ_ONLY)
    def list_failed_runs(dag_id: str, since: str = "7d", limit: int = 10) -> dict[str, Any]:
        """Liste les exécutions en échec d'un DAG depuis `since` (ex. '24h', '7d' ou date ISO),
        avec pour chacune les tâches en échec (à diagnostiquer) et les tâches bloquées en aval."""
        since_dt = parse_since(since, backend.now())
        runs = call(backend.list_dag_runs, dag_id, state="failed", start_date_gte=since_dt, limit=limit)
        out = []
        for run in runs:
            tis = call(backend.list_task_instances, dag_id, run["dag_run_id"])
            failed = sorted((t for t in tis if t["state"] in FAILED_STATES),
                            key=lambda t: t.get("end_date") or "")
            versions = run.get("dag_versions") or []
            out.append({
                "run_id": run["dag_run_id"],
                "date_logique": run.get("logical_date"),
                "debut": run.get("start_date"),
                "fin": run.get("end_date"),
                "version_dag": versions[-1]["version_number"] if versions else None,
                "taches_en_echec": [{
                    "task_id": t["task_id"],
                    "tentative": t.get("try_number"),
                    "tentatives_max": (t.get("max_tries") or 0) + 1,
                    "debut": t.get("start_date"),
                    "fin": t.get("end_date"),
                    "duree_s": _num(t.get("duration")),
                    "operateur": t.get("operator_name") or t.get("operator"),
                } for t in failed],
                "taches_bloquees": sorted(t["task_id"] for t in tis if t["state"] == "upstream_failed"),
            })
        return {"dag_id": dag_id, "depuis": since_dt.isoformat(), "nombre": len(out), "executions": out}

    @server.tool(annotations=READ_ONLY)
    def get_task_logs(dag_id: str, run_id: str, task_id: str, try_number: int | None = None) -> dict[str, Any]:
        """Journal d'une tentative de tâche (par défaut la dernière), tronqué aux lignes utiles :
        traceback complet, lignes précédant chaque erreur, avertissements, fin du journal.
        Chaque ligne garde son numéro `n` dans le journal complet."""
        tis = call(backend.list_task_instances, dag_id, run_id)
        ti = next((t for t in tis if t["task_id"] == task_id), None)
        if ti is None:
            raise ToolError(f"Tâche {task_id!r} introuvable dans l'exécution {run_id!r}")
        last_try = ti.get("try_number") or 0
        if last_try == 0:
            raise ToolError(f"La tâche {task_id!r} n'a pas été exécutée (état : {ti['state']})")
        tentative = try_number or last_try
        if not 1 <= tentative <= last_try:
            raise ToolError(f"Tentative {tentative} inexistante (tentatives disponibles : 1 à {last_try})")

        content = call(backend.get_task_log, dag_id, run_id, task_id, tentative)
        lines = render(content, list(source_lines(dag_id, run_version(dag_id, run_id))))
        kept, omitted = select_useful(lines, before=lines_before, max_lines=max_lines)
        red = Redactor()
        return {
            "dag_id": dag_id, "run_id": run_id, "task_id": task_id,
            "tentative": tentative, "tentatives_disponibles": list(range(1, last_try + 1)),
            "etat_tache": ti["state"],
            "total_lignes": len(lines),
            "lignes": [{"n": ln.n, "texte": red.redact(ln.text)} for ln in kept],
            "lignes_omises": omitted,
            "secrets_masques": red.total,
        }

    @server.tool(annotations=READ_ONLY)
    def get_dag_source(dag_id: str, run_id: str | None = None) -> dict[str, Any]:
        """Code source du DAG. Avec `run_id`, renvoie la version exacte exécutée par ce run
        (le code a pu changer depuis). Les secrets écrits dans le code sont masqués."""
        version = run_version(dag_id, run_id) if run_id else None
        data = call(backend.get_dag_source, dag_id, version)
        red = Redactor(source_code=True)
        source = red.redact(data["content"])
        return {
            "dag_id": dag_id,
            "version": data.get("version_number"),
            "nb_lignes": len(source.splitlines()),
            "source": source,
            "secrets_masques": red.total,
        }

    @server.tool(annotations=READ_ONLY)
    def get_task_history(dag_id: str, task_id: str, limit: int = 20) -> dict[str, Any]:
        """Historique récent d'une tâche sur toutes les exécutions du DAG (plus récent d'abord),
        avec un résumé : taux d'échec, durées, nombre d'échecs consécutifs récents.
        Utile pour repérer une panne intermittente ou qui se répète."""
        tis = call(backend.list_task_history, dag_id, task_id, limit)
        entries = [{
            "run_id": t["dag_run_id"],
            "date_logique": t.get("logical_date"),
            "etat": t["state"],
            "tentatives": t.get("try_number"),
            "debut": t.get("start_date"),
            "duree_s": _num(t.get("duration")),
        } for t in tis]
        done = [e for e in entries if e["etat"] in ("success", "failed")]
        durations = [e["duree_s"] for e in done if e["duree_s"] is not None]
        consecutive = 0
        for e in done:
            if e["etat"] != "failed":
                break
            consecutive += 1
        return {
            "dag_id": dag_id, "task_id": task_id, "entrees": entries,
            "resume": {
                "executions": len(done),
                "succes": sum(e["etat"] == "success" for e in done),
                "echecs": sum(e["etat"] == "failed" for e in done),
                "echecs_consecutifs_recents": consecutive,
                "duree_mediane_s": _num(statistics.median(durations)) if durations else None,
                "duree_max_s": _num(max(durations)) if durations else None,
            },
        }

    return server


def build_backend(kind: str, snapshot: str | None) -> AirflowBackend:
    if kind == "replay":
        if not snapshot:
            raise SystemExit("--snapshot est obligatoire en mode replay")
        return ReplayAirflow.from_file(snapshot)
    return LiveAirflow(
        os.environ.get("AIRFLOW_API_URL", "http://localhost:8080"),
        os.environ.get("AIRFLOW_USERNAME", "diag_reader"),
        os.environ.get("AIRFLOW_PASSWORD") or os.environ.get("AIRFLOW_READER_PASSWORD", ""),
    )


def main() -> None:
    p = argparse.ArgumentParser(description="Serveur MCP de diagnostic Airflow (lecture seule)")
    p.add_argument("--backend", choices=["live", "replay"], default=os.environ.get("MCP_BACKEND", "live"))
    p.add_argument("--snapshot", default=os.environ.get("MCP_SNAPSHOT"))
    p.add_argument("--transport", choices=["stdio", "streamable-http"], default=os.environ.get("MCP_TRANSPORT", "stdio"))
    p.add_argument("--host", default=os.environ.get("MCP_HOST", "127.0.0.1"))
    p.add_argument("--port", type=int, default=int(os.environ.get("MCP_PORT", "8765")))
    args = p.parse_args()

    server = create_server(build_backend(args.backend, args.snapshot))
    if args.transport == "stdio":
        server.run("stdio")
    else:
        allowed = [h.strip() for h in os.environ.get("MCP_ALLOWED_HOSTS", "localhost:*,127.0.0.1:*").split(",")]
        security = TransportSecuritySettings(
            enable_dns_rebinding_protection=True,
            allowed_hosts=allowed,
            allowed_origins=[f"http://{h}" for h in allowed],
        )
        server.run("streamable-http", host=args.host, port=args.port, transport_security=security)


if __name__ == "__main__":
    main()
