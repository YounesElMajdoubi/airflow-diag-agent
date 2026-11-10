"""Accès à Airflow pour le serveur MCP : trois implémentations d'une même interface.

- LiveAirflow       : appelle l'API REST v2 d'Airflow 3 (lecture seule : uniquement des GET).
- RecordingAirflow  : enveloppe LiveAirflow et enregistre chaque réponse brute (capture des pannes).
- ReplayAirflow     : rejoue un instantané enregistré, sans Airflow (évaluation reproductible, CI).

Les réponses sont BRUTES (non masquées) : le masquage et la troncature sont faits
dans le serveur MCP, au même endroit pour les trois modes. Ainsi, améliorer le
masquage améliore aussi les rejeux, et la mesure des fuites reste honnête.
"""

from __future__ import annotations

import json
import threading
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Protocol

import httpx


class AirflowError(RuntimeError):
    """Erreur lisible renvoyée à l'agent (ressource absente, accès refusé…)."""


class AirflowBackend(Protocol):
    def now(self) -> datetime: ...
    def list_dag_runs(self, dag_id: str, *, state: str | None = None,
                      start_date_gte: datetime | None = None, limit: int = 25) -> list[dict]: ...
    def get_dag_run(self, dag_id: str, run_id: str) -> dict: ...
    def list_task_instances(self, dag_id: str, run_id: str) -> list[dict]: ...
    def get_task_log(self, dag_id: str, run_id: str, task_id: str, try_number: int) -> list: ...
    def get_dag_source(self, dag_id: str, version_number: int | None = None) -> dict: ...
    def list_task_history(self, dag_id: str, task_id: str, limit: int = 20) -> list[dict]: ...


def parse_dt(value: str | None) -> datetime | None:
    if not value:
        return None
    return datetime.fromisoformat(value.replace("Z", "+00:00"))


# --------------------------------------------------------------------------- live

class LiveAirflow:
    """Client de l'API REST v2. Authentification par jeton JWT (POST /auth/token)."""

    def __init__(self, base_url: str, username: str, password: str, timeout: float = 30.0):
        self.base_url = base_url.rstrip("/")
        self._creds = {"username": username, "password": password}
        self._http = httpx.Client(base_url=self.base_url, timeout=timeout)
        self._token: str | None = None
        self._lock = threading.Lock()

    def now(self) -> datetime:
        return datetime.now(timezone.utc)

    def _login(self) -> None:
        r = self._http.post("/auth/token", json=self._creds)
        if r.status_code >= 400:
            raise AirflowError(f"Authentification Airflow refusée (HTTP {r.status_code})")
        self._token = r.json()["access_token"]

    def _send(self, path: str, params: dict | None, headers: dict) -> httpx.Response:
        """Deux nouveaux essais sur erreur réseau passagère (Airflow chargé), puis erreur lisible."""
        for attempt in range(3):
            try:
                return self._http.get(path, params=params, headers=headers)
            except httpx.TransportError as e:
                if attempt == 2:
                    raise AirflowError(f"Airflow injoignable ou trop lent ({type(e).__name__}) : {path}") from e
                time.sleep(2 * (attempt + 1))
        raise AssertionError("inatteignable")

    def _get(self, path: str, params: dict | None = None, accept: str = "application/json") -> Any:
        for attempt in (1, 2):
            with self._lock:
                if self._token is None:
                    self._login()
                headers = {"Authorization": f"Bearer {self._token}", "Accept": accept}
            r = self._send(path, params, headers)
            if r.status_code == 401 and attempt == 1:  # jeton expiré : on se reconnecte une fois
                with self._lock:
                    self._token = None
                continue
            if r.status_code == 404:
                raise AirflowError(f"Introuvable dans Airflow : {path}")
            if r.status_code == 403:
                raise AirflowError(f"Accès refusé par Airflow : {path}")
            r.raise_for_status()
            return r.json()
        raise AirflowError("Authentification Airflow impossible")

    def list_dag_runs(self, dag_id, *, state=None, start_date_gte=None, limit=25):
        params: dict[str, Any] = {"order_by": "-run_after", "limit": limit}
        if state:
            params["state"] = state
        if start_date_gte:
            params["start_date_gte"] = start_date_gte.isoformat()
        return self._get(f"/api/v2/dags/{dag_id}/dagRuns", params)["dag_runs"]

    def get_dag_run(self, dag_id, run_id):
        return self._get(f"/api/v2/dags/{dag_id}/dagRuns/{run_id}")

    def list_task_instances(self, dag_id, run_id):
        return self._get(f"/api/v2/dags/{dag_id}/dagRuns/{run_id}/taskInstances", {"limit": 200})["task_instances"]

    def get_task_log(self, dag_id, run_id, task_id, try_number):
        data = self._get(
            f"/api/v2/dags/{dag_id}/dagRuns/{run_id}/taskInstances/{task_id}/logs/{try_number}",
            {"full_content": "true"},
        )
        return data["content"]

    def get_dag_source(self, dag_id, version_number=None):
        params = {"version_number": version_number} if version_number else None
        return self._get(f"/api/v2/dagSources/{dag_id}", params)

    def list_task_history(self, dag_id, task_id, limit=20):
        # "~" = toutes les exécutions du DAG (convention de l'API Airflow)
        return self._get(
            f"/api/v2/dags/{dag_id}/dagRuns/~/taskInstances",
            {"task_id": task_id, "order_by": "-run_after", "limit": limit},
        )["task_instances"]


# ---------------------------------------------------------------------- recording

def _key(*parts: Any) -> str:
    return "|".join(str(p) for p in parts)


class RecordingAirflow:
    """Enveloppe un backend et garde une copie de chaque réponse, pour l'instantané."""

    def __init__(self, inner: AirflowBackend):
        self.inner = inner
        self.calls: dict[str, Any] = {}
        self.captured_at = inner.now()

    def now(self):
        return self.inner.now()

    def list_dag_runs(self, dag_id, *, state=None, start_date_gte=None, limit=25):
        # On enregistre sans filtre de date : le rejeu refiltrera avec son horloge figée.
        res = self.inner.list_dag_runs(dag_id, state=state, limit=limit)
        self.calls[_key("list_dag_runs", dag_id, state)] = res
        return [r for r in res if not start_date_gte or (parse_dt(r.get("start_date")) or self.now()) >= start_date_gte]

    def get_dag_run(self, dag_id, run_id):
        res = self.inner.get_dag_run(dag_id, run_id)
        self.calls[_key("get_dag_run", dag_id, run_id)] = res
        return res

    def list_task_instances(self, dag_id, run_id):
        res = self.inner.list_task_instances(dag_id, run_id)
        self.calls[_key("list_task_instances", dag_id, run_id)] = res
        return res

    def get_task_log(self, dag_id, run_id, task_id, try_number):
        res = self.inner.get_task_log(dag_id, run_id, task_id, try_number)
        self.calls[_key("get_task_log", dag_id, run_id, task_id, try_number)] = res
        return res

    def get_dag_source(self, dag_id, version_number=None):
        res = self.inner.get_dag_source(dag_id, version_number)
        self.calls[_key("get_dag_source", dag_id, version_number)] = res
        return res

    def list_task_history(self, dag_id, task_id, limit=20):
        res = self.inner.list_task_history(dag_id, task_id, limit)
        self.calls[_key("list_task_history", dag_id, task_id)] = res
        return res

    def snapshot(self, meta: dict) -> dict:
        return {"meta": {**meta, "captured_at": self.captured_at.isoformat()}, "calls": self.calls}


# ------------------------------------------------------------------------- replay

class ReplayAirflow:
    """Rejoue un instantané : mêmes réponses, horloge figée à l'instant de la capture."""

    def __init__(self, snapshot: dict):
        self.snapshot = snapshot
        self.calls = snapshot["calls"]
        self._now = parse_dt(snapshot["meta"]["captured_at"])

    @classmethod
    def from_file(cls, path: str | Path) -> "ReplayAirflow":
        return cls(json.loads(Path(path).read_text(encoding="utf-8")))

    def now(self):
        return self._now

    def _lookup(self, *parts):
        key = _key(*parts)
        if key not in self.calls:
            raise AirflowError(f"Donnée non disponible dans l'instantané ({parts[0]} {parts[1:]})")
        return self.calls[key]

    def list_dag_runs(self, dag_id, *, state=None, start_date_gte=None, limit=25):
        runs = self._lookup("list_dag_runs", dag_id, state)
        if start_date_gte:
            runs = [r for r in runs if (parse_dt(r.get("start_date")) or self._now) >= start_date_gte]
        return runs[:limit]

    def get_dag_run(self, dag_id, run_id):
        return self._lookup("get_dag_run", dag_id, run_id)

    def list_task_instances(self, dag_id, run_id):
        return self._lookup("list_task_instances", dag_id, run_id)

    def get_task_log(self, dag_id, run_id, task_id, try_number):
        return self._lookup("get_task_log", dag_id, run_id, task_id, try_number)

    def get_dag_source(self, dag_id, version_number=None):
        return self._lookup("get_dag_source", dag_id, version_number)

    def list_task_history(self, dag_id, task_id, limit=20):
        return self._lookup("list_task_history", dag_id, task_id)[:limit]
