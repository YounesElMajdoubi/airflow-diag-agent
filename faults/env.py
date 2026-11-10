"""Actions bas niveau sur l'environnement du pipeline, pour provoquer et réparer les pannes.

Deux implémentations de la même interface :
- DockerEnv : la pile docker-compose du projet (cas normal) ;
- LocalEnv  : Airflow et Postgres lancés directement sur la machine (mode développement,
              utilisé pour tester l'injecteur sans Docker).

Ce sont les SEULS endroits du projet qui écrivent dans l'environnement. L'agent, lui,
n'y a aucun accès : il ne voit Airflow qu'à travers le serveur MCP en lecture seule.
"""

from __future__ import annotations

import os
import shlex
import signal
import subprocess
import time
from pathlib import Path
from typing import Protocol

import httpx


class Env(Protocol):
    dag_file: Path

    def write_data(self, rel: str, content: bytes) -> None: ...
    def delete_data(self, rel: str) -> None: ...
    def chmod_data(self, rel: str, mode: str) -> None: ...
    def sql(self, sql: str, db: str = "dwh") -> None: ...
    def sql_background(self, sql: str, db: str = "dwh") -> subprocess.Popen: ...
    def stop(self, service: str) -> None: ...
    def start(self, service: str) -> None: ...


def _run(cmd: list[str], stdin: bytes | None = None, check: bool = True) -> subprocess.CompletedProcess:
    res = subprocess.run(cmd, input=stdin, capture_output=True)
    if check and res.returncode != 0:
        raise RuntimeError(f"Commande en échec ({res.returncode}) : {shlex.join(cmd)}\n{res.stderr.decode(errors='replace')}")
    return res


def wait_http(url: str, timeout: float = 60) -> None:
    deadline = time.time() + timeout
    while time.time() < deadline:
        try:
            if httpx.get(url, timeout=3).status_code < 500:
                return
        except httpx.HTTPError:
            pass
        time.sleep(1)
    raise RuntimeError(f"{url} ne répond pas après {timeout:.0f} s")


class DockerEnv:
    """Pile docker compose. Les fichiers de données vivent dans un volume nommé, d'où les `exec`."""

    def __init__(self, project_dir: Path, fournisseur_url: str):
        self.project_dir = project_dir
        self.dag_file = project_dir / "airflow" / "dags" / "cobaye_ventes.py"
        self.fournisseur_url = fournisseur_url
        self._compose = ["docker", "compose", "--project-directory", str(project_dir)]

    def _exec(self, service: str, args: list[str], stdin: bytes | None = None, user: str | None = None, check=True):
        cmd = self._compose + ["exec", "-T"] + (["-u", user] if user else []) + [service] + args
        return _run(cmd, stdin, check)

    def write_data(self, rel, content):
        path = f"/opt/airflow/data/{rel}"
        self._exec("airflow-scheduler", ["sh", "-c", f"mkdir -p \"$(dirname {path})\" && cat > {path}"], content)

    def delete_data(self, rel):
        self._exec("airflow-scheduler", ["rm", "-f", f"/opt/airflow/data/{rel}"])

    def chmod_data(self, rel, mode):
        self._exec("airflow-scheduler", ["sh", "-c", f"mkdir -p /opt/airflow/data/{rel} && chmod {mode} /opt/airflow/data/{rel}"])

    def sql(self, sql, db="dwh"):
        self._exec("entrepot", ["psql", "-U", "postgres", "-d", db, "-v", "ON_ERROR_STOP=1", "-q", "-f", "-"], sql.encode())

    def sql_background(self, sql, db="dwh"):
        cmd = self._compose + ["exec", "-T", "entrepot", "psql", "-U", "postgres", "-d", db, "-q", "-f", "-"]
        proc = subprocess.Popen(cmd, stdin=subprocess.PIPE, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
        proc.stdin.write(sql.encode())
        proc.stdin.close()
        return proc

    def stop(self, service):
        _run(self._compose + ["stop", service])

    def start(self, service):
        _run(self._compose + ["start", service])
        if service == "entrepot":
            deadline = time.time() + 60
            while self._exec("entrepot", ["pg_isready", "-U", "postgres"], check=False).returncode != 0:
                if time.time() > deadline:
                    raise RuntimeError("l'entrepôt ne redémarre pas")
                time.sleep(1)
        elif service == "fournisseur":
            wait_http(f"{self.fournisseur_url}/health")


class LocalEnv:
    """Mode développement sans Docker : Postgres local, API simulée lancée en sous-processus."""

    def __init__(self, project_dir: Path, data_dir: Path, pg_port: int, pg_data: Path,
                 pg_os_user: str | None, fournisseur_url: str, state_dir: Path):
        self.dag_file = project_dir / "airflow" / "dags" / "cobaye_ventes.py"
        self.project_dir, self.data_dir = project_dir, data_dir
        self.pg_port, self.pg_data, self.pg_os_user = pg_port, pg_data, pg_os_user
        self.fournisseur_url = fournisseur_url
        self.state_dir = state_dir
        state_dir.mkdir(parents=True, exist_ok=True)

    def write_data(self, rel, content):
        p = self.data_dir / rel
        p.parent.mkdir(parents=True, exist_ok=True)
        p.write_bytes(content)

    def delete_data(self, rel):
        (self.data_dir / rel).unlink(missing_ok=True)

    def chmod_data(self, rel, mode):
        p = self.data_dir / rel
        p.mkdir(parents=True, exist_ok=True)
        p.chmod(int(mode, 8))

    def _psql(self, db):
        return ["psql", "-h", "localhost", "-p", str(self.pg_port), "-U", "postgres", "-d", db, "-v", "ON_ERROR_STOP=1", "-q", "-f", "-"]

    def sql(self, sql, db="dwh"):
        _run(self._psql(db), sql.encode())

    def sql_background(self, sql, db="dwh"):
        proc = subprocess.Popen(self._psql(db), stdin=subprocess.PIPE, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
        proc.stdin.write(sql.encode())
        proc.stdin.close()
        return proc

    def _pg_ctl(self, *args):
        cmd = ["/usr/lib/postgresql/16/bin/pg_ctl", "-D", str(self.pg_data), *args]
        if self.pg_os_user:
            cmd = ["su", self.pg_os_user, "-c", shlex.join(cmd)]
        _run(cmd)

    def stop(self, service):
        if service == "entrepot":
            self._pg_ctl("stop", "-m", "fast")
        elif service == "fournisseur":
            pidfile = self.state_dir / "fournisseur.pid"
            if pidfile.exists():
                try:
                    os.kill(int(pidfile.read_text()), signal.SIGTERM)
                except ProcessLookupError:
                    pass  # PID périmé
                pidfile.unlink()
            # et toute instance lancée hors de l'injecteur
            _run(["pkill", "-f", "services/fournisseur/server.py"], check=False)
            deadline = time.time() + 10
            while time.time() < deadline:
                try:
                    httpx.get(f"{self.fournisseur_url}/health", timeout=1)
                    time.sleep(0.5)
                except httpx.HTTPError:
                    return  # vraiment arrêté
            raise RuntimeError("l'API fournisseur ne s'arrête pas")

    def start(self, service):
        if service == "entrepot":
            res = subprocess.run(["pg_isready", "-h", "localhost", "-p", str(self.pg_port)], capture_output=True)
            if res.returncode != 0:
                self._pg_ctl("start", "-o", f"-p {self.pg_port} -k /tmp", "-l", "/tmp/pg.log", "-w")
        elif service == "fournisseur":
            try:
                wait_http(f"{self.fournisseur_url}/health", timeout=2)
                return
            except RuntimeError:
                pass
            proc = subprocess.Popen(
                ["python3", str(self.project_dir / "services" / "fournisseur" / "server.py")],
                stdout=open(self.state_dir / "fournisseur.log", "ab"), stderr=subprocess.STDOUT,
                start_new_session=True,
            )
            (self.state_dir / "fournisseur.pid").write_text(str(proc.pid))
            wait_http(f"{self.fournisseur_url}/health")
