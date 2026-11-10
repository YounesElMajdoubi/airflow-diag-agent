"""Accès de l'agent à Airflow : UNIQUEMENT via un client MCP.

L'agent ne connaît ni l'URL d'Airflow, ni ses identifiants. Il ouvre une session
MCP vers le serveur airflow-diag, soit :
- en stdio (le serveur est lancé comme sous-processus : cas de l'évaluation et de la CI) ;
- en HTTP « streamable » (serveur déjà lancé, par ex. le service mcp-server du docker-compose).
"""

from __future__ import annotations

import os
import sys
from contextlib import asynccontextmanager
from pathlib import Path
from typing import Any, AsyncIterator

from mcp import Client, StdioServerParameters

ROOT = Path(__file__).resolve().parent.parent


class ToolCallError(RuntimeError):
    pass


class MCPTools:
    def __init__(self, client: Client):
        self.client = client
        self.calls: list[dict] = []  # trace des appels (affichée dans le rapport détaillé)

    async def call(self, name: str, args: dict[str, Any]) -> dict:
        res = await self.client.call_tool(name, args)
        self.calls.append({"outil": name, "arguments": args, "erreur": res.is_error})
        if res.is_error:
            text = res.content[0].text if res.content else "erreur inconnue"
            raise ToolCallError(f"{name} : {text}")
        return res.structured_content or {}


@asynccontextmanager
async def connect(*, url: str | None = None, snapshot: str | None = None,
                  env: dict[str, str] | None = None) -> AsyncIterator[MCPTools]:
    """url -> HTTP ; sinon lance `python -m mcp_server.server` en stdio (live, ou replay si snapshot)."""
    if url:
        target: Any = url
    else:
        args = ["-m", "mcp_server.server"]
        if snapshot:
            args += ["--backend", "replay", "--snapshot", snapshot]
        # Seules les variables utiles au serveur lui sont transmises (pas les clés des LLM).
        keep = ("AIRFLOW_API_URL", "AIRFLOW_USERNAME", "AIRFLOW_PASSWORD", "AIRFLOW_READER_PASSWORD", "PYTHONPATH")
        server_env = {k: v for k, v in os.environ.items() if k in keep}
        server_env["PYTHONPATH"] = os.pathsep.join(filter(None, [str(ROOT), server_env.get("PYTHONPATH")]))
        target = StdioServerParameters(command=sys.executable, args=args, env={**server_env, **(env or {})}, cwd=ROOT)
    async with Client(target) as client:
        yield MCPTools(client)
