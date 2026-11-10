"""Appels aux modèles, toujours via la passerelle LiteLLM (proxy compatible OpenAI).

L'agent ne connaît que des ALIAS (« deepseek-pro », « mistral-small », « local »…).
La correspondance alias -> fournisseur réel est dans litellm/config.yaml. Changer de
modèle ne demande donc aucune modification du code de l'agent.

Pour chaque appel on récupère :
- le coût calculé par LiteLLM (en-tête x-litellm-response-cost, en dollars) ;
- les jetons d'entrée / sortie (champ usage, format OpenAI) ;
- la durée mesurée côté agent.

`FakeLLM` sert aux tests : réponses scriptées, coût nul, marqué comme tel pour
qu'aucun résultat issu d'un faux modèle ne puisse finir dans le README.
"""

from __future__ import annotations

import os
import time
from dataclasses import dataclass
from typing import Callable, Protocol

import httpx


@dataclass
class LLMResponse:
    content: str
    model: str
    prompt_tokens: int
    completion_tokens: int
    cost_usd: float | None
    latency_s: float


class LLMError(RuntimeError):
    pass


class LLM(Protocol):
    alias: str
    is_fake: bool

    async def chat(self, messages: list[dict], *, json_mode: bool = True) -> LLMResponse: ...


RequestHook = Callable[[str, list[dict]], None]


class LiteLLMClient:
    """Client du proxy LiteLLM. `on_request` est appelé avec chaque requête (détection de fuites)."""

    is_fake = False

    def __init__(self, alias: str, base_url: str | None = None, api_key: str | None = None,
                 temperature: float = 0.0, timeout: float = 180.0, on_request: RequestHook | None = None):
        self.alias = alias
        self.base_url = (base_url or os.environ.get("LITELLM_URL", "http://localhost:4000")).rstrip("/")
        self.api_key = api_key or os.environ.get("LITELLM_MASTER_KEY", "")
        self.temperature = temperature
        self.timeout = timeout
        self.on_request = on_request

    async def chat(self, messages, *, json_mode=True):
        if self.on_request:
            self.on_request(self.alias, messages)
        body = {"model": self.alias, "messages": messages, "temperature": self.temperature}
        if json_mode:
            body["response_format"] = {"type": "json_object"}
        headers = {"Authorization": f"Bearer {self.api_key}"} if self.api_key else {}
        t0 = time.perf_counter()
        async with httpx.AsyncClient(timeout=self.timeout) as http:
            r = await http.post(f"{self.base_url}/chat/completions", json=body, headers=headers)
        latency = time.perf_counter() - t0
        if r.status_code >= 400:
            raise LLMError(f"LiteLLM {self.alias} : HTTP {r.status_code} {r.text[:300]}")
        data = r.json()
        usage = data.get("usage") or {}
        cost = r.headers.get("x-litellm-response-cost")
        return LLMResponse(
            content=data["choices"][0]["message"].get("content") or "",
            model=self.alias,
            prompt_tokens=usage.get("prompt_tokens", 0),
            completion_tokens=usage.get("completion_tokens", 0),
            cost_usd=float(cost) if cost not in (None, "", "None") else None,
            latency_s=latency,
        )


class FakeLLM:
    """Modèle factice pour les tests : `responder(messages) -> str`."""

    is_fake = True

    def __init__(self, alias: str, responder: Callable[[list[dict]], str], on_request: RequestHook | None = None):
        self.alias = alias
        self.responder = responder
        self.on_request = on_request
        self.calls: list[list[dict]] = []

    async def chat(self, messages, *, json_mode=True):
        if self.on_request:
            self.on_request(self.alias, messages)
        self.calls.append(messages)
        return LLMResponse(self.responder(messages), self.alias, 0, 0, 0.0, 0.0)
