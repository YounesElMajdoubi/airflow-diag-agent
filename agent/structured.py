"""Sortie structurée fiable : JSON -> Pydantic -> validations métier -> relance si invalide.

Boucle :
  1. on envoie le prompt (le schéma JSON attendu est dans le message système) ;
  2. on extrait le JSON de la réponse, on le valide avec Pydantic ;
  3. on applique une validation métier optionnelle (ex. : les lignes citées existent) ;
  4. en cas d'échec, on renvoie au modèle SA réponse + la liste précise des erreurs,
     et on redemande ; au bout de `max_retries` relances, on abandonne proprement.

On ne demande pas au fournisseur un « JSON schema » strict, car tous ne le
supportent pas (Ollama, certains modèles) : le mode JSON simple + validation
locale marche partout et rend la comparaison entre modèles équitable.
"""

from __future__ import annotations

import json
import re
from dataclasses import dataclass, field
from typing import Callable, Generic, TypeVar

from pydantic import BaseModel, ValidationError

from agent.llm import LLM, LLMError
from agent.schemas import AppelLLM

T = TypeVar("T", bound=BaseModel)
Validator = Callable[[BaseModel], list[str]]


@dataclass
class StructuredResult(Generic[T]):
    value: T | None                 # sortie valide (schéma + validations métier)
    last_value: T | None = None     # dernière sortie conforme au schéma, même si une validation métier échoue
    appels: list[AppelLLM] = field(default_factory=list)
    relances: int = 0
    erreurs: list[str] = field(default_factory=list)


def extract_json(text: str) -> dict:
    """Accepte un JSON nu, entouré de ```json ... ```, ou précédé d'un bloc <think>."""
    text = re.sub(r"<think>.*?</think>", "", text, flags=re.S).strip()
    fence = re.search(r"```(?:json)?\s*(\{.*\})\s*```", text, flags=re.S)
    if fence:
        text = fence.group(1)
    start, end = text.find("{"), text.rfind("}")
    if start == -1 or end == -1:
        raise ValueError("aucun objet JSON trouvé dans la réponse")
    return json.loads(text[start:end + 1])


def schema_instructions(model: type[BaseModel]) -> str:
    schema = json.dumps(model.model_json_schema(), ensure_ascii=False)
    return ("Réponds UNIQUEMENT par un objet JSON valide (aucun texte autour), "
            f"conforme à ce schéma JSON :\n{schema}")


async def call_structured(llm: LLM, etape: str, system: str, user: str, model: type[T], *,
                          validate: Validator | None = None, max_retries: int = 2) -> StructuredResult[T]:
    messages = [{"role": "system", "content": f"{system}\n\n{schema_instructions(model)}"},
                {"role": "user", "content": user}]
    result: StructuredResult[T] = StructuredResult(None)
    for attempt in range(max_retries + 1):
        try:
            resp = await llm.chat(messages)
        except LLMError as e:
            result.erreurs.append(f"{etape} : {e}")
            break
        call = AppelLLM(etape=etape, modele=resp.model, tokens_entree=resp.prompt_tokens,
                        tokens_sortie=resp.completion_tokens, cout_usd=resp.cost_usd,
                        duree_s=round(resp.latency_s, 3))
        result.appels.append(call)
        try:
            value = model.model_validate(extract_json(resp.content))
            problems = validate(value) if validate else []
        except (ValueError, ValidationError) as e:
            value, problems = None, [_short_error(e)]
        if value is not None:
            result.last_value = value
        if not problems:
            result.value = value
            return result
        call.valide = False
        result.erreurs.append(f"{etape} (essai {attempt + 1}) : " + " ; ".join(problems))
        if attempt < max_retries:
            result.relances += 1
            messages = messages + [
                {"role": "assistant", "content": resp.content},
                {"role": "user", "content": "Ta réponse est invalide :\n- " + "\n- ".join(problems)
                 + "\nCorrige ces points et renvoie uniquement le JSON complet."},
            ]
    return result


def _short_error(e: Exception) -> str:
    if isinstance(e, ValidationError):
        return "; ".join(f"{'.'.join(map(str, err['loc'])) or 'racine'} : {err['msg']}" for err in e.errors()[:8])
    return f"JSON illisible : {e}"
