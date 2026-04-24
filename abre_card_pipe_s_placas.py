import json
import os
from pathlib import Path
from typing import Any, Dict, List, Set

import requests
import urllib3  

from dotenv import load_dotenv
load_dotenv()

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)


METABASE_URL = "https://metabase.kovi.us"
METABASE_CARD_ID = 97847

PIPEFY_PIPE_ID = 301494865
PIPEFY_GRAPHQL_URL = "https://api.pipefy.com/graphql"

DRY_RUN = False

METABASE_API_KEY = os.getenv("METABASE_API_KEY")
PIPEFY_TOKEN = os.getenv("PIPEFY_TOKEN")

STATE_FILE = Path("processed_rows.json")

PIPEFY_FIELDS = {
    "license_plate": "placa",
    "city": "localiza_o",
    "created_at_brt": "data_da_solicita_o",
    "pendencia": "qual_a_pendencia_1",
}


def require_env() -> None:
    if not METABASE_API_KEY:
        raise RuntimeError("Defina a variável de ambiente METABASE_API_KEY")
    if not PIPEFY_TOKEN:
        raise RuntimeError("Defina a variável de ambiente PIPEFY_TOKEN")


def load_processed_keys() -> Set[str]:
    if not STATE_FILE.exists():
        return set()

    try:
        return set(json.loads(STATE_FILE.read_text(encoding="utf-8")))
    except Exception:
        return set()


def save_processed_keys(keys: Set[str]) -> None:
    STATE_FILE.write_text(
        json.dumps(sorted(keys), ensure_ascii=False, indent=2),
        encoding="utf-8",
    )


def normalize_text(value: Any) -> str:
    if value is None:
        return ""
    return str(value).strip()


def build_unique_key(row: Dict[str, Any]) -> str:
    license_plate = normalize_text(row.get("license_plate"))
    city = normalize_text(row.get("city"))
    created_at_brt = normalize_text(row.get("created_at_brt"))
    return f"{license_plate}|{city}|{created_at_brt}".lower()


def get_metabase_rows() -> List[Dict[str, Any]]:
    url = f"{METABASE_URL}/api/card/{METABASE_CARD_ID}/query/json"

    headers = {
        "x-api-key": METABASE_API_KEY,
        "Content-Type": "application/json",
    }

    response = requests.post(
        url,
        headers=headers,
        json={},
        timeout=90,
        verify=False  # 👈 adiciona isso
    )
    response.raise_for_status()

    data = response.json()
    if not isinstance(data, list):
        raise RuntimeError(f"Resposta inesperada do Metabase: {data}")

    return data


def pipefy_graphql(query: str, variables: Dict[str, Any]) -> Dict[str, Any]:
    headers = {
        "Authorization": f"Bearer {PIPEFY_TOKEN}",
        "Content-Type": "application/json",
    }

    response = requests.post(
        PIPEFY_GRAPHQL_URL,
        headers=headers,
        json={"query": query, "variables": variables},
        timeout=90,
        verify=False  # 👈 adiciona aqui também
    )
    response.raise_for_status()

    payload = response.json()

    if payload.get("errors"):
        raise RuntimeError(f"Erro GraphQL Pipefy: {payload['errors']}")

    return payload


def build_fields_attributes(row: Dict[str, Any]) -> List[Dict[str, Any]]:
    license_plate = normalize_text(row.get("license_plate"))
    city = normalize_text(row.get("city"))
    request_date = normalize_text(row.get("created_at_brt"))
    pendencia = normalize_text(row.get("pendencia"))

    if not license_plate:
        raise ValueError("license_plate vazio")

    if not city:
        raise ValueError("city vazio")

    if not request_date:
        raise ValueError("created_at_brt vazio")
    
    if not pendencia:
        raise ValueError("pendencia vazia")

    return [
        {
            "field_id": PIPEFY_FIELDS["license_plate"],
            "field_value": license_plate,
        },
        {
            "field_id": PIPEFY_FIELDS["city"],
            "field_value": city,
        },
        {
            "field_id": PIPEFY_FIELDS["created_at_brt"],
            "field_value": request_date,
        },
        {
            "field_id": PIPEFY_FIELDS["pendencia"],
            "field_value": pendencia,
        },
    ]

def create_pipefy_card(row: Dict[str, Any]) -> str:
    fields = build_fields_attributes(row)

    
    if DRY_RUN:
        print("\n[DRY RUN] Card NÃO será criado")
        print("Placa:", row.get("license_plate"))
        print("Cidade:", row.get("city"))
        print("Data:", row.get("created_at_brt"))
        print("Pendência:", row.get("pendencia"))
        print("Fields:", fields)
        return "DRY_RUN"

    mutation = """
    mutation CreateCard($input: CreateCardInput!) {
      createCard(input: $input) {
        card {
          id
          title
        }
      }
    }
    """

    variables = {
        "input": {
            "pipe_id": PIPEFY_PIPE_ID,
            "fields_attributes": fields,
        }
    }

    payload = pipefy_graphql(mutation, variables)
    return str(payload["data"]["createCard"]["card"]["id"])


def process() -> None:
    require_env()

    rows = get_metabase_rows()
    processed_keys = load_processed_keys()

    print(f"Total de linhas retornadas pelo Metabase: {len(rows)}")

    created = 0
    skipped = 0
    failed = 0

    for row in rows:
        try:
            unique_key = build_unique_key(row)

            if unique_key in processed_keys:
                print(f"[SKIP] Já processado: {unique_key}")
                skipped += 1
                continue

            card_id = create_pipefy_card(row)

            # 👇 só salva se NÃO for DRY RUN
            if not DRY_RUN:
                processed_keys.add(unique_key)
                save_processed_keys(processed_keys)

            print(
                f"[OK] Card criado: {card_id} | "
                f"placa={row.get('license_plate')} | "
                f"cidade={row.get('city')} | "
                f"pendencia={row.get('pendencia')} | "
                f"data={row.get('created_at_brt')}"
            )
            created += 1

        except Exception as exc:
            print(f"[ERRO] Linha: {row}")
            print(f"[ERRO] Motivo: {exc}")
            failed += 1

    print("\nResumo:")
    print(f"  Criados: {created}")
    print(f"  Ignorados: {skipped}")
    print(f"  Erros: {failed}")


if __name__ == "__main__":
    process()
