"""
Qdrant Collections Initialization — OrgBrain Banking Brain
Creates collections for banking event embeddings and customer profiles.
"""

import time
import logging
from qdrant_client import QdrantClient
from qdrant_client.models import (
    Distance,
    VectorParams,
    PayloadSchemaType,
    CreateAliasOperation,
    CreateAlias,
)

log = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)

EMBEDDING_DIM = 768  # nomic-embed-text output dimension

COLLECTIONS = [
    {
        "name": "banking_events",
        "description": "Narratives of significant banking events (transactions, alerts, onboarding)",
        "payload_indexes": [
            ("customer_id", PayloadSchemaType.KEYWORD),
            ("event_type",  PayloadSchemaType.KEYWORD),
            ("entity_type", PayloadSchemaType.KEYWORD),
            ("tx_timestamp", PayloadSchemaType.FLOAT),
            ("amount",      PayloadSchemaType.FLOAT),
            ("merchant_mcc", PayloadSchemaType.KEYWORD),
            ("channel",     PayloadSchemaType.KEYWORD),
        ],
    },
    {
        "name": "customer_profiles",
        "description": "Periodic customer profile summaries (re-embedded weekly by Airflow)",
        "payload_indexes": [
            ("customer_id",  PayloadSchemaType.KEYWORD),
            ("income_band",  PayloadSchemaType.KEYWORD),
            ("segment_ids",  PayloadSchemaType.KEYWORD),
            ("product_codes", PayloadSchemaType.KEYWORD),
            ("churn_score",  PayloadSchemaType.FLOAT),
            ("updated_at",   PayloadSchemaType.FLOAT),
        ],
    },
    {
        "name": "merchant_intel",
        "description": "Merchant behavioral summaries for spend pattern analysis",
        "payload_indexes": [
            ("merchant_name", PayloadSchemaType.KEYWORD),
            ("merchant_mcc",  PayloadSchemaType.KEYWORD),
            ("category_group", PayloadSchemaType.KEYWORD),
            ("tx_count_30d",  PayloadSchemaType.INTEGER),
        ],
    },
    {
        "name": "product_insights",
        "description": "Product usage narratives, adoption trends, complaint themes",
        "payload_indexes": [
            ("product_code",  PayloadSchemaType.KEYWORD),
            ("product_type",  PayloadSchemaType.KEYWORD),
            ("insight_type",  PayloadSchemaType.KEYWORD),
            ("generated_at",  PayloadSchemaType.FLOAT),
        ],
    },
]


def init_collections(qdrant_host: str = "localhost", qdrant_port: int = 6333):
    client = QdrantClient(host=qdrant_host, port=qdrant_port)

    # Wait for Qdrant to be ready
    for attempt in range(20):
        try:
            client.get_collections()
            break
        except Exception:
            log.info("Waiting for Qdrant... attempt %d", attempt + 1)
            time.sleep(3)

    existing = {c.name for c in client.get_collections().collections}

    for collection_def in COLLECTIONS:
        name = collection_def["name"]
        if name in existing:
            log.info("Collection '%s' already exists, skipping.", name)
            continue

        client.create_collection(
            collection_name=name,
            vectors_config=VectorParams(
                size=EMBEDDING_DIM,
                distance=Distance.COSINE,
                on_disk=True,  # save memory for large collections
            ),
        )
        log.info("Created collection '%s'", name)

        # Create payload indexes for efficient filtering
        for field_name, field_type in collection_def.get("payload_indexes", []):
            client.create_payload_index(
                collection_name=name,
                field_name=field_name,
                field_schema=field_type,
            )
            log.info("  Indexed payload field '%s' on '%s'", field_name, name)

    log.info("Qdrant collections initialized.")


if __name__ == "__main__":
    import os
    init_collections(
        qdrant_host=os.getenv("QDRANT_HOST", "localhost"),
        qdrant_port=int(os.getenv("QDRANT_PORT", "6333")),
    )
