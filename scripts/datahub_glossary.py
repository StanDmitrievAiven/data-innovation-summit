#!/usr/bin/env python3
"""
Enrich the demo's DataHub catalog with descriptions + a small business glossary
and link the glossary terms to the right datasets and columns.

What this adds (idempotent — every aspect is UPSERTed):

  1. Business glossary scoped to the webstore demo:
       * 1 root node: Webstore
       * 4 sub-nodes: Business Entities, Attributes, Privacy, CDC Metadata
       * 17 terms (Customer, Product, Order, OrderItem, Email, PersonalName,
         Region, Revenue, OrderStatus, SKU, Quantity, PII, DebeziumOp,
         SourceTimestamp, DeletionFlag, ...)
  2. Dataset descriptions on every PG / Kafka / CH copy of each entity,
     plus dataset-level glossary term links (e.g. all 5 customer datasets ->
     the Customer term).
  3. Column descriptions and column-level glossary term links via
     editableSchemaMetadata, e.g.:
       * customers.email  -> Email + PII
       * orders.total     -> Revenue
       * <table>.region   -> Region
       * __op / __source_ts_ms / __deleted -> their CDC metadata terms
"""
import json
import os
import sys
import time
import urllib.error
import urllib.request

GMS = os.environ["DH_GMS"].rstrip("/")
TOKEN = os.environ["DH_TOKEN"]

now_ms = int(time.time() * 1000)
actor = "urn:li:corpuser:datahub"


# --- HTTP helpers ----------------------------------------------------------

def post(path: str, body: dict) -> dict:
    req = urllib.request.Request(
        f"{GMS}{path}",
        method="POST",
        data=json.dumps(body).encode(),
        headers={
            "Authorization": f"Bearer {TOKEN}",
            "Content-Type": "application/json",
            "X-RestLi-Protocol-Version": "2.0.0",
        },
    )
    try:
        with urllib.request.urlopen(req, timeout=30) as r:
            raw = r.read().decode()
            return json.loads(raw) if raw else {}
    except urllib.error.HTTPError as e:
        body_text = e.read().decode()
        print(
            f"  ! POST {path} -> {e.code} {e.reason}\n      body: {body_text[:300]}",
            file=sys.stderr,
        )
        raise


def ingest_aspect(entity_type: str, entity_urn: str, aspect_name: str, aspect_value: dict) -> None:
    body = {
        "proposal": {
            "entityType": entity_type,
            "entityUrn": entity_urn,
            "aspectName": aspect_name,
            "changeType": "UPSERT",
            "aspect": {
                "value": json.dumps(aspect_value),
                "contentType": "application/json",
            },
        }
    }
    post("/aspects?action=ingestProposal", body)


def audit() -> dict:
    return {"time": now_ms, "actor": actor}


def node_urn(node_id: str) -> str:
    return f"urn:li:glossaryNode:{node_id}"


def term_urn(term_id: str) -> str:
    return f"urn:li:glossaryTerm:{term_id}"


# --- 1. Glossary nodes -----------------------------------------------------

NODES: dict[str, dict] = {
    "Webstore": {
        "name": "Webstore",
        "definition": "Business entities, attributes and metadata for the Aiven webstore demo "
                      "(PG -> Kafka -> ClickHouse -> Dashboard, with DataHub for catalog and lineage).",
        "parent": None,
    },
    "Webstore.BusinessEntities": {
        "name": "Business Entities",
        "definition": "Top-level subjects the webstore manages.",
        "parent": "Webstore",
    },
    "Webstore.Attributes": {
        "name": "Attributes",
        "definition": "Reusable column-level concepts that appear on multiple tables.",
        "parent": "Webstore",
    },
    "Webstore.Privacy": {
        "name": "Privacy",
        "definition": "Privacy-sensitive concepts (PII, regulated data).",
        "parent": "Webstore",
    },
    "Webstore.CDCMetadata": {
        "name": "CDC Metadata",
        "definition": "Change-data-capture envelope fields added to every Kafka and ClickHouse "
                      "row by Debezium's ExtractNewRecordState transform.",
        "parent": "Webstore",
    },
}

print("=== Glossary nodes ===")
for node_id, info in NODES.items():
    urn = node_urn(node_id)
    print(f"  {urn:<60s}  {info['name']}")
    node_info: dict = {
        "name": info["name"],
        "definition": info["definition"],
    }
    if info["parent"]:
        # Parent is encoded inside glossaryNodeInfo on this DataHub version
        # (not a separate parentNode aspect — that's glossaryTerm-only here).
        node_info["parentNode"] = node_urn(info["parent"])
    ingest_aspect("glossaryNode", urn, "glossaryNodeInfo", node_info)


# --- 2. Glossary terms -----------------------------------------------------
# (term_id, display_name, definition, parent_node_id)
TERMS: list[tuple[str, str, str, str]] = [
    ("Webstore.Customer",   "Customer",   "An individual who can place orders. Has a name, email and home region.",                                            "Webstore.BusinessEntities"),
    ("Webstore.Product",    "Product",    "A sellable item. Has a SKU, category, price (EUR) and inventory level.",                                            "Webstore.BusinessEntities"),
    ("Webstore.Order",      "Order",      "A purchase made by a customer. Has a status lifecycle and a total in EUR.",                                         "Webstore.BusinessEntities"),
    ("Webstore.OrderItem",  "Order Item", "A single line within an order: product, quantity, unit price at purchase.",                                          "Webstore.BusinessEntities"),

    ("Webstore.Email",        "Email Address",   "An RFC-5321 email address. Treated as PII.",                                                                  "Webstore.Attributes"),
    ("Webstore.PersonalName", "Personal Name",   "A natural person's display name. Treated as PII.",                                                            "Webstore.Attributes"),
    ("Webstore.Region",       "Region",          "A geographic bucket (EMEA, AMER, APAC, plus EMEA-* sub-regions). Drives the demo's headline filter.",         "Webstore.Attributes"),
    ("Webstore.Revenue",      "Revenue (EUR)",   "A monetary value in Euros. Source type Decimal(10,2), serialised to a JSON string by Debezium.",              "Webstore.Attributes"),
    ("Webstore.OrderStatus",  "Order Status",    "Lifecycle state of an order: pending / processing / shipped / delivered / cancelled.",                        "Webstore.Attributes"),
    ("Webstore.SKU",          "SKU",             "Stock Keeping Unit — a unique product identifier.",                                                           "Webstore.Attributes"),
    ("Webstore.Quantity",     "Quantity",        "An integer count of units bought.",                                                                            "Webstore.Attributes"),

    ("Webstore.PII",          "PII",             "Personally Identifiable Information. Datasets and columns tagged PII require restricted access "
                                                 "and downstream masking.",                                                                                    "Webstore.Privacy"),

    ("Webstore.DebeziumOp",      "Debezium Op",       "Operation code added by ExtractNewRecordState: 'c' (create), 'u' (update), 'd' (delete), 'r' (snapshot read).", "Webstore.CDCMetadata"),
    ("Webstore.SourceTimestamp", "Source Timestamp",  "Epoch milliseconds at which the change was committed in PostgreSQL.",                                              "Webstore.CDCMetadata"),
    ("Webstore.DeletionFlag",    "Deletion Flag",     "'true' if the row was deleted in the source. ClickHouse FINAL queries filter on this.",                            "Webstore.CDCMetadata"),
]

print("\n=== Glossary terms ===")
for term_id, name, definition, parent_id in TERMS:
    urn = term_urn(term_id)
    print(f"  {urn:<55s}  {name}  ({parent_id})")
    term_info: dict = {
        "name": name,
        "definition": definition,
        "termSource": "INTERNAL",
    }
    if parent_id:
        # Same as nodes: parent goes inside the Info aspect on this DataHub version.
        term_info["parentNode"] = node_urn(parent_id)
    ingest_aspect("glossaryTerm", urn, "glossaryTermInfo", term_info)


# --- 3. Dataset descriptions + dataset-level term links --------------------

TABLES = ("customers", "products", "orders", "order_items")

DATASET_TERMS: dict[str, list[str]] = {
    "customers":   ["Webstore.Customer"],
    "products":    ["Webstore.Product"],
    "orders":      ["Webstore.Order"],
    "order_items": ["Webstore.OrderItem"],
}

DATASET_DESCRIPTIONS: dict[str, str] = {
    "customers":   "Webstore customers — the source-of-truth for who places orders, plus their region (drives the EMEA filter on the dashboard).",
    "products":    "Webstore product catalog. Mutated continuously by the simulator's restock and reprice actions.",
    "orders":      "Customer orders. The simulator transitions each order through pending -> processing -> shipped -> delivered (with occasional cancellations).",
    "order_items": "Line items inside each order. Joining order_items + products + orders is the core of the EMEA top-products query.",
}


def all_dataset_urns_for(table: str) -> list[str]:
    return [
        f"urn:li:dataset:(urn:li:dataPlatform:postgres,defaultdb.public.{table},PROD)",
        f"urn:li:dataset:(urn:li:dataPlatform:kafka,webshop.public.{table},PROD)",
        f"urn:li:dataset:(urn:li:dataPlatform:clickhouse,service_kafka-1b5cb1e7.{table},PROD)",
        f"urn:li:dataset:(urn:li:dataPlatform:clickhouse,default.{table}_mv,PROD)",
        f"urn:li:dataset:(urn:li:dataPlatform:clickhouse,default.{table},PROD)",
    ]


print("\n=== Dataset descriptions + dataset-level glossary terms ===")
for table in TABLES:
    description = DATASET_DESCRIPTIONS[table]
    term_ids = DATASET_TERMS[table]
    for urn in all_dataset_urns_for(table):
        ingest_aspect("dataset", urn, "editableDatasetProperties", {
            "description": description,
        })
        ingest_aspect("dataset", urn, "glossaryTerms", {
            "terms": [{"urn": term_urn(t)} for t in term_ids],
            "auditStamp": audit(),
        })
        print(f"  {urn}")


# --- 4. Column descriptions + column-level term links ----------------------

# (description, [glossary term ids])
COLUMNS: dict[str, dict[str, tuple[str, list[str]]]] = {
    "customers": {
        "id":         ("Surrogate primary key.", []),
        "email":      ("Customer email address.", ["Webstore.Email", "Webstore.PII"]),
        "name":       ("Customer display name.", ["Webstore.PersonalName", "Webstore.PII"]),
        "region":     ("Geographic region — EMEA / EMEA-DACH / EMEA-Nordics / EMEA-UKI / AMER / APAC.", ["Webstore.Region"]),
        "country":    ("ISO country name.", ["Webstore.Region"]),
        "created_at": ("Time the row was first inserted in the source PG.", []),
        "updated_at": ("Time the row was last touched in the source PG.", []),
    },
    "products": {
        "id":         ("Surrogate primary key.", []),
        "sku":        ("Stock keeping unit (unique).", ["Webstore.SKU"]),
        "name":       ("Product display name.", []),
        "category":   ("Product category bucket (Electronics, Outerwear, Furniture, Lighting, ...).", []),
        "price":      ("Unit price in EUR. Stored as Decimal(10,2), JSON-encoded as a string by Debezium.", ["Webstore.Revenue"]),
        "inventory":  ("Current inventory level. Decremented on order, replenished by the simulator's restock action.", []),
        "created_at": ("Time the row was first inserted in the source PG.", []),
        "updated_at": ("Time the row was last touched in the source PG.", []),
    },
    "orders": {
        "id":          ("Surrogate primary key.", []),
        "customer_id": ("Foreign key to customers.id.", []),
        "status":      ("Order lifecycle state.", ["Webstore.OrderStatus"]),
        "total":       ("Order total in EUR (sum of order_items.quantity * order_items.unit_price).", ["Webstore.Revenue"]),
        "region":      ("Snapshot of the customer's region at order time. Drives the EMEA filter.", ["Webstore.Region"]),
        "created_at":  ("Time the order was placed.", []),
        "updated_at":  ("Time of the most recent status change.", []),
    },
    "order_items": {
        "id":         ("Surrogate primary key.", []),
        "order_id":   ("Foreign key to orders.id.", []),
        "product_id": ("Foreign key to products.id.", []),
        "quantity":   ("Number of units bought.", ["Webstore.Quantity"]),
        "unit_price": ("Unit price at the time of purchase, in EUR.", ["Webstore.Revenue"]),
    },
}

DEBEZIUM_COLUMNS: dict[str, tuple[str, list[str]]] = {
    "__op":           ("Debezium operation code: 'c' (create), 'u' (update), 'd' (delete), 'r' (snapshot read).", ["Webstore.DebeziumOp"]),
    "__source_ts_ms": ("Source-side commit timestamp in epoch milliseconds.", ["Webstore.SourceTimestamp"]),
    "__deleted":      ("'true' if the row was deleted in the source. ClickHouse FINAL queries filter on this.", ["Webstore.DeletionFlag"]),
}


def field_info(field_path: str, description: str, term_ids: list[str]) -> dict:
    info = {"fieldPath": field_path, "description": description}
    if term_ids:
        info["glossaryTerms"] = {
            "terms": [{"urn": term_urn(t)} for t in term_ids],
            "auditStamp": audit(),
        }
    return info


print("\n=== Column descriptions + glossary term links ===")
for table in TABLES:
    pg_col_map = COLUMNS[table]
    pg_fields = [field_info(c, *pg_col_map[c]) for c in pg_col_map]
    full_fields = pg_fields + [field_info(c, *DEBEZIUM_COLUMNS[c]) for c in DEBEZIUM_COLUMNS]

    for urn in all_dataset_urns_for(table):
        is_pg = "dataPlatform:postgres" in urn
        fields = pg_fields if is_pg else full_fields
        aspect = {
            "editableSchemaFieldInfo": fields,
            "created": audit(),
            "lastModified": audit(),
        }
        ingest_aspect("dataset", urn, "editableSchemaMetadata", aspect)
        print(f"  {urn}: {len(fields)} fields described")

print("\nDone. Open DataHub:")
print("  - Glossary tab in the left nav -> browse 'Webstore' folder.")
print("  - Any dataset's Documentation tab -> friendly description.")
print("  - Any dataset's Schema tab -> column descriptions and term pills.")
