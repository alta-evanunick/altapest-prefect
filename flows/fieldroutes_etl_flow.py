from __future__ import annotations
"""
FieldRoutes → Snowflake nightly ETL (Prefect 2.x)
• GET requests with header‑based auth (AuthenticationKey / AuthenticationToken)
• Entity‑specific date field (dateUpdated vs dateAdded)
• Exponential back‑off on 5xx & network errors
• Parent flow waits for all mapped tasks
"""

import json, time, datetime, urllib.parse
from typing import Dict, List
import requests
from prefect import flow, task, get_run_logger
from prefect.blocks.system import Secret
from prefect_snowflake import SnowflakeConnector

# ─────────────────────────────────────────────────────────────────────────────
# Entity metadata
# ─────────────────────────────────────────────────────────────────────────────
#   endpoint,         prod_table,   is_dim, includeData_small,  date_field
ENTITY_META = [
    ("customer",       "Customer_Dim",        True,  False, "dateUpdated"),
    ("employee",       "Employee_Dim",        True,  False, "dateUpdated"),
    ("office",         "Office_Dim",          True,   True, "dateAdded"),
    ("region",         "Region_Dim",          True,   True, "dateAdded"),
    ("serviceType",    "ServiceType_Dim",     True,   True, "dateAdded"),
    ("customerSource", "CustomerSource_Dim",  True,   True, "dateAdded"),
    ("genericFlag",    "GenericFlag_Dim",     True,   True, "dateAdded"),

    ("appointment",    "Appointment_Fact",    False, False, "dateUpdated"),
    ("subscription",   "Subscription_Fact",   False, False, "dateUpdated"),
    ("route",          "Route_Fact",          False, False, "dateUpdated"),
    ("ticket",         "Ticket_Dim",          False, False, "dateUpdated"),
    ("ticketItem",     "TicketItem_Fact",     False, False, "dateUpdated"),
    ("payment",        "Payment_Fact",        False, False, "dateUpdated"),
    ("appliedPayment", "AppliedPayment_Fact", False, False, "dateUpdated"),
    ("note",           "Note_Fact",           False, False, "dateAdded"),
    ("task",           "Task_Fact",           False, False, "dateAdded"),
    ("appointmentReminder", "AppointmentReminder_Fact", False, False, "dateUpdated"),
    ("door",           "DoorKnock_Fact",      False, False, "dateUpdated"),
    ("disbursement",   "FinancialTransaction_Fact", False, False, "dateUpdated"),
    ("chargeback",     "FinancialTransaction_Fact", False, False, "dateUpdated"),
    ("flagAssignment", "FlagAssignment_Fact", False, False, "dateAdded"),
]

# helper to chunk id lists
chunk = lambda seq, size: (seq[i : i + size] for i in range(0, len(seq), size))

# ─────────────────────────────────────────────────────────────────────────────
# Task – fetch one entity for one office
# ─────────────────────────────────────────────────────────────────────────────
@task(retries=1, retry_delay_seconds=60)
def fetch_entity(office: Dict, meta: Dict, window_start: datetime.datetime | None, window_end: datetime.datetime):
    logger = get_run_logger()
    logger.info("🚀 Nightly FieldRoutes ETL flow started")

    entity       = meta["endpoint"]
    table_name   = meta["table"]
    date_field   = meta["date_field"]
    includeData  = 1 if (meta["small"] and window_start) else 0

    base_url     = office["base_url"]
    headers = {
        "AuthenticationKey":   office["auth_key"],
        "AuthenticationToken": office["auth_token"],
    }

    # build query params
    params = {
        "officeIDs": office["office_id"],
        "includeData": includeData,
    }
    # FieldRoutes expects complex filters in JSON‑string form, e.g.
    #   dateCreated={"operator":"BETWEEN","value":["2025-01-01","2025-01-31"]}
    if window_start and not meta["is_dim"]:
        params[date_field] = json.dumps({
            "operator": "BETWEEN",
            "value": [
                window_start.strftime("%Y-%m-%d"),
                window_end.strftime("%Y-%m-%d")
            ]
        })

    # ── /search with retry ──────────────────────────────────────────────────
    for attempt in range(5):
        try:
            resp = requests.get(f"{base_url}/{entity}/search", headers=headers, params=params, timeout=30)
            resp.raise_for_status()
            break
        except requests.HTTPError as e:
            if e.response is not None and e.response.status_code >= 500:
                time.sleep(2 ** attempt); continue
            raise
        except Exception:
            time.sleep(2 ** attempt)
    else:
        raise RuntimeError(f"{entity}/search failed after retries – office {office['office_id']}")

    data = resp.json()
    records = data.get("resolvedObjects", []) or data.get("ResolvedObjects", [])
    unresolved = data.get(f"{entity}IDsNoDataExported", [])

    # ── /get chunks ─────────────────────────────────────────────────────────
    for id_chunk in chunk(unresolved, 1000):
        chunk_params = [(f"{entity}IDs", str(id)) for id in id_chunk]
        qs = urllib.parse.urlencode(chunk_params, doseq=True)
        for attempt in range(5):
            try:
                bulk = requests.get(f"{base_url}/{entity}/get?{qs}", headers=headers, timeout=30)
                bulk.raise_for_status(); break
            except Exception:
                if attempt == 4: raise
                time.sleep(2 ** attempt)
        bulk_data = bulk.json()
        records.extend(bulk_data if isinstance(bulk_data, list) else bulk_data.get("resolvedObjects", []))

    logger.info(f"Office {office['office_id']} – {entity}: {len(records)} records")

    # ── Load to Snowflake ───────────────────────────────────────────────────
    load_ts = window_end.strftime("%Y-%m-%d %H:%M:%S")
    sf = SnowflakeConnector.load("snowflake-altapestdb").get_connection()
    with sf as conn:
        cur = conn.cursor()
        cur.executemany(
            f"INSERT INTO RAW.fieldroutes.{table_name} (OfficeID, LoadDatetimeUTC, RawData) VALUES (%s,%s,PARSE_JSON(%s))",
            [(office["office_id"], load_ts, json.dumps(r)) for r in records],
        )
        # watermark
        cur.execute(
            "UPDATE RAW.REF.office_entity_watermark SET last_run_utc = %s WHERE office_id = %s AND entity_name = %s",
            (load_ts, office["office_id"], entity),
        )
        conn.commit()

# ─────────────────────────────────────────────────────────────────────────────
# Flow – nightly full load
# ─────────────────────────────────────────────────────────────────────────────
@flow(name="FieldRoutes_Nightly_ETL")
def run_nightly_fieldroutes_etl():
    logger = get_run_logger()
    now = datetime.datetime.now(datetime.UTC)
    yday = now - datetime.timedelta(days=1)

    # pull roster + watermarks
    sf = SnowflakeConnector.load("snowflake-altapestdb").get_connection()
    with sf as conn:
        cur = conn.cursor()
        cur.execute("""
            SELECT o.office_id, o.office_name, o.base_url,
                   o.secret_block_name_key, o.secret_block_name_token,
                   w.entity_name, w.last_run_utc
            FROM   Ref.offices_lookup o
            JOIN   RAW.REF.office_entity_watermark w USING (office_id);
        """)
        rows = cur.fetchall()

    offices: Dict[int, Dict] = {}
    for office_id, name, url, key_blk, tok_blk, entity_name, last_run in rows:
        offices.setdefault(office_id, {
            "office_id": office_id,
            "office_name": name,
            "base_url": url,
            "auth_key": Secret.load(key_blk).get(),
            "auth_token": Secret.load(tok_blk).get(),
            "watermarks": {},
        })
        offices[office_id]["watermarks"][entity_name] = last_run

    # submit & await
    futures = []
    meta_list = [
        {"endpoint": ep, "table": tbl, "is_dim": dim, "small": small, "date_field": df}
        for ep, tbl, dim, small, df in ENTITY_META
    ]

    for office in offices.values():
        for meta in meta_list:
            fut = fetch_entity.submit(
                office=office,
                meta=meta,
                window_start=yday,
                window_end=now,
            )
            futures.append(fut)

    for fut in futures:
        fut.result()   # will raise if any task crashed

    logger.info("✅ Nightly FieldRoutes ETL flow finished")

if __name__ == "__main__":
    run_nightly_fieldroutes_etl()
