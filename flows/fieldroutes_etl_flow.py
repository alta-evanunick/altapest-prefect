from __future__ import annotations
"""
Production ETL pipeline for FieldRoutes → Snowflake using Prefect 2.x.

High‑level design
=================
1. **RAW layer** – land each API record _verbatim_ as JSON (`VARIANT`) so schema drift never causes data loss.
2. **STAGED layer** – parse JSON → structured columns via Snowflake SQL (or dbt models).  This is where we create/alter tables and write the heavy DDL.
3. **PROD layer** – `MERGE`/upsert from STAGED into analytics‑ready fact & dimension tables.

This script only loads RAW and runs a *minimal* staging insert (a placeholder) — extend the `INSERT … SELECT` for each entity or hand it off to dbt.

Prereqs
-------
* **Static roster** – `Ref.offices_lookup` (office_id, office_name, base_url, secret_block_name_key, secret_block_name_token)
* **Dynamic watermark** – `RAW.REF.office_entity_watermark` (office_id, entity_name, last_run_utc)
* **Prefect blocks** – secret creds (`fieldroutes‑<OfficeName>‑auth‑key`), Snowflake connector `snowflake-altapestdb`
"""

import json
import time
import datetime
from typing import Dict, List
import requests

from prefect import flow, task, get_run_logger
from prefect.blocks.system import Secret
from prefect_snowflake import SnowflakeConnector

# Entity configuration: (api_endpoint, prod_table_name, is_dimension, small_volume_flag)
ENTITIES: List[tuple[str, str, bool, bool]] = [
    # Dimensions – small, can full‑refresh
    ("customer", "Customer_Dim", True, True),
    ("employee", "Employee_Dim", True, True),
    ("office", "Office_Dim", True, True),
    ("region", "Region_Dim", True, True),
    ("serviceType", "ServiceType_Dim", True, True),
    ("customerSource", "CustomerSource_Dim", True, True),
    ("genericFlag", "GenericFlag_Dim", True, True),

    # Fact / high‑volume – incremental
    ("appointment", "Appointment_Fact", False, False),
    ("subscription", "Subscription_Fact", False, False),
    ("route", "Route_Fact", False, False),
    ("ticket", "Ticket_Dim", False, False),
    ("ticketItem", "TicketItem_Fact", False, False),
    ("payment", "Payment_Fact", False, False),
    ("appliedPayment", "AppliedPayment_Fact", False, False),
    ("note", "Note_Fact", False, True),   # low‑volume so includeData is OK
    ("task", "Task_Fact", False, False),
    ("appointmentReminder", "AppointmentReminder_Fact", False, False),
    ("door", "DoorKnock_Fact", False, False),
    ("disbursement", "FinancialTransaction_Fact", False, False),
    ("chargeback", "FinancialTransaction_Fact", False, False),
    ("flagAssignment", "FlagAssignment_Fact", False, False),
]


def chunk(seq: list, size: int):
    """Yield successive *size*-chunked lists from *seq*."""
    for i in range(0, len(seq), size):
        yield seq[i : i + size]


@task(retries=1, retry_delay_seconds=60)
def fetch_entity(
    office_row: Dict,
    entity: str,
    table_name: str,
    is_dim: bool,
    small_volume: bool,
    window_start: datetime.datetime | None,
    window_end: datetime.datetime,
):
    """Extract & load *entity* for a single office, then update watermark."""
    logger = get_run_logger()
    office_id        = office_row["office_id"]
    base_url         = office_row["base_url"]
    secret_block_key = office_row["secret_block_name_key"]
    secret_block_token = office_row["secret_block_name_token"]

    auth_key  = Secret.load(secret_block_key).get()
    auth_token = Secret.load(secret_block_token).get()
    auth = {"authenticationKey": auth_key, "authenticationToken": auth_token}

    # Build /search payload
    payload = {"officeIDs": office_id, **auth, "includeData": 1 if (small_volume and window_start) else 0}
    if window_start and not is_dim:
        payload["dateUpdatedStart"] = window_start.strftime("%Y-%m-%dT%H:%M:%SZ")
        payload["dateUpdatedEnd"]   = window_end.strftime("%Y-%m-%dT%H:%M:%SZ")

    # --- /search with retry
    for attempt in range(5):
        try:
            resp = requests.post(f"{base_url}/{entity}/search", json=payload, timeout=30)
            resp.raise_for_status()
            break
        except Exception as e:
            if attempt == 4:
                raise
            time.sleep(2 ** attempt)

    data           = resp.json()
    records        = data.get("resolvedObjects", []) or data.get("ResolvedObjects", [])
    unresolved_ids = data.get(f"{entity}IDsNoDataExported", [])

    # --- /get pagination
    for id_chunk in chunk(unresolved_ids, 1000):
        for attempt in range(5):
            try:
                bulk = requests.post(f"{base_url}/{entity}/get", json={f"{entity}IDs": id_chunk, **auth}, timeout=30)
                bulk.raise_for_status()
                break
            except Exception:
                if attempt == 4:
                    raise
                time.sleep(2 ** attempt)
        bulk_data = bulk.json()
        records.extend(bulk_data if isinstance(bulk_data, list) else bulk_data.get("resolvedObjects", []))

    logger.info(f"Office {office_id} – {entity}: {len(records)} records fetched")

    load_ts = window_end.strftime("%Y-%m-%d %H:%M:%S")
    sf_block = SnowflakeConnector.load("snowflake-altapestdb")
    with sf_block.get_connection() as sf_conn:     # or .connect() on older plugin
        cur = sf_conn.cursor()
        # RAW layer (verbatim JSON)
        cur.executemany(
            f"INSERT INTO RAW.fieldroutes.{table_name} (officeid, loaddatetimeutc, rawdata) VALUES (%s,%s,PARSE_JSON(%s))",
            [(office_id, load_ts, json.dumps(r)) for r in records],
        )
        # STAGED – **placeholder** extraction (extend for real DDL later)
        cur.execute(
            f"""
            INSERT INTO STAGED.fieldroutes.{table_name}
            SELECT officeid,
                   loaddatetimeutc,
                   rawdata:'{entity}ID'::NUMBER       AS naturalid,
                   rawdata:'dateUpdated'::TIMESTAMP_NTZ AS dateupdated
            FROM   RAW.fieldroutes.{table_name}
            WHERE  loaddatetimeutc = '{load_ts}'
              AND  officeid = {office_id};
            """
        )
        # PROD merge
        cur.execute(
            f"""
            MERGE INTO PROD.fieldroutes.{table_name} T
            USING STAGED.fieldroutes.{table_name} S
              ON T.officeid = S.officeid AND T.naturalid = S.naturalid
            WHEN MATCHED THEN
              UPDATE SET dateupdated      = S.dateupdated,
                         loaddatetimeutc = S.loaddatetimeutc
            WHEN NOT MATCHED THEN
              INSERT (officeid, naturalid, dateupdated, loaddatetimeutc)
              VALUES (S.officeid, S.naturalid, S.dateupdated, S.loaddatetimeutc);
            """
        )
        # Watermark update
        cur.execute(
            """UPDATE RAW.REF.office_entity_watermark SET last_run_utc = %s WHERE office_id = %s AND entity_name = %s""",
            (load_ts, office_id, entity),
        )
        sf_conn.commit()


@flow(name="FieldRoutes_Nightly_ETL")
def run_nightly_fieldroutes_etl():
    logger      = get_run_logger()
    now = datetime.datetime.now(datetime.UTC)
    yesterday = now - datetime.timedelta(days=1)

    sf_block = SnowflakeConnector.load("snowflake-altapestdb")
    with sf_block.get_connection() as sf_conn:     # or .connect() on older plugin
        cur = sf_conn.cursor()
        cur.execute(
            """
            SELECT o.office_id, o.office_name, o.base_url, o.secret_block_name_key, o.secret_block_name_token,
                   w.entity_name, w.last_run_utc
            FROM   Ref.offices_lookup  o
            JOIN   RAW.REF.office_entity_watermark w USING (office_id);
            """
        )
        rows = cur.fetchall()

    offices: Dict[int, Dict] = {}
    for office_id, office_name, base_url, secret_block_key, secret_block_token, entity_name, last_run in rows:
        offices.setdefault(
            office_id,
            {
                "office_id": office_id,
                "office_name": office_name,
                "base_url": base_url,
                "secret_block_name_key": secret_block_key   ,
                "secret_block_name_token": secret_block_token,
                "watermarks": {},
            },
        )
        offices[office_id]["watermarks"][entity_name] = last_run

    # --- launch tasks
    futures = []
    for office in offices.values():
        for api_e, table_name, is_dim, small_vol in ENTITIES:
            fetch_entity.submit(
                office_row=office,
                entity=api_e,
                table_name=table_name,
                is_dim=is_dim,
                small_volume=small_vol,
                window_start=yesterday, # office["watermarks"].get(api_e),
                window_end=now,
            )

            futures.append(fut)

    # --- wait for completion; raise if any crashed
    for fut in futures:
        try:
            fut.result()        # blocks until that task finishes or fails
        except Exception as exc:
            logger.error(f"Sub-task crashed: {exc}")
            raise

if __name__ == "__main__":
    run_nightly_fieldroutes_etl()
