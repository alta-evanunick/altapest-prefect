# """Make the flows package importable when Prefect clones the Git repo."""

from importlib import import_module

# Auto‑import flow modules so deployment CLI finds them without fully‑qualified paths.
for _mod in [
    "flows.fieldroutes_etl_flow",
    "flows.fieldroutes_cdc_flow",
    "flows.deploy_flows",
]:
    import_module(_mod)
