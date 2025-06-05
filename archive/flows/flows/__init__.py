# """Make the flows package importable when Prefect clones the Git repo."""

# NOTE: Auto-imports disabled to prevent dependency conflicts between 
# Azure-based flows and Snowflake-direct flows. Prefect will find flows
# via the entrypoint specified in prefect.yaml
