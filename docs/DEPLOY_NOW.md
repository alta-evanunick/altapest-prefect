# Deploy the New Snowflake Flows - Quick Instructions

## Step 1: Deploy from prefect.yaml
```bash
cd "C:\Users\evanu\Documents\ETC Solutions\OctaneInsights\Prefect\altapest-prefect"
env\Scripts\activate

# Deploy all flows defined in prefect.yaml
prefect deploy --all

# Or deploy specific flows
prefect deploy --name fieldroutes-nightly-snowflake-direct
prefect deploy --name fieldroutes-cdc-snowflake-direct
```

## Step 2: Verify Deployments
Check that the new deployments appear in Prefect UI:
- `fieldroutes-nightly-snowflake-direct` (runs at 3 AM PT daily)
- `fieldroutes-cdc-snowflake-direct` (runs every 2 hours during business hours)

## Step 3: Run Manual Test
```bash
# Test with office 1 first
prefect deployment run 'FieldRoutes_Nightly_ETL_Snowflake/fieldroutes-nightly-snowflake-direct' -p test_office_id=1

# Monitor in Prefect UI and check logs
```

## Step 4: Check Data in Snowflake
Run the queries in `check_load_detailed.sql` to verify data is loading correctly.

## What Changed in prefect.yaml
1. Added new Snowflake-direct deployments at the top
2. Removed Azure dependencies from new deployments  
3. Tagged old deployments as [LEGACY]
4. Updated to use `flows/deploy_flows_snowflake.py` entrypoints

## Troubleshooting
If deployments don't show up:
1. Make sure you're on latest main branch: `git pull`
2. Check Prefect is connected: `prefect cloud workspace ls`
3. Force redeploy: `prefect deployment delete <old-deployment-name>` then `prefect deploy --all`

## Next: Full Load
Once test succeeds, run full load (all offices):
```bash
prefect deployment run 'FieldRoutes_Nightly_ETL_Snowflake/fieldroutes-nightly-snowflake-direct'
```

Remember: The new flow handles 50K+ record pagination automatically!