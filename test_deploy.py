# Test with office 1 and customer entity only
from flows.deploy_flows import run_nightly_fieldroutes_etl

# Test single office and entity
run_nightly_fieldroutes_etl(test_office_id=1, test_entity="customer")

# Test all entities for one office
# run_nightly_fieldroutes_etl(test_office_id=1)

# Test one entity for all offices
# run_nightly_fieldroutes_etl(test_entity="customer")

# Run normal full extract (no parameters)
# run_nightly_fieldroutes_etl()