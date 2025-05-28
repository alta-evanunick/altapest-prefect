CREATE OR REPLACE TABLE Ref.offices_lookup (
    office_id               INT PRIMARY KEY,
    office_name             STRING,
    base_url                STRING,
    secret_block_name_key   STRING,
    secret_block_name_token STRING
);

CREATE OR REPLACE TABLE Ref.office_entity_watermark (
    office_id    INT,
    entity_name  STRING,
    last_run_utc TIMESTAMP_NTZ,
    PRIMARY KEY (office_id, entity_name)
);