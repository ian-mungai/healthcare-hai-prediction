-- Attach the local Polaris catalog read-only for viewing the bronze tables.
-- Run through scripts/lakehouse/query.sh; credentials come from the environment at run time, never from this file.
.output /dev/null
.read /opt/analytics/resources.sql
SET autoinstall_known_extensions = false;
LOAD iceberg;
LOAD httpfs;
LOAD aws;
CREATE SECRET lakehouse_s3 (
    TYPE s3,
    PROVIDER credential_chain,
    CHAIN 'config',
    PROFILE getenv('AWS_PROFILE'),
    REGION getenv('AWS_REGION')
);
CREATE SECRET lakehouse_catalog (
    TYPE iceberg,
    CLIENT_ID getenv('POLARIS_READER_CLIENT_ID'),
    CLIENT_SECRET getenv('POLARIS_READER_CLIENT_SECRET'),
    OAUTH2_SERVER_URI 'http://polaris:8181/api/catalog/v1/oauth/tokens',
    OAUTH2_SCOPE 'PRINCIPAL_ROLE:ALL'
);
ATTACH 'hai_lakehouse' AS lakehouse (
    TYPE iceberg,
    ENDPOINT 'http://polaris:8181/api/catalog',
    SECRET lakehouse_catalog,
    ACCESS_DELEGATION_MODE 'none',
    READ_ONLY
);
USE lakehouse.bronze;
.output stdout
