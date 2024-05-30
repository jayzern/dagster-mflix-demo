from dagster import EnvVar
from dagster_embedded_elt.dlt import DagsterDltResource
from dagster_snowflake import SnowflakeResource

snowflake_resource = SnowflakeResource(
    account=EnvVar("SNOWFLAKE_ACCOUNT"),  # required
    user=EnvVar("SNOWFLAKE_USER"),  # required
    password=EnvVar("SNOWFLAKE_PASSWORD"),  # password or private key required
    warehouse="dagster_wh",
    database="dagster_db",
    schema="mflix",
    role="dagster_role",
)

dlt_resource = DagsterDltResource()
