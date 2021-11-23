"""Helper functions for workshop
"""

import pandas as pd
from sagemaker.feature_store.feature_group import FeatureGroup
import time
import boto3
import sagemaker
from sagemaker.serializers import JSONSerializer
import os
import json
from sagemaker.feature_store.feature_definition import (
    FeatureDefinition,
    FeatureTypeEnum,
)
from sagemaker.session import Session

# Session variables
boto_session = boto3.Session()
region = boto_session.region_name
sagemaker_client = boto_session.client(service_name="sagemaker", region_name=region)
featurestore_runtime = boto_session.client(
    service_name="sagemaker-featurestore-runtime", region_name=region
)
account_id = boto3.client("sts").get_caller_identity()["Account"]
feature_store_session = Session(
    boto_session=boto_session,
    sagemaker_client=sagemaker_client,
    sagemaker_featurestore_runtime_client=featurestore_runtime,
)


class FMSerializer(JSONSerializer):
    def serialize(self, data):
        js = {"instances": []}
        for row in data:
            js["instances"].append({"features": row.tolist()})
        return json.dumps(js)


def query_offline_store(
    feature_group_name, query, sagemaker_session, query_output_s3_uri=None, wait=True
):
    """Query an offline store

    Args:
        feature_group_name (str): Name of the feature group
        query (str): SQL query
        sagemaker_session (sagemaker.Session()): SageMaker session
        query_output_s3_uri (str, optional): S3 uri to store output of query. Defaults to None.
        wait (bool, optional): Wait for the query to finish running. Defaults to True.

    Returns:
        pandas.DataFrame: Query results as dataframe
    """

    feature_group = FeatureGroup(
        name=feature_group_name, sagemaker_session=sagemaker_session
    )
    feature_group_athena_query = feature_group.athena_query()
    if not query_output_s3_uri:
        query_output_s3_uri = f"s3://{sagemaker_session.default_bucket()}/query_results"
    try:
        feature_group_athena_query.run(
            query_string=query, output_location=query_output_s3_uri
        )
        if wait:
            feature_group_athena_query.wait()
            return feature_group_athena_query.as_dataframe(), feature_group_athena_query
        else:
            return None, None
    except Exception as e:
        print(e)
        print(
            f'\nNote that the "{feature_group.name}" Feature Group is a table called "{feature_group_athena_query.table_name}" in Athena.'
        )


def wait_for_feature_group_creation_complete(feature_group):
    """Wait for a FeatureGroup to finish creating

    Args:
        feature_group (FeatureGroup): Feature group
    """
    status = feature_group.describe().get("FeatureGroupStatus")
    print(f"Initial status: {status}")
    while status == "Creating":
        print(f"Waiting for feature group: {feature_group.name} to be created ...")
        time.sleep(5)
        status = feature_group.describe().get("FeatureGroupStatus")
    if status != "Created":
        raise SystemExit(
            f"Failed to create feature group {feature_group.name}: {status}"
        )
    print(f"FeatureGroup {feature_group.name} was successfully created.")


def get_feature_definitions(df, feature_group):
    """Get compatible feature definitions from a dataframe

    Args:
        df (pandas.DataFrame): Dataframe
        feature_group (FeatureGroup): Feature group

    Returns:
        list: List of feature definitions
    """
    # Dtype int_, int8, int16, int32, int64, uint8, uint16, uint32
    # and uint64 are mapped to Integral feature type.

    # Dtype float_, float16, float32 and float64
    # are mapped to Fractional feature type.

    # string dtype is mapped to String feature type.

    # Our schema of our data that we expect
    # _after_ SageMaker Processing
    feature_definitions = []
    for column in df.columns:
        feature_type = feature_group._DTYPE_TO_FEATURE_DEFINITION_CLS_MAP.get(
            str(df[column].dtype), None
        )
        if not feature_type:
            feature_type = FeatureTypeEnum.STRING
        feature_definitions.append(
            FeatureDefinition(column, feature_type)
        )  # You can alternatively define your own schema
    return feature_definitions


def create_feature_group(
    df, feature_group_name, record_id, s3_prefix, sagemaker_session
):
    """Create a new feature group

    Args:
        df (pandas.DataFrame): Dataframe
        feature_group_name (str): Feature group name
        record_id (str): Name of the column in your dataframe that represents a unique id
        s3_prefix (str): Prefix to store offline feature group data
        sagemaker_session (sagemaker.Session()): SageMaker session

    Returns:
        FeatureGroup: Feature group
    """
    # Add event time to df
    event_time_name = "event_time"
    current_time_sec = int(round(time.time()))
    df["event_time"] = pd.Series([current_time_sec] * len(df), dtype="float64")

    # If the df doesn't have an id column, add it
    if record_id not in df.columns:
        df[record_id] = df.index

    feature_group = FeatureGroup(
        name=feature_group_name, sagemaker_session=sagemaker_session
    )

    feature_definitions = get_feature_definitions(df, feature_group)
    feature_group.feature_definitions = feature_definitions

    try:
        feature_group.create(
            s3_uri=f"s3://{sagemaker_session.default_bucket()}/{s3_prefix}",
            record_identifier_name=record_id,
            event_time_feature_name="event_time",
            role_arn=sagemaker.get_execution_role(),
            enable_online_store=True,
        )
        wait_for_feature_group_creation_complete(feature_group)
    except Exception as e:
        code = e.response["Error"]["Code"]
        if code == "ResourceInUse":
            print(f"Using existing feature group: {feature_group.name}")
        else:
            raise (e)
    return feature_group


def get_feature_group_table_name(feature_group):
    """Get the table name associated with a feature group

    Args:
        feature_group (FeatureGroup): Feature group

    Returns:
        str: The feature group's table name
    """
    return feature_group.athena_query().table_name


def ingest_data_into_feature_group(df, feature_group):
    """Ingest data into a feature goup

    Args:
        df (pandas.DataFrame): Dataframe
        feature_group (FeatureGroup): Feature group
    """
    print(f"Ingesting data into feature group: {feature_group.name}...")
    try:
        ingestion_manager = feature_group.ingest(
            data_frame=df, max_workers=16, max_processes=16, wait=True
        )
    except Exception as e:
        print(e)
        failed_rows = ingestion_manager.failed_rows()
        num_failed_rows = len(failed_rows)
        print(f"Num failed rows: {num_failed_rows}")
        print(f"Failed rows: {failed_rows}")
    print(f"{len(df)} records ingested into feature group: {feature_group.name}")


def _get_offline_details(fg_name, sagemaker_session, s3_uri=None):
    """Get offline feature group details

    Args:
        fg_name (str): Feature group name
        sagemaker_session (sagemaker.Session()): SageMaker session
        s3_uri (str, optional): Where to store offline query results. Defaults to None.

    Returns:
        tuple: Offline feature group table, database, and temporary uri for query results
    """
    _data_catalog_config = sagemaker_session.sagemaker_client.describe_feature_group(
        FeatureGroupName=fg_name
    )["OfflineStoreConfig"]["DataCatalogConfig"]
    _table = _data_catalog_config["TableName"]
    _database = _data_catalog_config["Database"]

    if s3_uri is None:
        s3_uri = f"s3://{sagemaker_session.default_bucket()}/offline-store"
    _tmp_uri = f"{s3_uri}/query_results/"
    return _table, _database, _tmp_uri


def _run_query(query_string, tmp_uri, database, region, verbose=True):
    """Run athena query (used to get a feature group count)

    Args:
        query_string (str): Query string
        tmp_uri (str): Uri for query results
        database (str): Database name
        region (str): Region name
        verbose (bool, optional): Verbose output. Defaults to True.

    Returns:
        pandas.DataFrame: Dataframe
    """
    athena = boto3.client("athena")
    s3_client = boto3.client("s3", region_name=region)

    # Submit the Athena query
    if verbose:
        print("Running query :\n " + query_string + "\nOn  database: " + database)
    query_execution = athena.start_query_execution(
        QueryString=query_string,
        QueryExecutionContext={"Database": database},
        ResultConfiguration={"OutputLocation": tmp_uri},
    )

    # Wait for the Athena query to complete
    query_execution_id = query_execution["QueryExecutionId"]
    query_state = athena.get_query_execution(QueryExecutionId=query_execution_id)[
        "QueryExecution"
    ]["Status"]["State"]
    while query_state != "SUCCEEDED" and query_state != "FAILED":
        time.sleep(2)
        query_state = athena.get_query_execution(QueryExecutionId=query_execution_id)[
            "QueryExecution"
        ]["Status"]["State"]

    if query_state == "FAILED":
        print(athena.get_query_execution(QueryExecutionId=query_execution_id))
        failure_reason = athena.get_query_execution(
            QueryExecutionId=query_execution_id
        )["QueryExecution"]["Status"]["StateChangeReason"]
        print(failure_reason)
        df = pd.DataFrame()
        return df
    else:
        results_file_prefix = f"offline-store/query_results/{query_execution_id}.csv"

        # Prepare query results for training.
        filename = "query_results.csv"
        results_bucket = (tmp_uri.split("//")[1]).split("/")[0]
        s3_client.download_file(results_bucket, results_file_prefix, filename)
        df = pd.read_csv("query_results.csv")
        os.remove("query_results.csv")

        s3_client.delete_object(Bucket=results_bucket, Key=results_file_prefix)
        s3_client.delete_object(
            Bucket=results_bucket, Key=results_file_prefix + ".metadata"
        )
        return df


def get_historical_record_count(fg_name, sagemaker_session, s3_uri=None):
    """Get a record count from a given feature group

    Args:
        fg_name (str): Feature group name
        sagemaker_session (sagemaker.Session()): SageMaker session
        s3_uri (str, optional): Offline query result location. Defaults to None.

    Returns:
        int: Record count of the feature group
    """
    _table, _database, _tmp_uri = _get_offline_details(
        fg_name, sagemaker_session, s3_uri
    )
    _query_string = f'SELECT COUNT(*) FROM "' + _table + f'"'
    _tmp_df = _run_query(
        _query_string,
        _tmp_uri,
        _database,
        sagemaker_session.boto_region_name,
        verbose=False,
    )
    return _tmp_df.iat[0, 0]


def wait_for_offline_data(feature_group_name, df, sagemaker_session):
    """Wait for online data to be synced to the offline feature group

    Args:
        feature_group_name (str): The name of the Feature Group
        df (pandas.DataFrame): A Pandas dataframe of data to ingest into the Feature Group
    Returns: None
    """
    df_count = df.shape[0]
    # Before extracting the data we need to check if the offline feature store was populated
    offline_store_contents = None
    while offline_store_contents is None:
        fg_record_count = get_historical_record_count(
            feature_group_name, sagemaker_session
        )
        if fg_record_count >= df_count:
            print(
                f"Features are available in the offline store for {feature_group_name}!"
            )
            offline_store_contents = fg_record_count
        else:
            print("Waiting for data in offline store...")
            time.sleep(60)


def _wait_for_feature_group_deletion_complete(feature_group_name):
    """Wait for a feature group to delete

    Args:
        feature_group_name (str): Feature group name
    """
    region = boto3.Session().region_name
    boto_session = boto3.Session(region_name=region)
    sagemaker_client = boto_session.client(service_name="sagemaker", region_name=region)

    feature_group = FeatureGroup(
        name=feature_group_name, sagemaker_session=feature_store_session
    )

    while True:
        try:
            status = feature_group.describe().get("FeatureGroupStatus")
            print("Waiting for Feature Group Deletion")
            time.sleep(5)
        except:
            break
    return


def describe_feature_group(fg_name):
    """Get feature group metadata

    Args:
        fg_name (str): Feature group name

    Returns:
        dict: Feature group metadata
    """
    return sagemaker_client.describe_feature_group(FeatureGroupName=fg_name)


def delete_feature_group(fg_name, delete_s3=True):
    """Delete a feature group

    Args:
        fg_name (str): Feature group name
        delete_s3 (bool, optional): Delete the offline feature group data. Defaults to True.
    """
    has_offline_store = True
    try:
        describe_feature_group(fg_name)["OfflineStoreConfig"]
    except:
        has_offline_store = False
        pass

    if has_offline_store:
        offline_store_config = describe_feature_group(fg_name)["OfflineStoreConfig"]
        if not offline_store_config["DisableGlueTableCreation"]:
            table_name = offline_store_config["DataCatalogConfig"]["TableName"]
            catalog_id = offline_store_config["DataCatalogConfig"]["Catalog"]
            database_name = offline_store_config["DataCatalogConfig"]["Database"]

    # Delete s3 objects from offline store for this FG
    if delete_s3 and has_offline_store:
        s3_uri = describe_feature_group(fg_name)["OfflineStoreConfig"][
            "S3StorageConfig"
        ]["S3Uri"]
        base_offline_prefix = "/".join(s3_uri.split("/")[3:])
        offline_prefix = f"{base_offline_prefix}/{account_id}/sagemaker/{region}/offline-store/{fg_name}"
        s3_bucket_name = s3_uri.split("/")[2]
        s3 = boto3.resource("s3")
        bucket = s3.Bucket(s3_bucket_name)
        coll = bucket.objects.filter(Prefix=offline_prefix)
        print(
            f"Deleting all s3 objects in prefix: {offline_prefix} in bucket {s3_bucket_name}"
        )
        resp = coll.delete()

    resp = None
    try:
        resp = sagemaker_client.delete_feature_group(FeatureGroupName=fg_name)
    except:
        pass

    _wait_for_feature_group_deletion_complete(fg_name)
    return
