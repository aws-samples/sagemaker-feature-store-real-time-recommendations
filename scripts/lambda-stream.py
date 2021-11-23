import json
import base64
import subprocess
import os
import sys
from datetime import datetime
import time

import boto3

print(f"boto3 version: {boto3.__version__}")

try:
    sm = boto3.Session().client(service_name="sagemaker")
    sm_fs = boto3.Session().client(service_name="sagemaker-featurestore-runtime")
except:
    print(f"Failed while connecting to SageMaker Feature Store")
    print(f"Unexpected error: {sys.exc_info()[0]}")

# Read Environment Vars
CUSTOMER_ACTIVITY_FEATURE_GROUP = os.environ["click_stream_feature_group_name"]


def ingest_record(
    fg_name, customer_id, sum_activity_weight_last_2m, avg_product_health_index_last_2m
):
    record = [
        {"FeatureName": "customer_id", "ValueAsString": str(customer_id)},
        {
            "FeatureName": "sum_activity_weight_last_2m",
            "ValueAsString": str(sum_activity_weight_last_2m),
        },
        {
            "FeatureName": "avg_product_health_index_last_2m",
            "ValueAsString": str(avg_product_health_index_last_2m),
        },
        {
            "FeatureName": "event_time",
            "ValueAsString": str(int(round(time.time()))),
        },
    ]
    sm_fs.put_record(FeatureGroupName=fg_name, Record=record)
    return


def lambda_handler(event, context):
    inv_id = event["invocationId"]
    app_arn = event["applicationArn"]
    records = event["records"]
    print(
        f"Received {len(records)} records, invocation id: {inv_id}, app arn: {app_arn}"
    )

    ret_records = []
    for rec in records:
        data = rec["data"]
        agg_data_str = base64.b64decode(data)
        agg_data = json.loads(agg_data_str)
        print(agg_data)

        customer_id = agg_data["CUSTOMER_ID"]
        sum_activity_weight_last_2m = agg_data["SUM_ACTIVITY_WEIGHT_LAST_2M"]
        avg_product_health_index_last_2m = agg_data["AVG_PRODUCT_HEALTH_INDEX_LAST_2M"]
        print(
            f"Updating agg features for customerId: {customer_id}, Sum of activity weight last 2m: {sum_activity_weight_last_2m}, Average product health index last 2m: {avg_product_health_index_last_2m}"
        )
        ingest_record(
            CUSTOMER_ACTIVITY_FEATURE_GROUP,
            customer_id,
            sum_activity_weight_last_2m,
            avg_product_health_index_last_2m,
        )

        # Flag each record as being "Ok", so that Kinesis won't try to re-send
        ret_records.append({"recordId": rec["recordId"], "result": "Ok"})
    return {"records": ret_records}

