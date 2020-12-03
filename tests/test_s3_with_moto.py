import datetime
import json
from dateutil.tz import tzutc
from io import BytesIO
from unittest.mock import patch

import boto3
from botocore.stub import Stubber, ANY
from botocore.response import StreamingBody
import pytest
from moto import mock_s3

from s3 import S3Service
dummy_bucket_name = "kkjaskdkajnsdkjndjkas"


@mock_s3
def test_put_object():
    conn = boto3.resource('s3', region_name='us-east-1')
    # We need to create the bucket since this is all in Moto's 'virtual' AWS account
    conn.create_bucket(Bucket=dummy_bucket_name)

    document_service = S3Service(boto3.client("s3"))
    object_to_save = {"hello": "world"}
    key= "text.json"
    document_service.put_object(dummy_bucket_name,key,object_to_save , metadata={})

    body = conn.Object(dummy_bucket_name, key).get()['Body'].read().decode("utf-8")

    assert body == json.dumps(object_to_save)