
import unittest

import boto3
from botocore.stub import Stubber
from textract import TextractService

import pytest


@pytest.fixture
def stub_and_client():
    """Patch Textract client with Stubbed client
    """
    textract = boto3.client("textract")
    stub = Stubber(textract)

    return {"stub": stub, "client": textract}


def test_analyze_document(stub_and_client):
    textract_stub = stub_and_client["stub"]
    client = stub_and_client["client"]
    textract_service = TextractService(client)
    bucket = "random-bucket"
    key = "hello/1.png"
    feature_types = ["FORMS", "VALUES"]
    mock_response = {"DocumentMetadata": {}}
    expected_params = {
        "Document": {
            "S3Object": {"Bucket": bucket, "Name": key}
        },
        "FeatureTypes": feature_types,
    }

    textract_stub.add_response("analyze_document", mock_response, expected_params)

    with textract_stub:
        response = textract_service.analyze_document(bucket, key, feature_types)
        assert response == mock_response
        with pytest.raises(ValueError, match=r"bucket, key and feature types are required fields"):
            textract_service.analyze_document(None, key, feature_types)


    textract_stub.add_client_error("analyze_document" ,service_error_code='ThrottlingException')
    with textract_stub:
        with pytest.raises(Exception , match=r"Throttling Error"):
            textract_service.analyze_document(bucket, key, feature_types)

    # textract_stub.add_client_error("analyze_document",service_error_code='UnsupportedDocumentException')
    # with textract_stub:
    #     with pytest.raises(Exception , match=r"Do something for Bad documents"):
    #         textract_service.analyze_document(bucket, key, feature_types)
