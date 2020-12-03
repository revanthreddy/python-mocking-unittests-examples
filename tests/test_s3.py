import io
import json
import unittest

import boto3
from botocore.exceptions import ClientError
from botocore.response import StreamingBody
from botocore.stub import Stubber
from s3 import S3Service

client = boto3.client("s3")
stubber = Stubber(client)


class TestS3(unittest.TestCase):
    def setUp(self):
        self.s3 = S3Service(client)

    def test_put_object(self):
        bucket = "random-bucket"
        key = "hello/1.png"
        object_to_save = {"hello": "world"}
        metadata = {"height": "100"}
        mock_response = {
            "ETag": '"6805f2cfc46c0f04559748bb039d69ae"',
            "VersionId": "Bvq0EDKxOcXLJXNo_Lkz37eM3R4pfzyQ",
            "ResponseMetadata": {"...": "..."},
        }
        expected_params = {
            "Body": json.dumps(object_to_save),
            "Bucket": bucket,
            "Key": key,
            "Metadata": metadata,
        }

        stubber.add_response("put_object", mock_response, expected_params)
        with stubber:
            response = self.s3.put_object(bucket, key, object_to_save, metadata)
            self.assertEqual(response, mock_response)
            with self.assertRaisesRegex(
                Exception, "Bucket, key, object and metadata are required"
            ):
                self.s3.put_object(None, key, object_to_save, {})
            with self.assertRaisesRegex(
                Exception, "Bucket, key, object and metadata are required"
            ):
                self.s3.put_object(None, None, object_to_save, {})
            with self.assertRaisesRegex(
                Exception, "Bucket, key, object and metadata are required"
            ):
                self.s3.put_object(None, key, None, {})
            with self.assertRaisesRegex(
                Exception, "Bucket, key, object and metadata are required"
            ):
                self.s3.put_object(None, key, object_to_save, None)

        stubber.add_client_error("put_object")
        with stubber:
            with self.assertRaises(ClientError):
                self.s3.put_object(bucket, key, object_to_save, {})

    def test_get_metadata(self):
        bucket = "random-bucket"
        key = "hello/1.png"
        mock_metadata = {"height": "100", "width": "200"}
        mock_response = {"Metadata": mock_metadata}
        expected_params = {"Bucket": bucket, "Key": key}

        stubber.add_response("head_object", mock_response, expected_params)
        with stubber:
            response = self.s3.get_object_metadata(bucket, key)
            self.assertEqual(response, mock_response["Metadata"])

            with self.assertRaisesRegex(Exception, "Bucket, key are required"):
                self.s3.get_object_metadata(bucket, None)
            with self.assertRaisesRegex(Exception, "Bucket, key are required"):
                self.s3.get_object_metadata(None, None)
            with self.assertRaisesRegex(Exception, "Bucket, key are required"):
                self.s3.get_object_metadata(None, key)

        stubber.add_client_error("head_object")
        with stubber:
            with self.assertRaises(ClientError):
                self.s3.get_object_metadata(bucket, key)

    def test_get_object(self):
        bucket = "random-bucket"
        key = "hello/1.png"
        mock_metadata = {"height": "100", "width": "200"}
        expected_json_file = {"hello": "world"}
        encoded_message = json.dumps(expected_json_file).encode()
        raw_stream = StreamingBody(io.BytesIO(encoded_message), len(encoded_message))
        mock_response = {"Body": raw_stream, "Metadata": mock_metadata}
        expected_params = {"Bucket": bucket, "Key": key}
        stubber.add_response("get_object", mock_response, expected_params)
        with stubber:
            response = self.s3.get_object(bucket, key)
            self.assertEqual(response, expected_json_file)

            with self.assertRaisesRegex(Exception, "Bucket, key are required"):
                self.s3.get_object(bucket, None)
            with self.assertRaisesRegex(Exception, "Bucket, key are required"):
                self.s3.get_object(None, None)
            with self.assertRaisesRegex(Exception, "Bucket, key are required"):
                self.s3.get_object(None, key)

        stubber.add_client_error("get_object")
        with stubber:
            with self.assertRaises(ClientError):
                self.s3.get_object(bucket, key)

    def test_get_object_and_its_metadata(self):
        bucket = "random-bucket"
        key = "hello/1.png"
        mock_metadata = {"height": "100", "width": "200"}
        expected_json_file = {"hello": "world"}
        encoded_message = json.dumps(expected_json_file).encode()
        raw_stream = StreamingBody(io.BytesIO(encoded_message), len(encoded_message))
        mock_response = {"Body": raw_stream, "Metadata": mock_metadata}
        expected_params = {"Bucket": bucket, "Key": key}
        stubber.add_response("get_object", mock_response, expected_params)
        with stubber:
            response_object, object_metadata = self.s3.get_object_and_its_metadata(
                bucket, key
            )
            self.assertEqual(response_object, expected_json_file)
            self.assertEqual(object_metadata, object_metadata)

            with self.assertRaisesRegex(Exception, "Bucket, key are required"):
                self.s3.get_object_and_its_metadata(bucket, None)
            with self.assertRaisesRegex(Exception, "Bucket, key are required"):
                self.s3.get_object_and_its_metadata(None, None)
            with self.assertRaisesRegex(Exception, "Bucket, key are required"):
                self.s3.get_object_and_its_metadata(None, key)

        stubber.add_client_error("get_object")
        with stubber:
            with self.assertRaises(ClientError):
                self.s3.get_object_and_its_metadata(bucket, key)


if __name__ == "__main__":
    unittest.main()