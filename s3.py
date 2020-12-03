import json


class S3Service:
    def __init__(self, boto_client):
        self.client = boto_client

    def put_object(self, bucket, key, object_to_save, metadata):
        if bucket is None or key is None or object_to_save is None or metadata is None:
            raise Exception("Bucket, key, object and metadata are required")
        response = self.client.put_object(
            Body=json.dumps(object_to_save), Bucket=bucket, Key=key, Metadata=metadata
        )

        return response

    def get_object(self, bucket, key):
        obj, metadata = self.get_object_and_its_metadata(bucket, key)
        return obj

    def get_object_metadata(self, bucket, key):
        if bucket is None or key is None:
            raise Exception("Bucket, key are required")

        response = self.client.head_object(Bucket=bucket, Key=key)

        return response["Metadata"]

    def get_object_and_its_metadata(self, bucket, key):
        if bucket is None or key is None:
            raise Exception("Bucket, key are required parameters")
        s3_client_obj = self.client.get_object(Bucket=bucket, Key=key)
        s3_client_data = s3_client_obj["Body"].read().decode("utf-8")
        obj = json.loads(s3_client_data)

        return obj, s3_client_obj["Metadata"]