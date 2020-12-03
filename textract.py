
class TextractService:
    def __init__(self, botoClient):
        self.client = botoClient

    def analyze_document(self, bucket, key, feature_types):
        """
            :param bucket:  my-bucket_name"
            :param key: hello/1/2/file.png
            :param feature_types: ["FORMS" | "TABLES"] or ["FORMS" , "TABLES"]
            :return: Analyze response https://docs.aws.amazon.com/textract/latest/dg/API_AnalyzeDocument.html  # noqa: E501

            Can raise
            Textract.Client.exceptions.InvalidParameterException
            Textract.Client.exceptions.InvalidS3ObjectException
            Textract.Client.exceptions.UnsupportedDocumentException
            Textract.Client.exceptions.DocumentTooLargeException
            Textract.Client.exceptions.BadDocumentException
            Textract.Client.exceptions.AccessDeniedException
            Textract.Client.exceptions.ProvisionedThroughputExceededException
            Textract.Client.exceptions.InternalServerError
            Textract.Client.exceptions.ThrottlingException
            Textract.Client.exceptions.HumanLoopQuotaExceededException
        """

        if bucket is None or key is None or feature_types is None:
            raise ValueError("bucket, key and feature types are required fields")

        try:
            response = self.client.analyze_document(
                Document={"S3Object": {"Bucket": bucket, "Name": key}},
                FeatureTypes=feature_types,
            )
            return response
        except Exception as err:
            if err.response["Error"]["Code"] in {
                "ThrottlingException",
                "ProvisionedThroughputExceededException",
            }:
                raise Exception("Throttling Error")

            elif err.response["Error"]["Code"] in {
                "UnsupportedDocumentException",
                "DocumentTooLargeException",
                "BadDocumentException",
            }:
                raise ValueError("Do something for Bad documents")
