from storage_workflows.crdb.factory.aws_session_factory import AwsSessionFactory
from storage_workflows.logging.logger import Logger

logger = Logger()


class S3Gateway:

    @staticmethod
    def read_objects_with_pagination(bucket_name, page_token=None, page_size=1):
        """
        Read objects from an S3 bucket using pagination with list_objects_v2.

        Args:
        - bucket_name (str): The name of the S3 bucket.
        - page_token (str, optional): The page token for paginated results.
        - page_size (int, optional): The maximum number of objects to retrieve in each page.

        Returns:
        - objects (list): A list of objects in the current page.
        - next_page_token (str): A pointer to the next page or None if there are no more pages.
        """

        # Create an S3 client
        s3_client = AwsSessionFactory.s3()

        # Define the parameters for the S3 list_objects_v2 call
        params = {
            'Bucket': bucket_name,
            'MaxKeys': page_size,
        }

        # If a page token is provided, set it in the parameters
        if page_token:
            params['ContinuationToken'] = page_token

        # Use list_objects_v2 to list objects
        response = s3_client.list_objects_v2(**params)

        # Extract the objects and the next page token
        objects = response.get('Contents', [])
        next_page_token = response.get('NextContinuationToken')

        return objects, next_page_token

    @staticmethod
    def read_object_contents(bucket_name, key):
        # Create an S3 client
        s3_client = AwsSessionFactory.s3()
        try:
            # Get the object data from S3
            response = s3_client.get_object(Bucket=bucket_name, Key=key)

            # Read and process the contents of the object
            object_contents = response['Body'].read()

            logger.info(f"Contents of {key}:")
            logger.info(object_contents.decode('utf-8'))
            return object_contents.decode('utf-8')

        except Exception as e:
            logger.error(f"Error processing object {key}: {str(e)}")
