from storage_workflows.crdb.factory.aws_session_factory import AwsSessionFactory


class S3Gateway:

    # @staticmethod
    # def list_all_bucket_contents(bucket_name):
    #     s3_client = AwsSessionFactory.s3()
    #     response = s3_client.list_objects_v2(Bucket=bucket_name)
    #
    #     # Check if there are any objects in the bucket
    #     if 'Contents' in response:
    #         # Loop through the objects and read their contents
    #         for obj in response['Contents']:
    #             object_key = obj['Key']
    #             # Download and read the object's content
    #             try:
    #                 response = s3_client.get_object(Bucket=bucket_name, Key=object_key)
    #                 content = response['Body'].read().decode('utf-8')
    #
    #                 print(f"Contents of {object_key}:\n{content}")
    #             except Exception as e:
    #                 print(f"Error reading {object_key}: {str(e)}")
    #     else:
    #         print(f"The bucket {bucket_name} is empty.")

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

