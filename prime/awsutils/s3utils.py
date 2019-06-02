import boto3
from botocore.exceptions import ClientError, NoCredentialsError
import csv
import sys
from awsutils.settings import settings


def create_s3_resource():
    print(
        "\nRUNNING SOLUTION CODE:",
        "create_s3_resource!")
    s3 = boto3.resource('s3')
    return s3


def upload_file_to_bucket(file, bucket, key=None):
    print(
        "\nRUNNING SOLUTION CODE:",
        "upload_file_to_bucket!")
    if key is None:
        key = file
    bucket.upload_file(file, key)


def download_file_from_bucket(bucket, key):
    print(
        "\nRUNNING SOLUTION CODE:",
        "download_file_from_bucket!")
    bucket.download_file(key, key)


def setup_bucket(s3, bucket, region, create=False):
    exists = True
    try:
        # Check if you have permissions to access the bucket
        s3.meta.client.head_bucket(Bucket=bucket)
        # Delete any existing objects in the bucket
        # s3.Bucket(bucket).objects.delete()
    except NoCredentialsError as e:
        print("Error: " + e.response['Error']['Code'] +
              " " + e.response['Error']['Message'])
        sys.exit()
    except ClientError as e:
        error_code = int(e.response['Error']['Code'])
        if error_code == 404:
            exists = False
            if create:
                # Bucket does not exist, so create it.
                # Do not specify a LocationConstraint if the region is us-east-1 -
                # S3 does not like this!!
                create_bucket_config = {}
                if region != "us-east-1":
                    create_bucket_config["LocationConstraint"] = region
                    s3.create_bucket(Bucket=bucket, CreateBucketConfiguration=create_bucket_config)
                else:
                    s3.create_bucket(Bucket=bucket)
                print('Created bucket: ' + bucket)
        else:
            print("Specify a unique bucket name. Bucket names can contain lowercase letters, numbers, and hyphens.")
            print(
                "It is possible that a bucket with the name '" +
                bucket +
                "' already exists. You may not have permissions to access the bucket.")
            print("Error: " + e.response['Error']['Code'] +
                  " " + e.response['Error']['Message'])
            sys.exit()


class S3Handler:

    s3 = None
    input_bucket = None
    output_bucket = None

    def __init__(self, input_bucket_name=settings["s3"]["input_bucket"],
                 output_bucket_name=settings["s3"]["output_bucket"]):
        self.input_bucket_name = input_bucket_name
        self.output_bucket_name = output_bucket_name

        # Set the region in which the lab is running
        self.region_name = boto3.session.Session().region_name

        # Create S3 resource
        self.s3 = create_s3_resource()

        # Set up the input.py bucket and copy the CSV files. Also, set up the
        # output bucket
        if self.input_bucket_name is not None:
            setup_bucket(self.s3, bucket=self.input_bucket_name, region=self.region_name)
            self.input_bucket = self.s3.Bucket(self.input_bucket_name)

        if self.output_bucket_name is not None:
            setup_bucket(self.s3, bucket=self.output_bucket_name, region=self.region_name, create=True)
            self.output_bucket = self.s3.Bucket(self.output_bucket_name)

    def read_input_bucket(self, headings=True):
        # this assumes data is csv files with headers
        # Get summary information for all objects in input.py bucket
        # Iterate over the list of object summaries
        reader = dict()
        for object_summary in self.input_bucket .objects.all():
            # Get the object key from each object summary
            csvkey = object_summary.key

            # Retrieve the object with the specified key from the input.py bucket
            download_file_from_bucket(self.input_bucket, csvkey)

            # Convert the file from CSV to JSON format
            file_name = file.split('.')[0]
            file = open(csvkey, 'r')

            # Get the headings
            if headings:
                heading = file.readlines(1)[0].split(',')
                reader[file_name] = csv.DictReader(file, fieldnames=heading)
            else:
                reader[file_name] = csv.DictReader(file)

    def write_output_bucket(self, file, filename=None):
        upload_file_to_bucket(file, self.output_bucket, filename)






