# DESCRIPTION
The empty-buckets.py script can be used to empty a list of s3 buckets of objects and object versions.
Buckets with versioning enabled will have versioning disabled during the delete process, and re-enabled once it is complete.

While boto3 will handle pagination completely under the hood, when using methods such as `bucket.objects.all().delete()`, emptying large buckets with this method means the script running for a long period of time without any feedback. To provide ongoing feedback during the delete process, this script iterates through each page using the paginators provided by boto3, providing feedback of the result of deleting the contents of each page. This keeps fetch requests low and mirrors what occurs under the hood with the more abstract delete methods.

# USAGE
## Install Requirements
`pip install -r requirements.txt`
## Setup AWS environment
Make sure you are working in the correct AWS environment by setting the environment variables `AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY` and `AWS_SESSION_TOKEN`. Or, setup your AWS profile for the account you want to manage.
## Empty buckets
`python3 empty-buckets.py bucket-1 bucket-2 bucket-3 ...`

# REQUIREMENTS
boto3