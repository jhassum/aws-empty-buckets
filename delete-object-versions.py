import argparse
import boto3

parser = argparse.ArgumentParser(description='Delete object versions from bucket.')
parser.add_argument("bucket", type=str, help='bucket name')
parser.add_argument('--list-objects', help='list result of each delete', action="store_true")
parser.add_argument('--verbose', help='show steps', action="store_true")

args = parser.parse_args()
BUCKET_NAME = args.bucket
verbose = args.verbose
list_objects = args.list_objects

print("Emptying Bucket '" + BUCKET_NAME + "'")

# Create a client
client = boto3.client('s3')
s3 = boto3.resource('s3')
# Create a reusable Paginator
paginator = client.get_paginator('list_object_versions')
bucket = s3.Bucket(BUCKET_NAME)

# Create a PageIterator from the Paginator
page_iterator = paginator.paginate(Bucket=BUCKET_NAME)

count_page = 0
count_deleted = 0
errored = []
for page in page_iterator:
  count_page += 1
  # Compile Objects to Delete
  if verbose:
    print("Compile Objects...")
  else:
    print(".", end="\r", flush=True)
  versions = [{"Key": v["Key"], "VersionId": v["VersionId"]} for v in page.get("Versions", [])]
  delete_markers = [{"Key": v["Key"], "VersionId": v["VersionId"]} for v in page.get("DeleteMarkers", [])]
  objects = versions + delete_markers
  
  # Delete Objects
  if verbose:
    print("Delete " + str(len(objects)) + " Objects (" + str(len(versions)) + " Versions, " + str(len(delete_markers)) + " DeleteMarkers)...")
  else:
    print("..", end="\r", flush=True)
  res = bucket.delete_objects(Delete={
    'Objects': objects
  })
  
  # Compile Results
  if verbose:
    print("Compile Results...")
  else:
    print("...", end="\r", flush=True)
  count_deleted += len(res.get("Deleted", []))
  errored.extend(res.get("Errors", []))
  print("Page: " + str(count_page) + ", Deleted: " + str(count_deleted) + ", Errored: " + str(len(errored)))
  if list_objects:
    for o in res.get("Deleted", []):
      print("DELETED: " + str(o))
    for e in res.get("Errors", []):
      print("ERROR: " + str(e))

print("Errors:")
for e in errored:
  print(e)