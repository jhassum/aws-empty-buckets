import argparse
import boto3

parser = argparse.ArgumentParser(description='Delete object versions from bucket.')
parser.add_argument("buckets", type=str, nargs='+', help='bucket name')
parser.add_argument('--list-objects', help='list result of each delete', action="store_true")
parser.add_argument('--verbose', help='show steps', action="store_true")

args = parser.parse_args()
buckets = args.buckets
verbose = args.verbose
list_objects = args.list_objects


# Create a client
client = boto3.client('s3')
s3 = boto3.resource('s3')
# Create a reusable Paginator
objects_paginator = client.get_paginator('list_objects')
object_versions_paginator = client.get_paginator('list_object_versions')

for bucket_name in buckets:
  print("Emptying Bucket '" + bucket_name + "'")
  bucket = s3.Bucket(bucket_name)
  bucket_versioning = bucket.Versioning()
  original_versioning = bucket_versioning.status
  if original_versioning == "Enabled":
    print("Suspend bucket versioning.")
    bucket_versioning.suspend()
  else:
    print("bucket versioning not enabled.")
  # Create a PageIterator from the Paginator
  objects_page_iterator = objects_paginator.paginate(Bucket=bucket_name)
  object_versions_page_iterator = object_versions_paginator.paginate(Bucket=bucket_name)
  
  # Delete All Objects
  print("Delete all objects.")
  count_page = 0
  count_deleted = 0
  errored = []
  for page in objects_page_iterator:
    count_page += 1
    # Compile Objects to Delete
    if verbose:
      print("Compile Objects...")
    else:
      print(".", end="\r", flush=True)
    objects = [{"Key": o["Key"]} for o in page.get("Contents", [])]
    
    # Delete Objects
    if verbose:
      print("Delete " + str(len(objects)) + " Objects...")
    else:
      print("..", end="\r", flush=True)
    if len(objects) > 0:
      res = bucket.delete_objects(Delete={
        'Objects': objects
      })
    else:
      res = {}
      
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
  
  # Delete All Versions
  print("Delete all object versions.")
  count_page = 0
  count_deleted = 0
  errored = []
  for page in object_versions_page_iterator:
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
    if len(objects) > 0:
      res = bucket.delete_objects(Delete={
        'Objects': objects
      })
    else:
      res = {}
      
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
  
  # Restore bucket versioning
  if original_versioning == "Enabled":
    print("Re-enable bucket versioning.")
    bucket_versioning.enable()
    
  print("Emptied Bucket " + bucket_name)
