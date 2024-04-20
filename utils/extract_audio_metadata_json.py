import boto3
import json
import os
from moviepy.editor import VideoFileClip
from tempfile import NamedTemporaryFile

# Constants
BUCKET_NAME = os.environ.get('BUCKET_NAME')
AWS_ACCESS_KEY = os.environ.get('AWS_ACCESS_KEY')
AWS_SECRET_ACCESS_KEY = os.environ.get('AWS_SECRET_ACCESS_KEY')
PREFIX = 'train_strong/'  # Folder structure in S3, leave as '' if not needed

# Initialize S3 client
s3_client = boto3.client('s3',aws_access_key_id=AWS_ACCESS_KEY, aws_secret_access_key=AWS_SECRET_ACCESS_KEY)

def list_files(bucket, prefix):
    """ Generate objects in an S3 bucket. """
    paginator = s3_client.get_paginator("list_objects_v2")
    for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
        for obj in page.get('Contents', []):
            yield obj['Key']

def process_file(key):
    """ Process an individual file to extract metadata. """
    # Download the file temporarily
    with NamedTemporaryFile(suffix=".mp4", delete=True) as tmp:
        s3_client.download_file(Bucket=BUCKET_NAME, Key=key, Filename=tmp.name)
        # Extract audio metadata
        clip = VideoFileClip(tmp.name)
        audio = clip.audio
        return {
            "path": key,
            "duration": audio.duration,
            "sample_rate": audio.fps,
            "channels": audio.nchannels,
            "amplitude": None,  
            "weight": None,     
            "info_path": None   
        }

def create_metadata_file():
    """ Create a metadata JSONL file from S3 bucket data. """
    keys = list_files(BUCKET_NAME, PREFIX)
    with open("data.jsonl", "w") as f:
        i=0
        for key in keys:
            i+=1
            if key.endswith('.mp4'):
                metadata = process_file(key)
                json_record = json.dumps(metadata)
                f.write(json_record + "\n")
            if i%100==0:
                print(i)
            if i>2000:
                break
        
# Execute the function to create the metadata file
create_metadata_file()