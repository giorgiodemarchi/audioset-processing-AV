import os 
import subprocess
import boto3
import pandas as pd
from io import StringIO

from utils.aws_utils import already_in_dataset

S3_DEST_DIR_KEY = "original_eval/"

# AWS Access
with open('secrets.txt', 'r') as f:
    lines = f.readlines()
    AWS_SECRET_ACCESS_KEY = lines[0].split('=')[1].strip()
    AWS_ACCESS_KEY = lines[1].split('=')[1].strip()
    bucket_name = lines[2].split('=')[1].strip()


df = pd.read_csv('audioset_data/audioset_eval_strong.tsv', sep='\t')
segments_ids = df.segment_id.unique()

def download_video(youtube_id, start_time, end_time, local_temp_dir="temp_output/"):
    """
    Runs commands with yt-dlp and ffmpeg to download an mp4 file and store it temporarily locally
    """
    file_name = youtube_id + ".mp4"
    video_url = "https://www.youtube.com/watch?v=" + youtube_id
    local_file_name = local_temp_dir + file_name
    start_time_str = str(start_time)
    interval = str(end_time - start_time)

    # Get the direct URL of the best audio-video stream using yt-dlp
    process = subprocess.run(["yt-dlp", "-f", "best", "-g", video_url], capture_output=True, text=True)
    direct_url = process.stdout.strip()
    
    # Check if yt-dlp succeeded
    if process.returncode != 0:
        print("yt-dlp failed:", process.stderr)
        return 1
    else:
        command = ["ffmpeg", "-n", "-ss", start_time_str, "-t", interval, "-i", direct_url, local_file_name]
        exit_code = subprocess.run(command).returncode

    return exit_code, local_file_name

def free_local_memory(local_file):
    os.remove(local_file)


def upload_video_to_s3(local_file, s3_path):
    """
    Puts video mp4 object in an ad-hoc folder in the specified s3 path
    """
    s3_key = s3_path + "video.mp4"
    s3 = boto3.client('s3',
                          aws_access_key_id = AWS_ACCESS_KEY,
                          aws_secret_access_key = AWS_SECRET_ACCESS_KEY)
        
    s3.upload_file(local_file, bucket_name, s3_key)


def upload_metadata_to_s3(video_df, s3_path, bucket_name, aws_acess_key, aws_secret_access_key):
    """
    Uploads csv objects containing labels for each strongly-labelled segments
    """
    s3 = boto3.client('s3',
                      aws_access_key_id = aws_acess_key,
                      aws_secret_access_key = aws_secret_access_key)
    
    metadata_file_key = s3_path + 'metadata.csv'
    csv_buffer = StringIO()
    video_df.to_csv(csv_buffer, index=False)
    
    s3.put_object(Bucket=bucket_name, Key=metadata_file_key, Body=csv_buffer.getvalue())
    

if __name__ == '__main__':
    j=0
    for video_url in segments_ids:

        video_df = df[df['segment_id']==video_url]
        start_time = video_df.start_time_seconds.min()
        end_time = video_df.end_time_seconds.max()
        
        already_present, length = already_in_dataset(video_url, bucket_name, S3_DEST_DIR_KEY, AWS_ACCESS_KEY, AWS_SECRET_ACCESS_KEY)
        if j%100==0:
            print(f"{length} points in dataset.")

        if already_present == 0:
            exit_code, local_file = download_video(video_url, start_time, end_time)

            if exit_code == 0:
                # Put objects on S3
                s3_path = "original_eval/" + str(video_url) + "/"
                upload_video_to_s3(local_file, s3_path)
                upload_metadata_to_s3(video_df, s3_path, bucket_name, AWS_ACCESS_KEY, AWS_SECRET_ACCESS_KEY)

                # Free local space
                free_local_memory(local_file)
        else:
            print('already present')
            
            j+=1
        
        if j > 3:
            break
        