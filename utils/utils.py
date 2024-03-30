import os 
import subprocess
import boto3
from io import StringIO


def download_video(youtube_id, start_time, end_time, local_temp_dir="temp_output/"):
    """
    Runs commands with yt-dlp and ffmpeg to download an mp4 file and store it temporarily locally
    
    Sometimes there's this error:
    ERROR: [youtube] video_id: Private video. Sign in if you've been granted access to this video
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


def upload_video_to_s3(local_file, s3_path, bucket_name, aws_access_key, aws_secret_access_key):
    """
    Puts video mp4 object in an ad-hoc folder in the specified s3 path
    """
    s3_key = s3_path + "video.mp4"
    s3 = boto3.client('s3',
                          aws_access_key_id = aws_access_key,
                          aws_secret_access_key = aws_secret_access_key)
        
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