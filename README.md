# audioset-processing-AV
Toolkit for downloading audio-video pairs from the Google Audioset dataset

The pipeline includes:
- Reading IDs and labels from the audioset tsv files
- Downloading the audio-video pair to a temporary local file (ffmpeg + yt-dlp)
- Loading files on an AWS S3 bucket (boto3)
