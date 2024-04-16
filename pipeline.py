import pandas as pd
import os 

from utils.aws_utils import already_in_dataset
from utils.utils import download_video, upload_metadata_to_s3, upload_video_to_s3, free_local_memory

if __name__ == '__main__':
    S3_DEST_DIR_KEY = "train_strong/"
    SET = 'train'

    # AWS Access
    AWS_SECRET_ACCESS_KEY = os.environ.get("AWS_SECRET_ACCESS_KEY")
    AWS_ACCESS_KEY = os.environ.get("AWS_ACCESS_KEY")
    bucket_name = os.environ.get("BUCKET_NAME")

    # Download loop
    df = pd.read_csv(f'audioset_data/audioset_{SET}_strong.tsv', sep='\t')
    segments_ids = df.segment_id.unique()
    j=0
    errors = 0
    for i, video_url in enumerate(segments_ids):
 
        try:
            video_df = df[df['segment_id']==video_url]
            start_time = int(video_url.split('_')[-1])/1000
            end_time = start_time + video_df.end_time_seconds.max()
            
            already_present, length = already_in_dataset(video_url, bucket_name, S3_DEST_DIR_KEY, AWS_ACCESS_KEY, AWS_SECRET_ACCESS_KEY)

            if j%100==0:
                print(f"{length} points in dataset.")

            if already_present == 0:
                exit_code, local_file = download_video(video_url, start_time, end_time)

                if exit_code == 0:
                    # Put objects on S3
                    s3_path = S3_DEST_DIR_KEY + str(video_url) + "/"
                    upload_video_to_s3(local_file, s3_path, bucket_name, AWS_ACCESS_KEY, AWS_SECRET_ACCESS_KEY)
                    upload_metadata_to_s3(video_df, s3_path, bucket_name, AWS_ACCESS_KEY, AWS_SECRET_ACCESS_KEY)

                    # Free local space
                    free_local_memory(local_file)
                    
                    j+=1
            else:
                print('Already present')

        except Exception as e:
            print(f"Download/Upload failed for id: {video_url}")
            print(e)
            errors +=1 
            print(errors)
        

        if j > 15000:
            break