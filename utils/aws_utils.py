import boto3


def get_folder_names(directory_name, aws_access_key_id, aws_secret_access_key, bucket_name = 'detroit-project-data-bucket'):
    """
    Connect to S3 and read all folder (datapoints) names in the images dataset
    """
    s3_client = boto3.client('s3', 
                             aws_access_key_id=aws_access_key_id, 
                             aws_secret_access_key=aws_secret_access_key)

    paginator = s3_client.get_paginator('list_objects_v2')
    response_iterator = paginator.paginate(
        Bucket=bucket_name,
        Prefix=directory_name,
        Delimiter='/'
    )

    folder_names = []
    for response in response_iterator:
        if response.get('CommonPrefixes') is not None:
            for prefix in response.get('CommonPrefixes'):
                # Extract the folder name from the prefix key
                folder_name = prefix.get('Prefix')
                # Removing the base directory and the trailing slash to get the folder name
                folder_name = folder_name[len(directory_name):].strip('/')
                folder_names.append(folder_name)

    return folder_names

def already_in_dataset(video_id, directory_name, aws_access_key_id, aws_secret_access_key):
    """
    Check if video_id is already in dataset
    
    """
    coords_stored = []
    items = get_folder_names(directory_name, aws_access_key_id, aws_secret_access_key)

    for item in items:
        if item == video_id:
            return 1, len(items)

    return 0, len(items)