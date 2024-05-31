# #%%
# import boto3
# import pyart
# from datetime import datetime

# # Define the S3 bucket and the specific date
# bucket_name = "noaa-nexrad-level2"
# date = datetime(2022, 3, 22)
# station = "KHGX"

# # Initialize boto3 client for S3
# s3 = boto3.client('s3')

# # Format the prefix to list files for the specific date and station
# prefix = f"{date.year}/{date.month:02d}/{date.day:02d}/{station}/"

# # List files in the specified S3 bucket and prefix
# response = s3.list_objects_v2(Bucket=bucket_name, Prefix=prefix)

# # Check if any files were found
# if 'Contents' in response:
#     file_keys = [content['Key'] for content in response['Contents']]
# else:
#     raise ValueError(f"No files found for date {date} at station {station}")

# # Read each file using Py-ART
# radar_objects = []
# for file_key in file_keys:
#     file_url = f"s3://{bucket_name}/{file_key}"
#     radar = pyart.io.read_nexrad_archive(file_url)
#     radar_objects.append(radar)

# # Now radar_objects contains Py-ART Radar objects for all files of the day
# # You can process these objects as needed

# # %%
