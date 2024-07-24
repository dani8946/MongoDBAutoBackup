import os
import datetime
import pymongo
import boto3
from botocore.exceptions import NoCredentialsError

# MongoDB connection details
MONGO_URI = 'your_mongodb_uri'
DATABASE_NAME = 'your_database_name'

# AWS S3 details
AWS_ACCESS_KEY = 'your_aws_access_key'
AWS_SECRET_KEY = 'your_aws_secret_key'
BUCKET_NAME = 'your_bucket_name'

def backup_mongodb():
    client = pymongo.MongoClient(MONGO_URI)
    db = client[DATABASE_NAME]

    # Create backup directory if it doesn't exist
    backup_dir = './mongodb_backups'
    if not os.path.exists(backup_dir):
        os.makedirs(backup_dir)

    # Generate backup file name
    backup_file = f"{backup_dir}/{DATABASE_NAME}_backup_{datetime.datetime.now().strftime('%Y%m%d%H%M%S')}.bson"

    # Run the mongodump command to create backup
    os.system(f"mongodump --uri={MONGO_URI} --archive={backup_file} --gzip")

    # Upload to S3
    upload_to_s3(backup_file)

    client.close()

def upload_to_s3(file_name):
    s3 = boto3.client('s3', aws_access_key_id=AWS_ACCESS_KEY, aws_secret_access_key=AWS_SECRET_KEY)
    try:
        s3.upload_file(file_name, BUCKET_NAME, os.path.basename(file_name))
        print("Backup uploaded successfully to S3")
    except FileNotFoundError:
        print("The file was not found")
    except NoCredentialsError:
        print("Credentials not available")

if __name__ == '__main__':
    backup_mongodb()
