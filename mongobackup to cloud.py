import json
import os
from pymongo import MongoClient
from datetime import datetime
import time
from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow
from googleapiclient.discovery import build
from googleapiclient.http import MediaFileUpload

# MongoDB connection settings
MONGO_URI = "mongodb://192.168.0.99:27017/"  # Replace with your MongoDB URI
DATABASE_NAME = "scheq"  # Replace with your database name

# Google Drive folder ID where backups will be uploaded
GDRIVE_FOLDER_ID = '1_wDAMoZ7PZwt4DpoDf17Unn4kX_Zafc_'  # Replace with your Google Drive folder ID

# OAuth 2.0 authentication scopes for Google Drive API
SCOPES = ['https://www.googleapis.com/auth/drive.file']


# Google Drive service initialization
def authenticate_gdrive():
    """Authenticate and create a Google Drive service."""
    creds = None
    # The file token.json stores the user's access and refresh tokens.
    # It is created automatically when the authorization flow completes for the first time.
    if os.path.exists('token.json'):
        creds = Credentials.from_authorized_user_file('token.json', SCOPES)

    if not creds or not creds.valid:
        if creds and creds.expired and creds.refresh_token:
            creds.refresh(Request())
        else:
            flow = InstalledAppFlow.from_client_secrets_file(
                'credentials.json', SCOPES)
            creds = flow.run_local_server(port=0)

        # Save the credentials for the next run
        with open('token.json', 'w') as token:
            token.write(creds.to_json())

    return build('drive', 'v3', credentials=creds)


def json_serializer(obj):
    """Custom JSON serializer for non-serializable types."""
    if isinstance(obj, datetime):
        return obj.isoformat()  # Convert datetime to ISO 8601 string format
    raise TypeError(f"Type {type(obj)} not serializable")


def export_database_to_json(mongo_uri, database_name, output_file):
    try:
        # Connect to MongoDB
        client = MongoClient(mongo_uri)
        db = client[database_name]

        # Initialize a dictionary to store the database content
        database_content = {}

        # Iterate through all collections in the database
        for collection_name in db.list_collection_names():
            collection = db[collection_name]
            documents = list(collection.find())

            # Convert ObjectId to string for JSON serialization
            for document in documents:
                document["_id"] = str(document["_id"])

            # Add the collection's documents to the database content
            database_content[collection_name] = documents

        # Write the database content to a JSON file
        with open(output_file, "w", encoding="utf-8") as file:
            json.dump(database_content, file, indent=4, ensure_ascii=False, default=json_serializer)

        print(f"Database '{database_name}' exported successfully to {output_file}")
    except Exception as e:
        print(f"Error exporting database: {e}")


def upload_to_gdrive(local_file, folder_id):
    """Upload a file to Google Drive."""
    try:
        service = authenticate_gdrive()

        # Create a MediaFileUpload instance
        media = MediaFileUpload(local_file, mimetype='application/json')

        # Create the metadata for the file
        file_metadata = {'name': os.path.basename(local_file), 'parents': [folder_id]}

        # Upload the file
        file = service.files().create(body=file_metadata, media_body=media, fields='id').execute()

        print(f"Backup uploaded to Google Drive: {file['id']}")
    except Exception as e:
        print(f"Error uploading backup to Google Drive: {e}")


def backup_mongo_database():
    """Take a backup of the MongoDB database every hour."""
    while True:
        # Generate the output file name with the current timestamp
        timestamp = datetime.now().strftime("%Y-%m-%dT%H-%M-%S")
        output_file = f"scheq_backup_{timestamp}.json"

        # Export the database to a JSON file
        export_database_to_json(MONGO_URI, DATABASE_NAME, output_file)

        # Upload the backup to Google Drive
        upload_to_gdrive(output_file, GDRIVE_FOLDER_ID)

        # Wait for 1 hour before the next backup
        time.sleep(3600)


if __name__ == "__main__":
    backup_mongo_database()
