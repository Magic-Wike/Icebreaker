"""
Contains all functions and calls to Google API. Used primarily for creating and accessing files in Google Drive.

Creates public Google Maps Search results csv in Drive for upload to Phantombuster.
Will eventuially add function to pull UGP customer csvs from Drive location TBD 
"""

import os
import yaml
from google.oauth2.credentials import Credentials
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
from googleapiclient.http import MediaFileUpload
import pandas as pd
import requests

# Load the authentication settings from the settings.yaml file
with open('settings.yaml', 'r') as f:
    settings = yaml.safe_load(f)

# Create a Credentials object from the authentication settings
creds = Credentials.from_authorized_user_info(info=settings)

# Create a Google Drive API client
drive = build('drive', 'v3', credentials=creds)

def print_recent_files():
    """prints user's 10 most recent files and their file_ids"""
    results = drive.files().list(pageSize=10, fields="nextPageToken, files(id, name)").execute()
    items = results.get('files', [])
    if not items:
        print('No files found.')
    else:
        print('Files:')
        for item in items:
            print(f'{item["name"]} ({item["id"]})')

def get_csv_for_phantombuster(search_urls, TAG):
    """creates a csv file from get_urls results (list of search urls), uploads to Google Drive,
       sets sharing settings to public, and returns public link to .csv"""
    df = pd.DataFrame(search_urls)

    # Create a CSV file from the DataFrame
    filename = f'{TAG}_urls.csv'
    csv_file = f'search_urls/{filename}'
    df.to_csv(csv_file, index=False)

    # Upload the CSV file to Google Drive and set the sharing settings to public
    print('\nCreating file in Google Drive...')
    try:
        service = build('drive', 'v3', credentials=creds)
        folder_id = '1xb70HHln-Bw27f9HPJaD-BvDVBJ-esJ_'
        file_metadata = {'name': filename, 'parents': [folder_id], 'mimeType': 'application/vnd.google-apps.spreadsheet', 'allowFileDiscovery': False}
        media = MediaFileUpload(csv_file, mimetype='text/csv')
        file = service.files().create(body=file_metadata, media_body=media, fields='id').execute()
        print('\nSetting file permissions to public...')
        permission = {'type': 'anyone', 'role': 'writer'}
        service.permissions().create(fileId=file['id'], body=permission).execute()
        file_link = f'https://docs.google.com/spreadsheets/d/{file["id"]}/edit?usp=sharing'
        print(f'File "{csv_file}" uploaded to Google Drive at {file_link}')
        return (file_link, filename)
    except HttpError as error:
        print(f'An error occurred: {error}')

# print_recent_files()