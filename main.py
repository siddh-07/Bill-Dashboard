# Fetch Data from G-mail API and save to CSV

import os
import csv
from re import sub
from google.oauth2.credentials import Credentials
from googleapiclient.discovery import build
from google_auth_oauthlib.flow import InstalledAppFlow

# Set up the Gmail API
SCOPES = ['https://www.googleapis.com/auth/gmail.readonly']

def main():
    creds = None
    if os.path.exists('token.json'):
        creds = Credentials.from_authorized_user_file('token.json', SCOPES)
    else:
        creds = authenticate_gmail_api()

    service = build('gmail', 'v1', credentials=creds)

    # Fetch the list of messages
    results = service.users().messages().list(userId='me',maxResults=100).execute()
    messages = results.get('messages', [])

    # Prepare CSV file
    with open('emails.csv', mode='w', newline='', encoding='utf-8') as csv_file:
        fieldnames = ['id', 'threadId', 'labelIds','subject', 'from', 'to', 'date']
        writer = csv.DictWriter(csv_file, fieldnames=fieldnames)
        writer.writeheader()

        # Process each message
        for message in messages:
            msg_id = message['id']
            msg = service.users().messages().get(
                userId='me',
                id=msg_id,
                format='metadata',
                metadataHeaders=['Subject', 'From', 'To', 'Date']
            ).execute()

            headers = msg.get('payload', {}).get('headers', [])

            subject = next((h['value'] for h in headers if h['name'] == 'Subject'), '')
            from_email = next((h['value'] for h in headers if h['name'] == 'From'), '')
            to_email = next((h['value'] for h in headers if h['name'] == 'To'), '')
            date = next((h['value'] for h in headers if h['name'] == 'Date'), '')

            thread_id = message['threadId']
            label_ids = ','.join(message.get('labelIds', []))

            writer.writerow({
                'id': msg_id,
                'threadId': thread_id,
                'labelIds': label_ids,
                'subject': subject,
                'from': from_email,
                'to': to_email,
                'date': date
            })

    print("Data has been saved to emails.csv")
    
def authenticate_gmail_api():

    flow = InstalledAppFlow.from_client_secrets_file(
        'credentials.json',
        SCOPES
    )

    creds = flow.run_local_server(
        port=0,
        access_type='offline',
        prompt='consent'
    )

    with open('token.json', 'w') as token:
        token.write(creds.to_json())

    return creds
    

if __name__ == '__main__':
    main()