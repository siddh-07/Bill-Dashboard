import os
import csv
import logging
from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow
from google.auth.transport.requests import Request
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError

# --- CONFIGURATION ---
SCOPES = ['https://www.googleapis.com/auth/gmail.readonly']
CREDENTIALS_FILE = 'credentials.json'
TOKEN_FILE = 'token.json'
OUTPUT_FILE = 'emails.csv'

# Configure Logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

class GmailService:
    def __init__(self):
        self.creds = None
        self.service = None
        self.authenticate()
        
    def authenticate(self):
        """
        Handles OAuth2 authentication and token management.
        Security: Uses local token storage to avoid repeated logins.
        """
        try:
            # Load existing tokens if they exist
            if os.path.exists(TOKEN_FILE):
                self.creds = Credentials.from_authorized_user_file(TOKEN_FILE, SCOPES)

            # Refresh or initiate new login if credentials are invalid/expired
            if not self.creds or not self.creds.valid:
                if self.creds and self.creds.expired and self.creds.refresh_token:
                    logging.info("Refreshing expired access token...")
                    self.creds.refresh(Request())
                else:
                    if not os.path.exists(CREDENTIALS_FILE):
                        raise FileNotFoundError(f"Missing {CREDENTIALS_FILE}. Please download it from Google Cloud Console.")
                    
                    logging.info("Initiating new authentication flow...")
                    flow = InstalledAppFlow.from_client_secrets_file(CREDENTIALS_FILE, SCOPES)
                    self.creds = flow.run_local_server(port=0)

                # Save the credentials for the next run
                with open(TOKEN_FILE, 'w') as token:
                    token.write(self.creds.to_json())

            self.service = build('gmail', 'v1', credentials=self.creds)
            logging.info("Gmail API Service initialized successfully.")

        except Exception as e:
            logging.error(f"Authentication Failed: {e}")
            raise

    def get_emails(self, max_results=100):
        """
        Fetches a list of messages from the user's account.
        """
        if not self.service:
            raise Exception("Gmail service not initialized. Call authenticate() first.")
        
        try:
            logging.info(f"Fetching last {max_results} messages...")
            
            query = (
    '('
    'subject:("application for" OR "thank you for applying" OR '
    '"application received" OR "interview" OR "assessment" OR '
    '"offer" OR "regarding your application")'
    ')'
    ' AND '
    '('
    'from:(careers OR hr OR recruitment OR gov OR talent OR hiring)'
    ')'
    ' -subject:("job alert" OR "job alerts" OR "recommended jobs" OR "linkedin news")'
)
            results = self.service.users().messages().list(userId='me', maxResults=max_results,q= query).execute()
            return results.get('messages', [])
        except HttpError as error:
            logging.error(f"An HTTP error occurred: {error}")
            return []

    def get_message_details(self, msg_id):
        """
        Retrieves metadata for a specific message ID.
        """
        if not self.service:
            raise Exception("Gmail service not initialized.")
        
        try:
            msg = self.service.users().messages().get(
                userId='me',
                id=msg_id,
                format='metadata',
                metadataHeaders=['Subject', 'From', 'To', 'Date']
            ).execute()
            return msg
        except HttpError as error:
            logging.warning(f"Could not retrieve message {msg_id}: {error}")
            return None

    def process_and_save(self, messages):
        """
        Parses headers and saves data to CSV with error handling for file IO.
        """
        fieldnames = ['id', 'threadId', 'labelIds', 'subject', 'from', 'to', 'date']
        
        try:
            with open(OUTPUT_FILE, mode='w', newline='', encoding='utf-8') as csv_file:
                writer = csv.DictWriter(csv_file, fieldnames=fieldnames)
                writer.writeheader()

                for message in messages:
                    msg_id = message['id']
                    msg_data = self.get_message_details(msg_id)
                    
                    if not msg_data:
                        continue

                    headers = msg_data.get('payload', {}).get('headers', [])
                    
                    # Helper function to extract specific header values safely
                    def get_header(name):
                        return next((h['value'] for h in headers if h['name'] == name), 'N/A')

                    writer.writerow({
                        'id': msg_id,
                        'threadId': message.get('threadId'),
                        'labelIds': ','.join(msg_data.get('labelIds', [])),
                        'subject': get_header('Subject'),
                        'from': get_header('From'),
                        'to': get_header('To'),
                        'date': get_header('Date')
                    })
            logging.info(f"Successfully saved data to {OUTPUT_FILE}")
            
        except IOError as e:
            logging.error(f"File Error: Could not write to {OUTPUT_FILE}. Is it open in another program? {e}")

def main():
    gmail_app = GmailService()
    
    try:
        gmail_app.authenticate()
        messages = gmail_app.get_emails(max_results=10)
        
        if not messages:
            logging.info("No messages found.")
            return

        gmail_app.process_and_save(messages)
        
    except Exception as e:
        logging.critical(f"Application failed: {e}")

if __name__ == '__main__':
    main()