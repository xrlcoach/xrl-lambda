import gspread
from google.oauth2.service_account import Credentials

scope = ["https://spreadsheets.google.com/feeds", 'https://www.googleapis.com/auth/spreadsheets',
            "https://www.googleapis.com/auth/drive.file", "https://www.googleapis.com/auth/drive"]

credentials = Credentials.from_service_account_file('XRL_test.json', scopes=scope)
client = gspread.authorize(credentials)