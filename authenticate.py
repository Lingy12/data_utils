from google_auth_oauthlib.flow import Flow
from google.auth.transport.requests import Request
import os

def main():
    # Define the scope for the request
    SCOPES = ['https://www.googleapis.com/auth/gmail.send']

    # Create the flow using the client_secrets.json file
    flow = Flow.from_client_secrets_file(
        '/home/geyu/credentials.json',
        scopes=SCOPES,
        redirect_uri='urn:ietf:wg:oauth:2.0:oob')

    # Generate the authorization URL
    auth_url, _ = flow.authorization_url(prompt='consent')

    print("Please go to this URL and authorize access:")
    print(auth_url)

    # Ask for the authorization code
    code = input("Enter the authorization code here: ")

    # Exchange the authorization code for an access token
    flow.fetch_token(code=code)

    # Save the credentials for later use
    creds = flow.credentials
    with open('token.json', 'w') as token:
        token.write(creds.to_json())

    print("Authentication successful.")

if __name__ == "__main__":
    main()

