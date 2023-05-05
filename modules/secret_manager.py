from google.cloud import secretmanager
from google.oauth2.service_account import Credentials

def get_secret(project_id, secret, token_path):
    """
    Function to read secrets from the Google Cloud Secret Manager.

    :param project_id: The project id.
    :type project_id: string
    :param secret: The name of the secret to read.
    :type secret: string
    :param token_path: The path of the token that will connect to Google Cloud project.
    :type token_path: string
    """
    try:
        creds = Credentials.from_service_account_file(token_path)
        client = secretmanager.SecretManagerServiceClient(credentials = creds)
    except FileNotFoundError:
        client = secretmanager.SecretManagerServiceClient()
    secret_path = f'projects/{project_id}/secrets/{secret}/versions/latest'
    try:
        response = client.access_secret_version(request = {'name': secret_path})
        return response.payload.data.decode('UTF-8')
    except Exception as e:
        print('SecretManager Error: ' + e)
        return None