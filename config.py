import boto3
from dotenv import load_dotenv
import os

load_dotenv()

acess_key = os.getenv("ACESS_KEY")
secret_key = os.getenv("ACESS_KEY")

sts = boto3.client("sts",aws_access_key_id = acess_key, aws_secret_access_key = secret_key)

response = sts.get_session_token()

credentials = response["Credentials"]

print(credentials["AccessKeyId"])
print(credentials["SecretAccessKey"])
print(credentials["SessionToken"])