import boto3
from botocore.exceptions import BotoCoreError, ClientError
from dotenv import load_dotenv
import os

load_dotenv()

acess_key = os.getenv("ACESS_KEY")
secret_key = os.getenv("ACESS_KEY")



class AWSClient:
    def __init__(self, service_name, region_name="us-east-2"):
        self.session = boto3.Session()
        self.region = region_name
        self.client = self.session.client(service_name, region_name=region_name, aws_access_key_id = acess_key, aws_secret_access_key = secret_key )
        # Atributo privado para cache
        self._account_id = None

    @property
    def account_id(self):
        """Permite acessar como self.account_id em vez de self.get_account_id()"""
        if self._account_id is None:
            sts = self.session.client("sts")
            self._account_id = sts.get_caller_identity()["Account"]
        return self._account_id