# standard lib
from typing import List
from datetime import datetime
import logging

# 3rd party
import numpy as np
import boto3


class SatPuller:

    def connect(self):
        raise NotImplemented

    def pull_images(self) -> np.array:
        raise NotImplemented


class S3Puller(SatPuller):
    config = {}

    REGION = "sentinel-s2-l1c"
    s3_client = None

    def __init__(self, config, tile_id: str):

        self.config = config

    def connect(self):
        session = boto3.Session(
            aws_access_key_id=self.config['id']
            , aws_secret_access_key=self.config['key']
        )

        self.s3_client = boto3.client('s3')
        response = self.s3_client.head_object(Bucket=f'{self.REGION}'
                                              , RequestPayer='requester'
                                              , Key='readme.html')

        if response['ResponseMetadata']['HTTPStatusCode'] == 200:
            logging.fatal(f'Cannot establish connection with bucket: {self.REGION}')

    def pull_images(self) -> np.array:
        objects = self.s3_client.list_objects(Bucket=self.REGION
                                              , Prefix='tiles/'
                                              , RequestPayer='requester')
        pass

    @staticmethod
    def filter_files(l: List, start: datetime, end: datetime) -> List:
        pass
