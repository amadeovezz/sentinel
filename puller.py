# standard lib
from typing import List, Tuple
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
    tile_id = ''
    REGION = "sentinel-s2-l1c"

    # Tile id
    utm: str
    lat_band: str
    square: str

    s3_client = None

    def __init__(self, config, tile_id: str):
        self.config = config
        self.tile_id = tile_id

        parsed = self.parse_tile_id()
        self.utm, self.lat_band, self.square = parsed[0], parsed[1], parsed[2]

    @staticmethod
    def filter_files(l: List, start: datetime, end: datetime) -> List:
        pass

    def parse_tile_id(self) -> Tuple:
        if len(self.tile_id) != 4:
            if len(self.tile_id) != 5:
                logging.fatal('please enter a valid tile_id...')

        utm = self.tile_id[0:2]
        lat_band = ''
        square = ''
        if utm[1].isalpha():
            utm = self.tile_id[0]
            lat_band = self.tile_id[1]
            square = self.tile_id[2:]
        else:
            utm = self.tile_id[0:2]
            lat_band = self.tile_id[2]
            square = self.tile_id[3:]

        return utm, lat_band,square

    def connect(self):
        session = boto3.Session(
            aws_access_key_id=self.config['id']
            , aws_secret_access_key=self.config['key']
        )

        self.s3_client = boto3.client('s3')
        response = self.s3_client.head_object(Bucket=f'{self.REGION}'
                                              , RequestPayer='requester'
                                              , Key='readme.html')

        if response['ResponseMetadata']['HTTPStatusCode'] != 200:
            logging.fatal(f'Cannot establish connection with bucket: {self.REGION}')

    def pull_images(self) -> np.array:
        # objects = self.s3_client.list_objects(Bucket=self.REGION
        #                                       , Prefix='tiles/'
        #                                       , RequestPayer='requester')
        pass

