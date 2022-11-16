# standard lib
from typing import List, Tuple
from datetime import datetime
from dateutil import parser
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

    # Tile id
    tile_id = ''
    utm: str
    lat_band: str
    square: str

    # Other
    s3_client = None
    start: datetime
    end: datetime

    def __init__(self, config={}, tile_id: str = '', start: str = '', end: str = ''):
        self.config = config
        self.tile_id = tile_id

        parsed = self.parse_tile_id()
        self.utm, self.lat_band, self.square = parsed[0], parsed[1], parsed[2]

        self.start = parser.parse(start)
        self.end = parser.parse(end)

    def filter_s3_files(self, l: List) -> List:
        def is_valid(response_obj):
            if self.start <= response_obj['LastModified'] <= self.end:
                band = response_obj['Key'].split('/')[-1]
                if band in ['B02.jp2', 'B03.jp2', 'B04.jp2']:
                    return True
            return False

        return list(filter(is_valid, l))

    def parse_tile_id(self) -> Tuple:
        if len(self.tile_id) != 4:
            if len(self.tile_id) != 5:
                logging.fatal('please enter a valid tile_id...')

        utm = self.tile_id[0:2]
        if utm[1].isalpha():
            utm = self.tile_id[0]
            lat_band = self.tile_id[1]
            square = self.tile_id[2:]
        else:
            utm = self.tile_id[0:2]
            lat_band = self.tile_id[2]
            square = self.tile_id[3:]

        return utm, lat_band, square

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
            logging.fatal(f'cannot establish connection with bucket: {self.REGION}...')

    def pull_images(self) -> np.array:
        paginator = self.s3_client.get_paginator('list_objects')
        page_iterator = paginator.paginate(Bucket=self.REGION
                                           , Prefix=f'tiles/{self.utm}/{self.lat_band}/{self.square}/'
                                           , RequestPayer='requester'
                                           , PaginationConfig={'MaxItems': 100})

        valid_list = []
        for page in page_iterator:
            valid_list.append(self.filter_s3_files(page['Contents']))
        valid_list
