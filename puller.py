# standard lib
from typing import List, Tuple, Dict
from datetime import datetime
from dateutil import parser
import logging
import os
from concurrent import futures

# 3rd party
import numpy as np
import boto3
import botocore


class SatPuller:

    def connect(self):
        raise NotImplemented

    def pull_images(self) -> int:
        raise NotImplemented


class S3Puller(SatPuller):
    config = {}
    BUCKET = "sentinel-s2-l1c"

    # Tile id
    tile_id = ''
    utm: str
    lat_band: str
    square: str

    # Other
    s3_client = None
    start: datetime
    end: datetime

    # File paths to store bandschannels
    red_band_path = ''
    green_band_path = ''
    blue_band_path = ''

    BAND_MAPPING = {
        'red': 'B04.jp2'
        , 'green': 'B03.jp2'
        , 'blue': 'B02.jp2'
    }

    def __init__(self
                 , config={}
                 , tile_id: str = ''
                 , start: str = ''
                 , end: str = ''
                 , red_band_path: str ='./tmp/red/'
                 , green_band_path: str ='./tmp/green/'
                 , blue_band_path: str ='./tmp/blue/'
                 ):
        self.config = config
        self.tile_id = tile_id

        parsed = self.parse_tile_id()
        self.utm, self.lat_band, self.square = parsed[0], parsed[1], parsed[2]

        self.start = parser.parse(start)
        self.end = parser.parse(end)

        self.red_band_path = red_band_path
        self.green_band_path = green_band_path
        self.blue_band_path = blue_band_path

    def filter_s3_files(self, l: List, band: str) -> List:
        def is_valid(response_obj):
            # Ignore preview directory
            if 'preview' not in response_obj['Key']:
                if self.start <= response_obj['LastModified'] <= self.end:
                    file_band = response_obj['Key'].split('/')[-1]
                    if file_band == self.BAND_MAPPING[band]:
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

        # Create one session
        session = boto3.Session(
            aws_access_key_id=self.config['id']
            , aws_secret_access_key=self.config['key']
        )

        # Clients are thread safe
        botocore_config = botocore.config.Config(max_pool_connections=50)
        self.s3_client = boto3.client('s3', config=botocore_config)

        response = self.s3_client.head_object(Bucket=f'{self.BUCKET}'
                                              , RequestPayer='requester'
                                              , Key='readme.html')

        if response['ResponseMetadata']['HTTPStatusCode'] != 200:
            logging.fatal(f'cannot establish connection with bucket: {self.BUCKET}...')
        logging.info(f'successfully established connection with bucket: {self.BUCKET}...')

    @staticmethod
    def flatten(l: List) -> List:
        return [item for sublist in l for item in sublist]

    @staticmethod
    def create_file_name(path: str, date: datetime) -> str:
        l = path.split('/')
        return f'{l[7]}-{l[4]}-{l[5]}-{l[6]}-{date.hour}-{l[8]}'

    def find_s3_file_paths(self) -> Dict:
        logging.info('searching for files in s3...')
        paginator = self.s3_client.get_paginator('list_objects')
        page_iterator = paginator.paginate(Bucket=self.BUCKET
                                           , Prefix=f'tiles/{self.utm}/{self.lat_band}/{self.square}/'
                                           , RequestPayer='requester'
                                           , PaginationConfig={'PageSize': 1000})

        red_channel_paths = []
        green_channel_paths = []
        blue_channel_paths = []
        for page in page_iterator:
            red_channel_paths.append(self.filter_s3_files(page['Contents'], band='red'))
            green_channel_paths.append(self.filter_s3_files(page['Contents'], band='green'))
            blue_channel_paths.append(self.filter_s3_files(page['Contents'], band='blue'))

        logging.info(f'found {len(self.flatten(red_channel_paths))} files for each band...')
        return {
            'red': self.flatten(red_channel_paths)
            , 'green': self.flatten(green_channel_paths)
            , 'blue': self.flatten(blue_channel_paths)
        }

    def download_image(self,s3_client, s3_file_path: str, download_path: str):
        logging.info(f'downloading {s3_file_path}...')
        s3_client.download_file(self.BUCKET
                                     , s3_file_path
                                     , download_path
                                     , ExtraArgs={'RequestPayer': 'requester'})

    def download_images(self, s3_client, s3_file_paths: List, path_to_download: str):
        logging.info(f'downloading files to {path_to_download} ...')

        if not os.path.exists(path_to_download):
            os.makedirs(path_to_download)

        for f in s3_file_paths:
            file_name = self.create_file_name(f['Key'], f['LastModified'])
            download_path = f'{path_to_download}{file_name}'
            self.download_image(s3_client, f['Key'], download_path)

    def pull_images(self) -> int:
        s3_paths = self.find_s3_file_paths()
        with futures.ThreadPoolExecutor(max_workers=30) as executor:
            executor.submit(self.download_images,self.s3_client, s3_paths['red'], self.red_band_path)
            executor.submit(self.download_images,self.s3_client, s3_paths['blue'], self.blue_band_path)
            executor.submit(self.download_images,self.s3_client, s3_paths['green'], self.green_band_path)

        executor.shutdown()

        return 0
