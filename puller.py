# standard lib
from typing import List, Tuple, Dict, Callable
from datetime import datetime
from dateutil import parser
import logging
import os
from concurrent import futures
import shutil

# 3rd party
import boto3
from boto3.s3.transfer import TransferConfig
import botocore


class S3Cli:

    # Other
    boto_client = None
    transfer_config = None

    def __init__(self
                 , bucket: str = 'sentinel-s2-l1c'
                 ):
        self.bucket = bucket

    def connect(self):
        # Create one session
        session = boto3.Session()
        session.get_credentials()

        # Clients are thread safe
        botocore_config = botocore.config.Config(max_pool_connections=100)
        self.boto_client = boto3.client('s3', config=botocore_config)

        # Improve download speed
        self.transfer_config = TransferConfig(multipart_threshold=1024 * 25,
                                              max_concurrency=20,
                                              multipart_chunksize=(1024 * 1024), # MB
                                              use_threads=True)

        # Make sure connection is correct
        response = self.boto_client.head_object(Bucket=f'{self.bucket}'
                                                , RequestPayer='requester'
                                                , Key='readme.html')

        if response['ResponseMetadata']['HTTPStatusCode'] != 200:
            logging.fatal(f'cannot establish connection with bucket: {self.bucket}...')
        logging.info(f'successfully established connection with bucket: {self.bucket}...')

    @staticmethod
    def flatten(l: List) -> List:
        return [item for sublist in l for item in sublist]

    def find_s3_files(self, bucket_prefix: str, filter_func: Callable) -> List[Dict]:
        """
        :param bucket_prefix: The filepath to search
        :param filter_func: A callable to perform filtering on
        :return: a list that contains paths to files in S3 and associated meta-data
        """
        logging.info('searching for files in s3...')
        paginator = self.boto_client.get_paginator('list_objects')
        # Server side filtering
        page_iterator = paginator.paginate(Bucket=self.bucket
                                           , Prefix=bucket_prefix
                                           , RequestPayer='requester'
                                           , PaginationConfig={'PageSize': 1000})

        paths = []
        for page in page_iterator:
            if 'Contents' not in page:
                logging.fatal('no files found...')
            contents = page['Contents']
            paths.append(filter_func(contents))

        return self.flatten(paths)

    def download_image(self, s3_client, s3_file_path: str, download_path: str):
        logging.info(f'downloading {s3_file_path}...')
        s3_client.download_file(self.bucket
                                , s3_file_path
                                , download_path
                                , Config=self.transfer_config
                                , ExtraArgs={'RequestPayer': 'requester'})

    def download_images(self
                        , s3_client
                        , s3_file_paths: List
                        , path_to_download: str
                        , filename_func: Callable):
        logging.info(f'downloading files to {path_to_download} ...')

        # Clear paths
        shutil.rmtree(path_to_download, ignore_errors=True)
        os.makedirs(path_to_download)

        for f in s3_file_paths:
            file_name = filename_func(f)
            download_path = f'{path_to_download}{file_name}'
            self.download_image(s3_client, f['Key'], download_path)


class RGBPuller:

    BAND_MAPPING = {
        'red': 'B04.jp2'
        , 'green': 'B03.jp2'
        , 'blue': 'B02.jp2'
    }

    BUCKET = "sentinel-s2-l1c"

    def __init__(self
                 , s3_cli: S3Cli
                 , tile_id: str = ''
                 , start: str = ''
                 , end: str = ''
                 , red_band_path: str = './tmp/red/'
                 , green_band_path: str = './tmp/green/'
                 , blue_band_path: str = './tmp/blue/'
                 ):

        self.s3_cli = s3_cli

        self.tile_id = tile_id
        parsed = self.parse_tile_id()
        utm, lat_band, square = parsed[0], parsed[1], parsed[2]
        self.bucket_prefix = f'tiles/{utm}/{lat_band}/{square}/'

        self.start = parser.parse(start)
        self.end = parser.parse(end)

        self.red_band_path = red_band_path
        self.green_band_path = green_band_path
        self.blue_band_path = blue_band_path

    def pull_images(self) -> int:
        self.s3_cli.connect()
        s3_paths = self.s3_cli.find_s3_files(self.bucket_prefix, self.filter_s3_files)

        with futures.ThreadPoolExecutor(max_workers=3) as executor:
            executor.submit(self.s3_cli.download_images
                            , self.s3_cli.boto_client
                            , self.group_by_band(s3_paths, self.BAND_MAPPING,'red')
                            , self.red_band_path
                            , self.create_file_name)

            executor.submit(self.s3_cli.download_images
                            , self.s3_cli.boto_client
                            , self.group_by_band(s3_paths, self.BAND_MAPPING,'blue')
                            , self.blue_band_path
                            , self.create_file_name)

            executor.submit(self.s3_cli.download_images
                            , self.s3_cli.boto_client
                            , self.group_by_band(s3_paths, self.BAND_MAPPING, 'green')
                            , self.green_band_path
                            , self.create_file_name)

        # TODO: catch exceptions here
        executor.shutdown()

        return 0

    @staticmethod
    def group_by_band(l: List, band_mapping: Dict, band: str) -> List[str]:
        def is_valid(response_obj):
            # Ignore preview directory
            file_band = response_obj['Key'].split('/')[-1]
            if file_band == band_mapping[band]:
                return True
            return False
        return list(filter(is_valid, l))

    @staticmethod
    def create_file_name(file_obj: Dict) -> str:
        l = file_obj['Key'].split('/')
        date = file_obj['LastModified']
        return f'{l[7]}-{l[4]}-{l[5]}-{l[6]}-{date.hour}-{l[8]}'

    def filter_s3_files(self, l: List[Dict]) -> List:
        # TODO: make this static
        def is_valid(response_obj: Dict):
            # Ignore preview directory
            if 'preview' not in response_obj['Key']:
                if self.start <= response_obj['LastModified'] <= self.end:
                    file_band = response_obj['Key'].split('/')[-1]
                    if file_band in [self.BAND_MAPPING['red'], self.BAND_MAPPING['green'], self.BAND_MAPPING['blue']]:
                        return True
            return False
        return list(filter(is_valid, l))

    def parse_tile_id(self) -> Tuple[str,str,str]:
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