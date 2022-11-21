# standard lib
import json
from typing import Dict, List

# 3rd party
import pytest
import rasterio

# lib
from puller import S3Cli, RGBPuller
from image_process import WindowImageProcessor, MedianMerger

KEYS_DIR = '../../keys/aws.json'

@pytest.fixture
def config():
    f = open(KEYS_DIR)
    return json.load(f)

@pytest.fixture
def preview_bucket_prefix():
    return 'tiles/18/T/TK/2015/10/27/0/preview/'

@pytest.fixture
def filter_rgb():
    def filter_func(l: List[Dict]):
        def is_valid(response_obj: Dict):
            # Ignore preview directory
            file_band = response_obj['Key'].split('/')[-1]
            if file_band in ['B04.jp2', 'B03.jp2', 'B02.jp2']:
                return True
            return False
        return list(filter(is_valid, l))
    return filter_func


def test_find_s3_files(config, preview_bucket_prefix, filter_rgb):
    """
    :param config:
    :return:
    """
    s3_cli = S3Cli(config=config)
    s3_cli.connect()

    files_to_find = ['B04.jp2', 'B03.jp2', 'B02.jp2']
    files = s3_cli.find_s3_files(preview_bucket_prefix, filter_rgb)
    for f in files:
        assert f['Key'].split('/')[-1] in files_to_find


def test_find_and_download_images(config, preview_bucket_prefix, filter_rgb, tmp_path):
    s3_cli = S3Cli(config=config)
    s3_cli.connect()

    files = s3_cli.find_s3_files(preview_bucket_prefix, filter_rgb)

    red_band = RGBPuller.group_by_band(files, RGBPuller.BAND_MAPPING, 'red')
    blue_band = RGBPuller.group_by_band(files, RGBPuller.BAND_MAPPING, 'blue')
    green_band = RGBPuller.group_by_band(files, RGBPuller.BAND_MAPPING, 'green')

    s3_cli.download_images(s3_cli.boto_client, red_band, f'{tmp_path}/red/', lambda x: 'red.jp2')
    s3_cli.download_images(s3_cli.boto_client, blue_band, f'{tmp_path}/blue/', lambda x: 'blue.jp2')
    s3_cli.download_images(s3_cli.boto_client, green_band, f'{tmp_path}/green/', lambda x: 'green.jp2')

    f_r = rasterio.open(f'{tmp_path}/red/red.jp2')
    assert f_r.profile['driver'] == 'JP2OpenJPEG'
    assert len(f_r.read(1)) == 687

    f_r = rasterio.open(f'{tmp_path}/blue/blue.jp2')
    assert f_r.profile['driver'] == 'JP2OpenJPEG'
    assert len(f_r.read(1)) == 687

    f_r = rasterio.open(f'{tmp_path}/green/green.jp2')
    assert f_r.profile['driver'] == 'JP2OpenJPEG'
    assert len(f_r.read(1)) == 687


def test_find_download_and_process(config, preview_bucket_prefix, filter_rgb, tmp_path):
    s3_cli = S3Cli(config=config)
    s3_cli.connect()

    files = s3_cli.find_s3_files(preview_bucket_prefix, filter_rgb)

    red_band = RGBPuller.group_by_band(files, RGBPuller.BAND_MAPPING, 'red')
    blue_band = RGBPuller.group_by_band(files, RGBPuller.BAND_MAPPING, 'blue')
    green_band = RGBPuller.group_by_band(files, RGBPuller.BAND_MAPPING, 'green')

    red_path = f'{tmp_path}/red/'
    blue_path = f'{tmp_path}/blue/'
    green_path = f'{tmp_path}/green/'

    s3_cli.download_images(s3_cli.boto_client, red_band, red_path, lambda x: 'red.jp2')
    s3_cli.download_images(s3_cli.boto_client, blue_band, blue_path, lambda x: 'blue.jp2')
    s3_cli.download_images(s3_cli.boto_client, green_band, green_path, lambda x: 'green.jp2')

    dest_path =  f'{tmp_path}/final/'

    w = WindowImageProcessor( img_shape_w=687
                             , img_shape_h=687
                             , red_band_path=red_path
                             , green_band_path=green_path
                             , blue_band_path=blue_path
                             , dest_path= dest_path
                             , merger=MedianMerger(), window_size_row=100)

    final_images = w.process()

    assert final_images['red'].shape[0] == 687
    assert final_images['red'].shape[1] == 687

    assert final_images['blue'].shape[0] == 687
    assert final_images['blue'].shape[1] == 687

    assert final_images['green'].shape[0] == 687
    assert final_images['green'].shape[1] == 687

    w.create_composite(final_images)
    composite = rasterio.open(f'{dest_path}final.tiff')
    assert composite.profile['count'] == 3
    # Read as a single image
    assert composite.read(1).shape == (687,687)
    # Read as multiple bands
    assert composite.read().shape == (3, 687, 687)