# standard lib
import json
import logging

# 3rd party
import click

# lib
from puller import S3Puller, RGBPuller
from image_process import WindowImageProcessor, MedianMerger


@click.command()
@click.argument('TILE_ID', default='18TTK')
@click.argument('START_DATETIME', default='2020-05-09T02:44:33.000000Z')
@click.argument('END_DATETIME', default='2020-12-26T02:44:33.000000Z')
@click.argument('OUTPUT_PATH', default='./tmp/final/')
@click.option('--AWS_PATH', default='./keys/aws.json')
@click.option('--LOGGING_LEVEL', default='INFO')
@click.option('--COMBINE_METHOD', default='median')
@click.option('--has_pulled', default=False, is_flag=True)
def main(tile_id, start_datetime, end_datetime, output_path, aws_path, combine_method, logging_level, has_pulled: bool):

    logging.basicConfig(level=logging.getLevelName(logging_level), format='%(message)s')

    if not has_pulled:
        f = open(aws_path)
        config = json.load(f)

        # Find and filter data
        s3_puller = S3Puller(config=config)
        rgb_puller = RGBPuller(s3_puller, tile_id, start_datetime, end_datetime)
        success = rgb_puller.pull_images()
        if success != 0:
            logging.fatal('failed to pull images...')

    # Manipulate data
    merger = None
    if combine_method == 'median':
        merger = MedianMerger()

    process = WindowImageProcessor(merger=merger, window_size_row=1000, dest_path=output_path)
    final_imgs = process.process()
    process.create_composite(final_imgs)


if __name__ == '__main__':
    main()
