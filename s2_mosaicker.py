# standard lib
import json
import logging

# 3rd party
import click

# lib
from puller import S3Cli, RGBPuller
from image_process import WindowImageProcessor, MedianMerger


@click.command()
@click.argument('TILE_ID', default='10UDV')
@click.argument('START_DATETIME', default='2019-08-26T02:44:33.000000Z')
@click.argument('END_DATETIME', default='2019-09-07T18:42:22.000000Z')
@click.argument('OUTPUT_PATH', default='./tmp/final/')
@click.option('--LOGGING_LEVEL', default='INFO', help='Default is INFO.')
@click.option('--COMBINE_METHOD', default='median', help='Method to process images. Default is median.')
@click.option('--has_pulled', default=False, is_flag=True, help='Pass this flag if you have already pulled images and just wish to process.')
def main(tile_id, start_datetime, end_datetime, output_path, combine_method, logging_level, has_pulled):

    logging.basicConfig(level=logging.getLevelName(logging_level), format='%(message)s')

    if not has_pulled:

        # Find and filter data
        s3_cli = S3Cli()
        rgb_puller = RGBPuller(s3_cli, tile_id, start_datetime, end_datetime)
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