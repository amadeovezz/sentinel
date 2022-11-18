# standard lib
import json
import logging

# 3rd party
# lib
from puller import S3Puller
from image_process import BinaryImageProcessor, WindowImageProcessor

if __name__ == '__main__':

    logging.basicConfig(level=logging.INFO, format='%(message)s')
    # Get creds
    f = open('./keys/aws.json')
    creds = json.load(f)
    tile_id = '9DVA'
    start = '2020-05-09T02:44:33.000000Z'
    end = '2020-12-26T02:44:33.000000Z'

    #Find and filter data
    puller = S3Puller(config=creds, tile_id=tile_id, start=start, end=end)
    puller.connect()
    success = puller.pull_images()
    if success != 0:
        logging.fatal('failed to pull images...')

    # Manipulate data
    process = WindowImageProcessor()
    arr = process.compute_rgb_median()
    process.create_composite(arr)






