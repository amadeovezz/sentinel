# standard lib
import json
import logging

# 3rd party
# lib
from puller import S3Puller
from image_process import ImageProcess

if __name__ == '__main__':

    logging.basicConfig(level=logging.INFO, format='%(message)s')
    # Get creds
    f = open('./keys/aws.json')
    creds = json.load(f)
    tile_id = '9DVA'
    start = '2020-05-09T02:44:33.000000Z'
    end = '2020-12-26T02:44:33.000000Z'
    red_band_path = './tmp/red/'
    green_band_path = './tmp/green/'
    blue_band_path = './tmp/blue/'


    #Find and filter data
    puller = S3Puller(config=creds, tile_id=tile_id, start=start, end=end)
    puller.connect()
    success = puller.pull_images()
    if success != 0:
        logging.fatal('failed to pull images...')

    # Manipulate data
    process = ImageProcess()
    median_images = process.find_median()
    composites = process.create_composite_image(median_images)
    process.save(composites)

