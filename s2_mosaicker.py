# standard lib
import json
import logging

# 3rd party
# lib
from puller import S3Puller
from image_process import ImageProcess

if __name__ == '__main__':

    logging.basicConfig(level=logging.ERROR, format='%(message)s')
    # Get creds
    f = open('./keys/aws.json')
    creds = json.load(f)
    tile_id = '60DVA'

    # Find and filter data
    puller = S3Puller(config=creds, tile_id=tile_id)
    puller.connect()
    images = puller.pull_images()

    # Manipulate data
    process = ImageProcess(images)
    median_images = process.find_median()
    composites = process.create_composite_image(median_images)
    process.save(composites)

