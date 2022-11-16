from puller import S3Puller
from image_process import ImageProcess


if __name__ == '__main__':

    # Find and filter data
    config = {}

    puller = S3Puller(config)
    puller.connect()
    images = puller.pull_images()

    # Manipulate data
    process = ImageProcess(images)
    median_images = process.find_median()
    composites = process.create_composite_image(median_images)
    process.save(composites)

