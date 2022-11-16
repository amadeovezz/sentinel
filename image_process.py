import numpy as np

class ImageProcess:

    images = None

    def __init__(self, images: np.array):
        self.images = images

    def find_median(self):
        pass

    def create_composite_image(self, images: np.array) -> np.array:
        pass

    def save(self, images: np.array):
        pass
