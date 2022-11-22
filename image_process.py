# standard lib
import logging
from abc import ABC, abstractmethod
import os
from concurrent import futures
import shutil
from typing import Dict

# 3rd party
import numpy as np

import rasterio
from rasterio.windows import Window


class ImageProcessor(ABC):

    def __init__(self
                 , img_shape_w :int = 10980
                 , img_shape_h :int = 10980
                 , dest_path: str = './tmp/final/'
                 , red_band_path: str = './tmp/red/'
                 , green_band_path: str = './tmp/green/'
                 , blue_band_path: str = './tmp/blue/'):

        self.img_shape_w = img_shape_w
        self.img_shape_h = img_shape_h
        self.dest_path = dest_path
        self.red_band_path = red_band_path
        self.green_band_path = green_band_path
        self.blue_band_path = blue_band_path


    @abstractmethod
    def process(self) -> Dict[str, np.ndarray]:
        raise NotImplemented()

    @staticmethod
    def num_files_in_dir(path: str):
        _, _, files = next(os.walk(path))
        return len(files)

    @staticmethod
    def get_profile(path) -> Dict:
        with rasterio.open(path) as src:
            profile = src.profile
        return profile

    def create_composite(self, arr_map: Dict[str, np.ndarray]):
        # Get geo metadata
        files = os.listdir(self.red_band_path)
        meta = self.get_profile(f'{self.red_band_path}{files[0]}')
        meta.update(count=3)
        meta.update(driver='GTiff')
        meta.update(photometric='RGB')

        dest = f'{self.dest_path}combined_image.tiff'
        shutil.rmtree(self.dest_path, ignore_errors=True)
        os.makedirs(self.dest_path)
        # Write each layer from R->G->B
        with rasterio.open(dest, 'w', **meta) as dst:
            dst.write(arr_map['red'], 1)
            dst.write(arr_map['green'], 2)
            dst.write(arr_map['blue'], 3)


class ArrayMerger(ABC):
    @abstractmethod
    def merge(self, arr: np.ndarray) -> np.ndarray:
        """
        :param arr: a 3d array where the depth arr[0], arr[1], arr[n] holds multiple versions of the same image or
        part of an image (2d array). Ex:

        x = [
            [[1,1,1]
            ,[1,1,1]]
        ],
        [
            [[3,3,3]
            ,[3,3,3]]
        ]

        :return: a 2d array where some computation is performed on each internal 2d array to decide on the final version
        Ex:
        mean(x) = [[2,2,2]
                  [2,2,2]]

        """
        raise NotImplemented


class MedianMerger(ArrayMerger):
    def merge(self, arr: np.ndarray) -> np.ndarray:
        """
        :param arr: The 3d array to merge

        Converts 0's to nan's, and we leverage nanmedian() here.
        Any nan that does not get converted to a 0 implies that all versions of the same image
        have intensity values 0. This seems unlikely but worth diving deeper on.

        Also note that we return the same type of the ndarray that was passed in.
        For uint16 jp2 images this means that if we have an even amount of numbers: 2,3
        the np.nanmedian() will return 2.5, however when cast it back to uint16 our median will
        rounded down to 2 (the cast always rounds down). To revisit if this is desirable behaviour.

        :return:
        """
        filtered_zeros = np.where(arr == 0, np.nan, arr)
        np.nanmedian(filtered_zeros, axis=0)
        filtered = np.nanmedian(filtered_zeros, axis=0)
        return np.nan_to_num(filtered, nan=0).astype(arr.dtype)


class WindowImageProcessor(ImageProcessor):

    def __init__(self, merger: ArrayMerger, window_size_row=2000, **kwargs):
        super().__init__(**kwargs)
        self.merger = merger
        self.window_size_row = window_size_row
        self.window_size_column = (0, self.img_shape_h)

    def process(self) -> Dict[str, np.array]:
        with futures.ProcessPoolExecutor(max_workers=3) as executor:
            future_red = executor.submit(self.window, 'red', f'{self.red_band_path}')
            future_green = executor.submit(self.window, 'green', f'{self.green_band_path}')
            future_blue = executor.submit(self.window, 'blue', f'{self.blue_band_path}')

        executor.shutdown()
        return {
            'red': future_red.result()
            , 'green': future_green.result()
            , 'blue': future_blue.result()
        }

    def window(self, band: str, path: str) -> np.ndarray:
        # Get some meta-data before we proceed
        num_of_files = self.num_files_in_dir(path)
        files = os.listdir(path)
        meta = self.get_profile(f'{path}{files[0]}')
        dtype = meta['dtype']

        # Create output array
        output_arr = np.zeros((self.img_shape_w, self.img_shape_h), dtype=dtype)
        logging.info(f'computing {band} band median across {num_of_files}'
                     f' with window size: {self.window_size_row} by {self.img_shape_h}')

        # Iterate down the image in 'windows' - with origin top left
        for row_idx in range(0, self.img_shape_w, self.window_size_row):
            logging.info(f'windowing through {band} imgs, at idx: {row_idx} ...')

            # Are we at the last iteration?
            if (self.img_shape_w - row_idx) > self.window_size_row:
                multiple_versions_arr = np.zeros((num_of_files, self.window_size_row, self.img_shape_h), dtype=dtype)
            else:
                multiple_versions_arr = np.zeros((num_of_files, self.img_shape_w - row_idx, self.img_shape_h), dtype=dtype)

            # Store all windows for each in file in multiple_versions_arr
            for i, file in enumerate(os.listdir(path)):
                with rasterio.open(f'{path}{file}') as src:
                    arr = src.read(1, window=Window.from_slices(
                        (row_idx, row_idx + self.window_size_row), self.window_size_column))
                    multiple_versions_arr[i] = arr

            # Perform merging
            out = self.merger.merge(multiple_versions_arr)
            output_arr[row_idx: row_idx + self.window_size_row, :] = out

        return output_arr
