# standard lib
import logging
import os
from concurrent import futures
import shutil
from typing import Dict

# 3rd party
import numpy as np

import rasterio
from rasterio.windows import Window


class ImageProcessor:
    IMG_SHAPE_W = 10980
    IMG_SHAPE_H = 10980

    def __init__(self
                 , dest_path: str = './tmp/final/'
                 , red_band_path: str = './tmp/red/'
                 , green_band_path: str = './tmp/green/'
                 , blue_band_path: str = './tmp/blue/'):
        self.dest_path = dest_path
        self.red_band_path = red_band_path
        self.green_band_path = green_band_path
        self.blue_band_path = blue_band_path

    @staticmethod
    def num_files_in_dir(path: str):
        _, _, files = next(os.walk(path))
        return len(files)

    def compute_rgb_median(self) -> np.array:
        raise NotImplemented

    def create_composite(self, arr: np.array):
        raise NotImplemented


class WindowImageProcessor(ImageProcessor):
    # Always compute across all columns

    def __init__(self, window_size_row=2000, **kwargs):
        super().__init__(**kwargs)
        self.window_size_row = window_size_row
        self.window_size_column = (0, self.IMG_SHAPE_H)

    def compute_median(self, band: str, path: str) -> np.array:

        num_of_files = self.num_files_in_dir(path)
        output_arr = np.zeros((self.IMG_SHAPE_W, self.IMG_SHAPE_H))

        logging.info(f'computing {band} band median across {num_of_files}'
                     f' with window size: {self.window_size_row} by {self.IMG_SHAPE_H}')

        # Iterate down the image in 'windows' - with origin top left
        for row_idx in range(0, self.IMG_SHAPE_W, self.window_size_row):
            logging.info(f'windowing through {band} imgs, at idx: {row_idx} ...')

            # Are we at the last iteration?
            if (self.IMG_SHAPE_W - row_idx) > self.window_size_row:
                combined_rows_arr = np.zeros((num_of_files, self.window_size_row, self.IMG_SHAPE_H))
            else:
                combined_rows_arr = np.zeros((num_of_files, self.IMG_SHAPE_W - row_idx, self.IMG_SHAPE_H))

            # Store are windows for each in file in combined_rows_arr
            for i, file in enumerate(os.listdir(path)):
                with rasterio.open(f'{path}{file}') as src:
                    arr = src.read(1, window=Window.from_slices(
                        (row_idx, row_idx + self.window_size_row), self.window_size_column))
                    combined_rows_arr[i] = arr

            # Compute median and assign value to output array
            output_arr[row_idx: row_idx + self.window_size_row, :] = np.median(combined_rows_arr, axis=0)

        return output_arr

    def compute_rgb_median(self) -> np.array:
        with futures.ProcessPoolExecutor(max_workers=3) as executor:
            future_red = executor.submit(self.compute_median, 'red', f'{self.red_band_path}')
            future_green = executor.submit(self.compute_median, 'green', f'{self.green_band_path}')
            future_blue = executor.submit(self.compute_median, 'blue', f'{self.blue_band_path}')

        output_arr = np.zeros((3, self.IMG_SHAPE_W, self.IMG_SHAPE_H))
        # Order matters here!
        output_arr[0] = future_blue.result()
        output_arr[1] = future_green.result()
        output_arr[2] = future_red.result()
        executor.shutdown()

        return output_arr

    def get_profile(self, path) -> Dict:
        with rasterio.open(path) as src:
            profile = src.profile
        return profile

    def create_composite(self, arr: np.array):
        # Get the metadata
        files = os.listdir(self.red_band_path)
        meta = self.get_profile(f'{self.red_band_path}{files[0]}')
        meta.update(count=3)
        meta['driver'] = 'GTiff'

        dest = f'{self.dest_path}stacked.tiff'
        shutil.rmtree(self.dest_path, ignore_errors=True)
        os.makedirs(self.dest_path)
        # Read each layer from B->G->R and write it to stack
        with rasterio.open(dest, 'w', **meta) as dst:
            dst.write_band(1, arr[0])
            dst.write_band(2, arr[1])
            dst.write_band(3, arr[2])


class BinaryImageProcessor(ImageProcessor):

    def __init__(self, binary_path='np/', **kwargs):
        super().__init__(**kwargs)
        self.np_binary_path = binary_path

    def jp2_files_to_binary(self):
        logging.info(f'converting .jp2 to binary np arrays...')
        with futures.ThreadPoolExecutor(max_workers=3) as executor:
            executor.submit(self.jp2_to_binary, self.red_band_path, self.np_binary_path)
            executor.submit(self.jp2_to_binary, self.green_band_path, self.np_binary_path)
            executor.submit(self.jp2_to_binary, self.blue_band_path, self.np_binary_path)

        executor.shutdown()

    def jp2_to_binary(self, jp2_path: str, np_binary_path: str):
        # Start fresh each time
        path_to_save = f'{jp2_path}{np_binary_path}'
        shutil.rmtree(path_to_save, ignore_errors=True)
        os.makedirs(path_to_save)
        for file in os.listdir(jp2_path):
            jp2_file = f'{jp2_path}{file}'
            with rasterio.open(jp2_file) as src:
                logging.info(f'writing {jp2_file} to disk as binary ...')
                f = rasterio.open(jp2_file)
                file_name = file.split('.')[0]
                self.save_np_array(f'{path_to_save}{file_name}', f.read(1))

    def save_np_array(self, path_to_save: str, arr: np.array):
        np.save(path_to_save, arr)

    def load_np_file(self, path: str):
        return np.load(path)

    def load_binary_files(self, path: str) -> np.array:
        # Do any files exist in our directory
        logging.info(f'reading all binary files from {path} into np arrays...')
        num_of_files = self.num_files_in_dir(path)
        output_arr = np.zeros((num_of_files, self.IMG_SHAPE_W, self.IMG_SHAPE_H))
        for idx, file in enumerate(os.listdir(path)):
            output_arr[idx] = self.load_np_file(f'{path}{file}')
        return output_arr

    def compute_median(self, band: str, binary_file_path: str) -> np.array:
        all_images_arr = self.load_binary_files(binary_file_path)
        logging.info('computing median in memory...')
        return np.median(all_images_arr, axis=0)

    def compute_rgb_median(self) -> np.array:
        logging.info(f'computing medians...')
        with futures.ProcessPoolExecutor(max_workers=3) as executor:
            future_red = executor.submit(self.compute_median, 'red', f'{self.red_band_path}{self.np_binary_path}')
            future_blue = executor.submit(self.compute_median, 'blue', f'{self.blue_band_path}{self.np_binary_path}')
            future_green = executor.submit(self.compute_median, 'green', f'{self.green_band_path}{self.np_binary_path}')

        output_arr = np.zeros((3, self.IMG_SHAPE_W, self.IMG_SHAPE_H))
        output_arr[0] = future_red.result()
        output_arr[1] = future_green.result()
        output_arr[2] = future_blue.result()
        executor.shutdown()
        return output_arr

    def create_composite(self, arr: np.array):
        pass
