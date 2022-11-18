# standard lib
import logging
import os
from concurrent import futures
import shutil

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

    # TODO: fix api
    def compute_rgb_median(self):
        raise NotImplemented

    def create_composite(self, arr: np.array):
        raise NotImplemented


class BinaryImageProcessor(ImageProcessor):

    def __init__(self, binary_path='np/', **kwargs):
        super().__init__(**kwargs)
        self.np_binary_path = binary_path

    def jp2_files_to_binary(self):
        logging.info(f'converting .jp2 to binary np arrays...')
        with futures.ThreadPoolExecutor(max_workers=3) as executor:
            executor.submit(self.jp2_to_binary, self.red_band_path, self.np_binary_path)
            executor.submit(self.jp2_to_binary, self.blue_band_path, self.np_binary_path)
            executor.submit(self.jp2_to_binary, self.green_band_path, self.np_binary_path)

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
        logging.info(f'reading binary files from {path} into np arrays...')
        num_of_files = self.num_files_in_dir(path)
        output_arr = np.zeros((num_of_files, self.IMG_SHAPE_W, self.IMG_SHAPE_H))
        for idx, file in enumerate(os.listdir(path)):
            output_arr[idx] = self.load_np_file(f'{path}{file}')
        return output_arr

    def compute_median(self, band: str, binary_file_path: str) -> np.array:
        all_images_arr = self.load_binary_files(binary_file_path)
        logging.info('computing median...')
        # TODO: fix this
        output_arr = np.median(all_images_arr, axis=0)
        np.save(f'{self.dest_path}-{band}',output_arr)

    def compute_rgb_median(self):
        logging.info(f'computing medians...')
        with futures.ProcessPoolExecutor(max_workers=3) as executor:
            executor.submit(self.compute_median, 'red', f'{self.red_band_path}{self.np_binary_path}')
            executor.submit(self.compute_median,'blue', f'{self.blue_band_path}{self.np_binary_path}')
            executor.submit(self.compute_median, 'green', f'{self.green_band_path}{self.np_binary_path}')
        executor.shutdown()

    def create_composite(self, arr: np.array):
        pass


class WindowImageProcess(ImageProcessor):
    # Always compute across columns
    WINDOW_SIZE_COLUMN = (0, 10980)

    def __init__(self, window_size_row=2000,  **kwargs):
        super().__init__(**kwargs)
        self.window_size_row = window_size_row

    @staticmethod
    def compute_median(arr: np.array) -> float:
        # Filter our zero values
        non_zero = arr[arr != 0]

        # Is it even?
        arr_len = len(non_zero)
        if arr_len % 2 == 0:
            left_idx = (arr_len / 2) - 1
            right_idx = (arr_len / 2) - 1
            return (non_zero[left_idx] + non_zero[right_idx]) / 2

        else:
            middle_idx = round((arr_len / 2))
            return non_zero[middle_idx]


    def compute_rgb_median(self):
        """
        :param path:
        :return:
        """
        # TODO: include all paths
        path = self.red_band_path

        num_of_files = self.num_files_in_dir(path)
        logging.info(f'computing median across {num_of_files} images in: {path}')

        output_arr = np.zeros((self.IMG_SHAPE_W, self.IMG_SHAPE_H))

        for row_idx in range(0, self.IMG_SHAPE_W, self.window_size_row):
            logging.info(f'computing median across {num_of_files} and {self.window_size_row} rows')

            # Fetch rows for every img and store in combined_rows_arr
            if (self.IMG_SHAPE_W - row_idx) > self.window_size_row:
                combined_rows_arr = np.zeros(
                    (num_of_files, self.window_size_row, self.IMG_SHAPE_H)
                )
            else:
                combined_rows_arr = np.zeros(
                    (num_of_files, self.IMG_SHAPE_W - row_idx, self.IMG_SHAPE_H)
                )

            for i, file in enumerate(os.listdir(path)):
                with rasterio.open(f'{path}{file}') as src:
                    arr = src.read(1, window=Window.from_slices(
                        (row_idx, row_idx + self.window_size_row), self.WINDOW_SIZE_COLUMN)
                    )
                    combined_rows_arr[i] = arr

            # Compute median and assign value to output array
            output_arr[row_idx: row_idx + self.window_size_row, :] = np.median(combined_rows_arr, axis=0)

        return output_arr