# standard lib
import logging
from abc import ABC, abstractmethod
import os
from concurrent import futures
import shutil
from typing import Dict, Tuple, List

# 3rd party
import numpy as np
import dask
import dask.array as da
from dask import delayed

import rasterio
from rasterio.windows import Window


class ImageProcessor(ABC):

    def __init__(self
                 , img_rows: int = 10980
                 , img_cols: int = 10980
                 , std_block_size: Tuple = (1024, 1024)
                 , dest_path: str = './tmp/final/'
                 , red_band_path: str = './tmp/red/'
                 , green_band_path: str = './tmp/green/'
                 , blue_band_path: str = './tmp/blue/'):
        """
        Each jp2 image is (10980,10980), and block sizes are (1024,1024) - unless we are at the edge of a column
        in which case we have (1024,740) and if we are at the edge of a row we have (740,1024).

        :param img_shape_w:
        :param img_shape_h:
        :param std_block_size: The standard layout of blocks on disk for a given file format
        Available via rasterio's: src.block_shapes
        :param dest_path:
        :param red_band_path:
        :param green_band_path:
        :param blue_band_path:
        """
        self.img_cols = img_cols
        self.img_rows = img_rows
        self.dest_path = dest_path
        self.red_band_path = red_band_path
        self.green_band_path = green_band_path
        self.blue_band_path = blue_band_path
        self.array_dtype = np.uint16 # TODO: infer this
        self.std_block_size = std_block_size

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


class DArrayMerger(ABC):
    @abstractmethod
    def merge(self, arr: da.array) -> da.array:
        """
        Similar to the ArrayMerger class however for dask arrays
        """
        raise NotImplemented


class MedianMerger(ArrayMerger):

    def __init__(self, include_zeros=True):
        self.included_zeros = include_zeros

    def merge(self, arr: np.ndarray) -> np.ndarray:
        """
        :param arr: The 3d array to merge

        Filtering out zeros is much slow, especially if we try to avoid using masked arrays here.
        The include_zero=False approach is converting 0's to nan's, and we leverage nanmedian() here.
        Any nan that does not get converted to a 0 implies that all versions of the same image
        have intensity values 0. This seems unlikely but worth diving deeper on.

        Also note that we return the same type of the ndarray that was passed in.
        For uint16 jp2 images this means that if we have an even amount of numbers: 2,3
        the np.nanmedian() will return 2.5, however when cast it back to uint16 our median will
        rounded down to 2 (the cast always rounds down). To revisit if this is desirable behaviour.

        :return:
        """
        if self.included_zeros:
            return np.median(arr, axis=0)
        else:
            filtered_zeros = np.where(arr == 0, np.nan, arr)
            np.nanmedian(filtered_zeros, axis=0)
            filtered = np.nanmedian(filtered_zeros, axis=0)
            return np.nan_to_num(filtered, nan=0).astype(arr.dtype)


class DMedianMerger(DArrayMerger):

    def __init__(self, include_zeros=True):
        self.included_zeros = include_zeros

    def merge(self, arr: da.array) -> np.ndarray:
        """
        :param arr:
        :return:
        """
        if self.included_zeros:
            return dask.array.median(arr, axis=0)
        else:
            raise NotImplemented()


class ParallelWindowProcessor(ImageProcessor):

    def __init__(self, merger: DArrayMerger, **kwargs):
        """

        This class offers a number of performance improvements over WindowImageProcessor. These include:

        - We leverage the block size from jp2 files to perform efficient windowed reads. Block reads are most efficient
         when the windows match the dataset's own block structure.

        - We leverage dask.delayed() to parallelize reading and computing median values.

        - We leverage dask arrays for blocked algorithms and ensure that our chunk sizes match the jp2 files block size.


        The overall approach to Windowed processing is similar to the WindowImageProcessor, however, this class
        does not accept a window size as a parameter, and instead automatically partitions each jp2 file into
        s sections where each section is an m by n matrix. Where s = round(rows in a image / block size of an image)
        so we would have (10980 / 1024) = 10 sections and the dimensions of each matrix is: m = block size
        and n = num of columns in a jp2 image, so we would have 1024 by 10980 size matrices.


        The size of each section is 1024 * 10980 ~ 11MG, so the total size of each multi-version array is proportional
        to the number of files we have, total_size = num_of_files * 11MG.

        :param kwargs:
        """
        super().__init__(**kwargs)
        self.max_num_of_sections = round(self.img_rows / self.std_block_size[0]) - 1  # indexed at zero
        self.merger = merger

    def get_windows_for_section(self, section_idx: int) -> List[Window]:
        """
        Generates a list of rasterio.Window() objects per section in a jp2 file.

        :param section_idx: The section
        :return:
        """
        if section_idx > self.max_num_of_sections:
            raise Exception('Requested row exceeds max number of rows')

        # Row indexing is fixed across a row
        row_start = section_idx * self.std_block_size[0] if section_idx != 0 else 0
        if section_idx == self.max_num_of_sections:
            row_stop = self.img_rows
        else:
            row_stop = row_start + self.std_block_size[0]

        # Column indexing increases
        column_start = np.arange(0, self.img_cols, self.std_block_size[1]).tolist()
        column_stop = np.arange(self.std_block_size[1], self.img_cols, self.std_block_size[1]).tolist()
        column_stop.append(self.std_block_size[1] - column_stop[-1])

        # Create tuple object
        return [Window.from_slices((row_start, row_stop), (column_start[i], column_stop[i]))
                for i in range(0, len(column_start) - 1)]

    def process(self) -> Dict[str, np.ndarray]:
        with futures.ProcessPoolExecutor(max_workers=3) as executor:
            future_red = executor.submit(self.delayed_window, f'{self.red_band_path}')
            future_green = executor.submit(self.delayed_window, f'{self.green_band_path}')
            future_blue = executor.submit(self.delayed_window, f'{self.blue_band_path}')

        executor.shutdown()
        return {
            'red': future_red.result()
            , 'green': future_green.result()
            , 'blue': future_blue.result()
        }

    def delayed_window(self, path: str) -> np.ndarray:
        delayed_results = []
        # Delay reading and computation
        for section_idx in range(1, self.max_num_of_sections + 1):
            windows = self.get_windows_for_section(section_idx)
            multi_version = self.read_chunks_of_arr(path, windows)
            merged = self.compute(multi_version)
            delayed_results.append(merged)

        # Parallelize computation
        results = dask.delayed(delayed_results)
        computed = results.compute()

        # Reassemble
        output_arr = np.zeros((self.img_cols, self.img_rows), dtype=self.array_dtype)
        row_index = 0
        for arr in computed:
            row_width = arr.shape[0]
            output_arr[row_index:row_index + row_width, :] = arr.compute()
            row_index += row_width

        return output_arr


    @delayed
    def compute(self, arr: da.array) -> da.array:
        return self.merger.merge(arr)

    @delayed
    def read_chunks_of_arr(self, path: str, windows: List[Window]) -> da.array:
        num_of_files = self.num_files_in_dir(path)
        row_width = windows[0].height

        multiple_versions_arr = da.zeros((num_of_files,row_width, self.img_cols)
                                        , dtype=self.array_dtype
                                        , chunks=(num_of_files, self.std_block_size[0], self.std_block_size[1]))

        for i, file in enumerate(os.listdir(path)):
            with rasterio.open(f'{path}{file}') as src:
                for window in windows:
                    arr = src.read(1, window=window)
                    multiple_versions_arr[i, 0:row_width, window.col_off:window.col_off + window.width] = arr
        return multiple_versions_arr


class WindowImageProcessor(ImageProcessor):

    def __init__(self, merger: ArrayMerger, window_size_row=2000, **kwargs):
        super().__init__(**kwargs)
        self.merger = merger
        self.window_size_row = window_size_row
        self.window_size_column = (0, self.img_cols)

    def process(self) -> Dict[str, np.ndarray]:
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
        output_arr = np.zeros((self.img_cols, self.img_rows), dtype=dtype)
        logging.info(f'computing {band} band median across {num_of_files}'
                     f' with window size: {self.window_size_row} by {self.img_rows}')

        # Iterate down the image in 'windows' - with origin top left
        for row_idx in range(0, self.img_cols, self.window_size_row):
            logging.info(f'windowing through {band} imgs, at idx: {row_idx} ...')

            # Are we at the last iteration?
            if (self.img_cols - row_idx) > self.window_size_row:
                multiple_versions_arr = np.zeros((num_of_files, self.window_size_row, self.img_rows), dtype=dtype)
            else:
                multiple_versions_arr = np.zeros((num_of_files, self.img_cols - row_idx, self.img_rows),
                                                 dtype=dtype)

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
