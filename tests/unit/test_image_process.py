# standard lib

# 3rd party
import pytest
import numpy as np
import rasterio
from rasterio.transform import from_origin
from rasterio import crs

# lib
from image_process import MedianMerger, WindowImageProcessor, ParallelWindowProcessor



class TestMedianMerger:

    def test_compute_median_even(self):
        arr = np.array(
            [
                [[1, 1, 1],
                 [1, 1, 1]]
              , [[3, 2, 3],
                 [5, 6, 7]]
            ], dtype='uint16'
        )
        m = MedianMerger()
        out = m.merge(arr)
        assert np.all(out[0, :] == np.array([2, 1, 2]))
        assert np.all(out[1, :] == np.array([3, 3, 4]))

    def test_compute_median_odd(self):
        arr = np.array(
            [
                [[1, 1, 1],
                 [1, 1, 1]]
              , [[3, 3, 3],
                 [7, 5, 3]]
              , [[5, 5, 5],
                 [5, 10, 1]]
            ], dtype='uint16'
        )
        m = MedianMerger()
        out = m.merge(arr)
        assert np.all(out[0, :] == np.array([3, 3, 3]))
        assert np.all(out[1, :] == np.array([5, 5, 1]))

    def test_all_zero(self):
        arr = np.array(
            [
                [[0, 0, 0],
                 [0, 0, 0]]
              , [[0, 0, 0],
                 [0, 0, 0]]
            ], dtype='uint16'

        )
        m = MedianMerger()
        out = m.merge(arr)
        assert np.all(out[0, :] == np.array([0, 0, 0]))
        assert np.all(out[1, :] == np.array([0, 0, 0]))

    def test_different_types(self):
        arr = np.array(
            [
                [[0, 2.5, 0],
                 [1, 1, 1]]
              , [[3, 3, 3],
                 [0, 0.0, 1.3]]
            ], dtype='uint16'

        )
        m = MedianMerger()
        out = m.merge(arr)
        assert np.all(out[0, :] == np.array([3, 2, 3]))
        assert np.all(out[1, :] == np.array([1, 1, 1]))


@pytest.fixture()
def img():
    return np.array([
        [1, 1, 1, 1, 1]
        , [2, 2, 2, 2, 2]
        , [0, 0, 0, 0, 0]
        , [4, 4, 4, 4, 4]
        , [1, 1, 1, 1, 1]]
        , dtype='uint16')


@pytest.fixture()
def img_2():
    return np.array([
        [1, 1, 0, 3, 1]
        , [2, 2, 2, 2, 2]
        , [3, 3, 3, 3, 3]
        , [5, 6, 7, 8, 9]
        , [3, 3, 3, 3, 3]]
        , dtype='uint16')


@pytest.fixture()
def create_img(img, img_2, tmp_path):
    meta = {
        'driver': 'GTiff', 'dtype': str(img.dtype)
        , 'height': img.shape[0], 'width': img.shape[1], 'count': 1
        , 'crs': crs.CRS.from_epsg(32709), 'transform': from_origin(10.0, 0.0, 399960.0, 0.0)
    }

    img_1_path = f'{tmp_path}/img-1.jp2'
    img_2_path = f'{tmp_path}/img-2.jp2'

    with rasterio.open(img_1_path, 'w', **meta) as dst:
        dst.write(img,1)

    with rasterio.open(img_2_path, 'w', **meta) as dst:
        dst.write(img_2,1)


def test_windowing(create_img, img, tmp_path):
    process = WindowImageProcessor(merger=MedianMerger()
                                , window_size_row=2
                                , dest_path='')
    process.img_shape_w = img.shape[0]
    process.img_shape_h = img.shape[1]

    arr = process.window('blue', f'{tmp_path}/')

    assert np.all(arr[0, :] == np.array([1,1,1,2,1]))
    assert np.all(arr[1, :] == np.array([2,2,2,2,2]))
    assert np.all(arr[2, :] == np.array([3,3,3,3,3]))
    assert np.all(arr[3, :] == np.array([4,5,5,6,6]))
    assert np.all(arr[4, :] == np.array([2,2,2,2,2]))


def test_block_windowing(create_img, img, tmp_path):
    process = ParallelWindowProcessor(merger=MedianMerger()
                                      , window_size_row=2
                                      , img_shape_w=5
                                      , img_shape_h=5
                                      , dest_path=''
                                      )

    process.img_shape_w = img.shape[0]
    process.img_shape_h = img.shape[1]

    arr = process.delayed_window(f'{tmp_path}/')

    assert np.all(arr[0, :] == np.array([1,1,1,2,1]))
    assert np.all(arr[1, :] == np.array([2,2,2,2,2]))
    assert np.all(arr[2, :] == np.array([3,3,3,3,3]))
    assert np.all(arr[3, :] == np.array([4,5,5,6,6]))
    assert np.all(arr[4, :] == np.array([2,2,2,2,2]))