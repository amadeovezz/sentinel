# standard lib
from typing import Dict, Tuple
from datetime import datetime
from dateutil.tz import tzutc

# 3rd party
import pytest

# lib
from puller import RGBPuller, S3Puller

def create_s3_response(
                         year_month_day: Tuple[int, int, int]
                       , filename: str, id: int
                        , tile_id: Tuple = (8,'DV','A')
                       ) -> Dict:

    year, month, day = year_month_day[0], year_month_day[1], year_month_day[2]
    filepath = f'tiles/{tile_id[0]}/{tile_id[1]}/{tile_id[2]}/{year}/{month}/{day}/0/{filename}'
    datetime_obj = datetime(year, month, day, 4, 12, 22, tzinfo=tzutc())
    return {
        'Key': filepath
        , 'LastModified': datetime_obj
        , 'id': id
    }


class TestRBGPuller:

    s3_puller = S3Puller({}, '')

    def test_single_char_UTM_parse(self):
        tile_id = "8DVA"
        start = '2017-08-26T02:44:33.000000Z'
        end = '2020-08-26T02:44:33.000000Z'

        puller = RGBPuller(self.s3_puller, tile_id=tile_id, start=start, end=end)
        parsed = puller.parse_tile_id()
        assert parsed[0] == '8'
        assert parsed[1] == 'D'
        assert parsed[2] == 'VA'

    def test_multi_char_UTM_parse(self):
        tile_id = "60DVA"
        start = '2017-08-26T02:44:33.000000Z'
        end = '2020-08-26T02:44:33.000000Z'

        puller = RGBPuller(self.s3_puller, tile_id=tile_id, start=start, end=end)
        parsed = puller.parse_tile_id()
        assert parsed[0] == '60'
        assert parsed[1] == 'D'
        assert parsed[2] == 'VA'

    def test_filter_date_range(self):
        s3_response = [
            create_s3_response((2016, 10, 19), 'B02.jp2', 1)
            , create_s3_response((2017, 1, 2), 'B02.jp2', 2)
            , create_s3_response((2018, 3, 11), 'B02.jp2', 3)
            , create_s3_response((2019, 2, 19), 'B02.jp2', 4)
            , create_s3_response((2020, 6, 7), 'B02.jp2', 5)
            , create_s3_response((2024, 10, 12), 'B02.jp2', 6)
        ]

        start = '2017-08-26T02:44:33.000000Z'
        end = '2020-08-26T02:44:33.000000Z'
        puller = RGBPuller(self.s3_puller, tile_id="8DVA", start=start, end=end)
        filtered_list = puller.filter_s3_files(s3_response)
        assert filtered_list[0]['id'] == 3
        assert filtered_list[1]['id'] == 4
        assert filtered_list[2]['id'] == 5

    def test_filter_preview_files(self):
        s3_response = [
            create_s3_response((2016, 10, 19), 'preview/B02.jp2', 1)
            , create_s3_response((2017, 1, 2), 'B02.jp2', 2)
            , create_s3_response((2018, 3, 11), 'preview/B02.jp2', 3)
            , create_s3_response((2019, 2, 19), 'B02.jp2', 4)
            , create_s3_response((2020, 6, 7), 'B02.jp2', 5)
        ]

        start = '2015-08-26T02:44:33.000000Z'
        end = '2021-08-26T02:44:33.000000Z'
        puller = RGBPuller(self.s3_puller, tile_id="8DVA", start=start, end=end)
        filtered_list = puller.filter_s3_files(s3_response)
        assert filtered_list[0]['id'] == 2
        assert filtered_list[1]['id'] == 4
        assert filtered_list[2]['id'] == 5

    def test_filter_and_group_by_band(self):
        s3_response = [
            create_s3_response((2016, 10, 19), 'B02.jp2', 1)
            , create_s3_response((2017, 1, 2), 'B04.jp2', 2)
            , create_s3_response((2018, 3, 11), 'B03.jp2', 3)
            , create_s3_response((2019, 2, 19), 'B02.jp2', 4)
            , create_s3_response((2020, 6, 7), 'B03.jp2', 5)
            , create_s3_response((2024, 10, 12), 'B04.jp2', 6)
        ]

        start = '2013-08-26T02:44:33.000000Z'
        end = '2030-08-26T02:44:33.000000Z'
        puller = RGBPuller(self.s3_puller, tile_id="8DVA", start=start, end=end)
        filtered_list = puller.filter_s3_files(s3_response)

        red_paths = puller.group_by_band(filtered_list, 'red')

        assert red_paths[0]['id'] == 2
        assert red_paths[1]['id'] == 6

