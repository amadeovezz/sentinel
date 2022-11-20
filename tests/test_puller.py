# standard lib
from datetime import datetime
from dateutil.tz import tzutc

# 3rd party
import pytest
import numpy as np

# lib
from puller import S3Puller


class TestS3Puller:

    def test_parse_tile_id(self):
        tile_id = "8DVA"
        puller = S3Puller(tile_id=tile_id)

        parsed = puller.parse_tile_id()
        assert parsed[0] == '8'
        assert parsed[1] == 'D'
        assert parsed[2] == 'VA'

        tile_id = "60DVA"
        puller = S3Puller(tile_id=tile_id)

        parsed = puller.parse_tile_id()
        assert parsed[0] == '60'
        assert parsed[1] == 'D'
        assert parsed[2] == 'VA'


    def test_filter_s3_dates(self):
        s3_response = [
            {'Key': 'tiles/9/D/VA/2016/10/19/0/B02.jp2'
                , 'LastModified': datetime(2016, 10, 19, 19, 48, 19, tzinfo=tzutc())
                , 'id': 1}
            , {'Key': 'tiles/9/D/VA/2018/9/1/0/B02.jp2'
                ,'LastModified': datetime(2018, 9, 1, 19, 48, 19, tzinfo=tzutc())
                , 'id': 2}

            , {'Key': 'tiles/9/D/VA/2019/11/3/0/B02.jp2'
                ,'LastModified': datetime(2019, 11, 3, 19, 48, 19, tzinfo=tzutc())
                , 'id': 3}

            , {'Key': 'tiles/9/D/VA/2019/12/3/0/B03.jp2'
                ,'LastModified': datetime(2019, 12, 3, 19, 48, 19, tzinfo=tzutc())
                , 'id': 4}

            , {'Key': 'tiles/9/D/VA/2019/12/3/0/preview/B03.jp2'
                ,'LastModified': datetime(2019, 12, 3, 19, 48, 19, tzinfo=tzutc())
                , 'id': 5}

            , {'Key': 'tiles/9/D/VA/2020/2/22/0/B04.jp2'
                ,'LastModified': datetime(2020, 2, 22, 19, 48, 19, tzinfo=tzutc())
                , 'id': 6}

            , {'Key': 'tiles/9/D/VA/2021/4/19/0/B01.jp2',
               'LastModified': datetime(2021, 4, 19, 19, 48, 19, tzinfo=tzutc())
                , 'id': 7}
        ]

        start = '2019-08-26T02:44:33.000000Z'
        end = '2021-08-26T02:44:33.000000Z'
        puller = S3Puller(tile_id="8DVA", start=start, end=end)
        filtered_list = puller.filter_s3_files(s3_response, band='red')
        assert filtered_list[0]['id'] == 6


def test_filter_s3_dates(self):
    s3_response = [
        {'Key': 'tiles/9/D/VA/2016/10/19/0/B02.jp2'
            , 'LastModified': datetime(2016, 10, 19, 19, 48, 19, tzinfo=tzutc())
            , 'id': 1}
        , {'Key': 'tiles/9/D/VA/2018/9/1/0/B02.jp2'
            ,'LastModified': datetime(2018, 9, 1, 19, 48, 19, tzinfo=tzutc())
            , 'id': 2}

        , {'Key': 'tiles/9/D/VA/2019/11/3/0/B02.jp2'
            ,'LastModified': datetime(2019, 11, 3, 19, 48, 19, tzinfo=tzutc())
            , 'id': 3}

        , {'Key': 'tiles/9/D/VA/2019/12/3/0/B03.jp2'
            ,'LastModified': datetime(2019, 12, 3, 19, 48, 19, tzinfo=tzutc())
            , 'id': 4}

        , {'Key': 'tiles/9/D/VA/2019/12/3/0/preview/B03.jp2'
            ,'LastModified': datetime(2019, 12, 3, 19, 48, 19, tzinfo=tzutc())
            , 'id': 5}

        , {'Key': 'tiles/9/D/VA/2020/2/22/0/B04.jp2'
            ,'LastModified': datetime(2020, 2, 22, 19, 48, 19, tzinfo=tzutc())
            , 'id': 6}

        , {'Key': 'tiles/9/D/VA/2021/4/19/0/B01.jp2',
           'LastModified': datetime(2021, 4, 19, 19, 48, 19, tzinfo=tzutc())
            , 'id': 7}
    ]

    start = '2019-08-26T02:44:33.000000Z'
    end = '2021-08-26T02:44:33.000000Z'
    puller = S3Puller(tile_id="8DVA", start=start, end=end)
    filtered_list = puller.filter_s3_files(s3_response, band='red')
    assert filtered_list[0]['id'] == 6





def test_filter_s3_response(self):
        s3_response = [
            {'Key': 'tiles/9/D/VA/2016/10/19/0/B02.jp2'
                , 'LastModified': datetime(2016, 10, 19, 19, 48, 19, tzinfo=tzutc())
                , 'id': 1}
            , {'Key': 'tiles/9/D/VA/2018/9/1/0/B02.jp2'
                ,'LastModified': datetime(2018, 9, 1, 19, 48, 19, tzinfo=tzutc())
                , 'id': 2}

            , {'Key': 'tiles/9/D/VA/2019/11/3/0/B10.jp2'
                ,'LastModified': datetime(2019, 11, 3, 19, 48, 19, tzinfo=tzutc())
                , 'id': 3}

            , {'Key': 'tiles/9/D/VA/2019/12/3/0/B03.jp2'
                ,'LastModified': datetime(2019, 12, 3, 19, 48, 19, tzinfo=tzutc())
                , 'id': 4}

            , {'Key': 'tiles/9/D/VA/2019/12/3/0/preview/B03.jp2'
                ,'LastModified': datetime(2019, 12, 3, 19, 48, 19, tzinfo=tzutc())
                , 'id': 5}

            , {'Key': 'tiles/9/D/VA/2020/2/22/0/B04.jp2'
                ,'LastModified': datetime(2020, 2, 22, 19, 48, 19, tzinfo=tzutc())
                , 'id': 6}

            , {'Key': 'tiles/9/D/VA/2021/4/19/0/B01.jp2',
               'LastModified': datetime(2021, 4, 19, 19, 48, 19, tzinfo=tzutc())
                , 'id': 7}
        ]

        start = '2019-08-26T02:44:33.000000Z'
        end = '2021-08-26T02:44:33.000000Z'
        puller = S3Puller(tile_id="8DVA", start=start, end=end)
        filtered_list = puller.filter_s3_files(s3_response, band='red')
        assert filtered_list[0]['id'] == 6
