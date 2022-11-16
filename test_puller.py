# 3rd party
import pytest
import numpy as np

# lib
from puller import S3Puller

class TestS3Puller:

    def test_parse_tile_id(self):
        tile_id = "8DVA"
        puller = S3Puller(config={}, tile_id=tile_id)

        parsed = puller.parse_tile_id()
        assert parsed[0] == '8'
        assert parsed[1] == 'D'
        assert parsed[2] == 'VA'

        tile_id = "60DVA"
        puller = S3Puller(config={}, tile_id=tile_id)

        parsed = puller.parse_tile_id()
        assert parsed[0] == '60'
        assert parsed[1] == 'D'
        assert parsed[2] == 'VA'




