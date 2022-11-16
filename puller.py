from typing import List
import numpy as np
import boto3
from datetime import datetime

class SatPuller:

    def connect(self):
        raise NotImplemented

    def pull_images(self) -> np.array:
        raise NotImplemented


class S3Puller(SatPuller):

    config = {}

    def __init__(self, config):
        self.config = config

    def connect(self):
        pass

    def pull_images(self) -> np.array:
        pass

    @staticmethod
    def filter_files(l: List, start: datetime, end: datetime) -> List:
        pass
