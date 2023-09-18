"""
These are the project paths that will be used.

Name: Felipe Lana Machado
Date: 03/03/2022
"""

import os

BASE_PATH = os.path.abspath('.')
TEST_PATH = os.path.join(BASE_PATH, 'tests')
DATA_PATH = os.path.join(BASE_PATH, 'data')
DATA_RAW = os.path.join(DATA_PATH, 'raw')
DATA_PROCESSED = os.path.join(DATA_PATH, 'processed')
DOCS_PATH = os.path.join(BASE_PATH, 'docs')
LOGS_PATH = os.path.join(DOCS_PATH, 'logs')
ABI_PATH = os.path.join(DOCS_PATH, 'abi')
IMAGES_PATH = os.path.join(DOCS_PATH, 'images')
