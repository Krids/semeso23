
from ast import Tuple
import datetime
from itertools import count
import json
import os
import logging as log

import pandas as pd
from requests import HTTPError
from tqdm import tqdm
from web3 import Web3
from web3.exceptions import BlockNotFound

from src.utils.project_paths import ABI_PATH

class Blockchain:

    def __init__(self, abi: str, provider_url: str = "https://eth-mainnet.alchemyapi.io/v2/ClCx82fFJ1PmOkY5_tynl9stkzVuQb3V") -> None:
        """This is responsible to iniciate the class with two parameters,
        the first is the name of the abi that we going to use and the second
        is the provider url that we going to use.

        To grab a provider url please visit https://www.alchemyapi.io

        Args:
            abi (str): The name of the abi, usually the name of the coin of the contract.
            provider_url (_type_, optional): The URL provide by alchemy api to grab data from ethereum blockchain.
              Defaults to "https://eth-mainnet.alchemyapi.io/v2/ClCx82fFJ1PmOkY5_tynl9stkzVuQb3V".
        """        
        with open(os.path.join(ABI_PATH, f'{abi}.json'), 'r') as f:
            self.ABI = json.load(f)
        self.w3 = Web3(Web3.HTTPProvider(provider_url))

    def get_block_timestamp(self, block_num: int) -> datetime.datetime:
        """This is responsible to receive a blocknumber from ethereum and
          get the timestamp for this block.

        Args:
            block_num (int): The number of the block that we need the timestamp.

        Returns:
            datetime.datetime: The timestamp of the block.
        """        
        try:
            block_info = self.w3.eth.getBlock(block_num)
        except BlockNotFound:
            return None
        last_time = block_info["timestamp"]
        return datetime.datetime.utcfromtimestamp(last_time)
        