
from ast import Tuple
import datetime
from itertools import count
import json
import os
import logging as log
from typing import Dict, List

from requests import HTTPError
from tqdm import tqdm
from web3 import Web3, contract
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
            block_info = self.w3.eth.get_block(block_num)
        except BlockNotFound:
            return None
        last_time = block_info["timestamp"]
        return datetime.datetime.utcfromtimestamp(last_time)
        
    def _get_events(self, start_block: int, contract_address: str = "0x767FE9EDC9E0dF98E07454847909b5E959D7ca0E", event_name: str = "Transfer") -> List[Dict]:
        """This function is responsible to get the event data on the blockchain,
        using a contract address and a start block to search.

        Args:
            start_block (int): The starting block, because ethereum exists before the contract
            that we are looking.
            contract_address (str): The contract address that we a looking for the events. Should match
            with the abi from the class.
            event_name (str, optional): The event we are looking for. Defaults to "Transfer".

        Returns:
            List[Dict]: A list of events in a dict form.
        """

        abi = self.ABI
        contract = self.w3.eth.contract(address=contract_address, abi=abi)
        start, end = start_block, start_block + 5000                                                                                                                      
        dict_list = []
        for batch in tqdm(range(int((self.w3.eth.block_number - start_block) / 10000))):
            data = self._get_events_data(contract, event_name, block_start=start, block_end=end)
            start, end = self._get_new_block_window(len(data), start, end)
            new_dict_list = self._parse_event_batch_to_dict(data, event_name)
            if len(new_dict_list) == 0:
                pass
            else:
                dict_list.extend(new_dict_list)
            if start > self.w3.eth.block_number:
                break
        return dict_list
   
    def _get_new_block_window(self, n_elements, start, end):
        """This method controls the flow of blocks that we are reading.

        Args:
            n_elements (_type_): Number of elements
            start (_type_): Block to start the reading
            end (_type_): Block to finish the reading

        Returns:
            int: New start block and new end block.
        """        
        # window_size = end - start
        # if n_elements > 5000:
        #     window_size /= 2
        # elif n_elements < 50:
        #     window_size *= 2
        # start = end + 1
        # end = start + window_size - 1
        start = end + 1
        end = start + 10000
        return int(start), int(end) 

    def _get_events_data(self, contract, event_name: str, block_start: int, block_end: int) -> dict:
        """This method use the contract to get some event based on the initial block and the end block.

        Args:
            contract (_type_): The contract object from web3.
            event_name (str): The name for the event that we are listen to.
            block_start (int): The initial block.
            block_end (int): The last block.

        Returns:
            dict: Returns the dict containing the information for the event in the block's range.
        """        
        staked_filter = contract.events[event_name].create_filter(fromBlock=int(block_start), toBlock=int(block_end))
        try:
            data = staked_filter.get_all_entries()
        except ValueError as e:
            print(e)
            raise
        return data
    
    def _parse_event_batch_to_dict(self, data: dict, event_name: str):
        """This method is responsible to translate the abi event to dataframe.

        Args:
            pool_label (str): The name of the pool address.
            data (dict): The dictionary containing the data from the contract.
            event_name (str): The name of the event that we are currently looking.

        Returns:
            ps.DataFrame: Returns the dataframe containing the information for the event that we are listen.
        """        
        parsed = []
        for i in range(len(data)):
            if event_name == "Transfer":
                event_log = {
                    "txhash": data[i]["transactionHash"].hex(),
                    "blockNumber": data[i]["blockNumber"],
                    "from": data[i]['args']['from'],
                    "to": data[i]['args']['to'],
                    "value": data[i]['args']['value']
                }
            parsed.append(event_log)
        return parsed

