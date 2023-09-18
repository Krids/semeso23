
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

from utils.project_paths import ABI_PATH

class BlockchainTest:

    def __init__(self) -> None:
        with open(os.path.join(ABI_PATH, 'abi_v2.json'), 'r') as f:
            self.ABI = json.load(f)
        # provider_url = "https://eth-mainnet.alchemyapi.io/v2/uanCKV5LOP7NtaVUos3qtH-R-V1xy-A3"
        provider_url = "https://eth-mainnet.alchemyapi.io/v2/ClCx82fFJ1PmOkY5_tynl9stkzVuQb3V"
        self.w3 = Web3(Web3.HTTPProvider(provider_url))
        # self.dynamo = Dynamodb(session=boto())
        self.hard_block_start = 12297563
        self.hard_block_end = self.w3.eth.block_number
        self.block_window_size = 1000
        with open(os.path.join(FILES_PATH, 'pools.json'), 'r') as f:
            self.pools_map = json.load(f)

    def get_block_timestamp(self, block_num) -> datetime.datetime:
        """Get Ethereum block timestamp"""
        try:
            block_info = self.w3.eth.getBlock(block_num)
        except BlockNotFound:
            return None
        last_time = block_info["timestamp"]
        return datetime.datetime.utcfromtimestamp(last_time)

    def _map_blocknr_to_ts(self, df: pd.DataFrame) -> pd.DataFrame:
        df.blockNumber = df.blockNumber.astype(int)
        try:
            blocknr_ts_df = pd.read_csv(os.path.join(DATA_RAW, "blocknr_ts_map.csv"))
            blocknr_ts_df.blockNumber = blocknr_ts_df.blockNumber.astype(int)
            blocknr_ts_df.timestamp = pd.to_datetime(blocknr_ts_df.timestamp)
            last_block_nr = blocknr_ts_df.blockNumber.max()
        except FileNotFoundError:
            blocknr_ts_df = pd.DataFrame.from_dict({}, columns=["blockNumber", "timestamp"], orient="index")
            last_block_nr = self.hard_block_start
        print(f"Last block number found in old data: {last_block_nr}")
        print(f"Block numbers missing timestamps: {len(df[df.blockNumber > last_block_nr].blockNumber.unique())}")
        m = {}
        for block_nr in tqdm(df[df.blockNumber > last_block_nr].blockNumber.unique()):
            ts = self.get_block_timestamp(int(block_nr))
            m[block_nr] = ts
        new_blocknr_ts_df = pd.DataFrame.from_dict(m, columns=["timestamp"], orient="index").reset_index()
        new_blocknr_ts_df.columns = ["blockNumber", "timestamp"]
        full_map = pd.concat([blocknr_ts_df, new_blocknr_ts_df], axis=0, ignore_index=True)
        full_map.blockNumber = full_map.blockNumber.astype(int)
        full_map.timestamp = full_map.timestamp.astype(str)
        full_map.to_csv(os.path.join(DATA_RAW, "blocknr_ts_map.csv"), index=False)

        df = df.merge(full_map, on='blockNumber', how='inner')

        return df

    def load_old_blocknr_ts_df(self, df):
        df.blockNumber = df.blockNumber.astype(int)
        try:
            blocknr_ts_df = pd.read_csv(os.path.join(DATA_RAW, "blocknr_ts_map.csv"))
            blocknr_ts_df.blockNumber = blocknr_ts_df.blockNumber.astype(int)
            blocknr_ts_df.timestamp = pd.to_datetime(blocknr_ts_df.timestamp)
            last_block_nr = blocknr_ts_df.blockNumber.max()
            print(f"Old data found, last block number found: {last_block_nr}")
        except FileNotFoundError:
            blocknr_ts_df = pd.DataFrame.from_dict({}, columns=["blockNumber", "timestamp"], orient="index")
            last_block_nr = self.hard_block_start
            print(f"No old data found, setting last block number to start value: {last_block_nr}")
        return blocknr_ts_df, last_block_nr

    def _make_events_df_per_pool(self, pool_address: str, pool_label: str, event_name: str, abi=None) -> list:
        """This is the main method of the class, and is responsible for the call on the other methods to get the 
        data for an event on a single contract address.

        Args:
            pool_address (str): The address of the pool that we are going to listen for the events.
            pool_label (str): The name of the pool that we are going to listen for the events.
            event_name (str): The name of the event that we are going to listen.
            abi (_type_, optional): The abi for the contract address inputed. (None) Defaults to the ABI from the class variable.

        Returns:
            pd.DataFrame: Return a dataframe containing all the data from the contract's event.
        """        
        if abi is None:
            abi = self.ABI
        contract = self.w3.eth.contract(address=pool_address, abi=abi)
        start, end = self.hard_block_start, self.hard_block_start + self.block_window_size
        dfs = []
        for batch in count():
            data = self._get_events_data(contract, event_name, block_start=start, block_end=end)
            start, end = self._get_new_block_window(len(data), start, end)
            df = self._parse_event_batch_to_df(pool_label, data, event_name)
            if df.empty:
                pass
            else:
                dfs.append(df)
            if start > self.hard_block_end:
                break
        if len(dfs) != 0:
            dfs = pd.concat(dfs)
        else:
            if event_name == 'LogStake':
                dfs = pd.DataFrame(columns=['pool', 'txhash', 'blockNumber', 'timestamp', 'from', 'by', 'value', 'stakeId', 'lockUntil'])
            else:
                dfs = pd.DataFrame(columns=['pool', 'txhash', 'blockNumber', 'timestamp', 'from', 'by', 'value', 'sILV'])

        dfs = self._map_blocknr_to_ts(dfs)

        return dfs

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
        staked_filter = contract.events[event_name].createFilter(fromBlock=int(block_start), toBlock=int(block_end))
        try:
            data = staked_filter.get_all_entries()
        except ValueError as e:
            print(e)
            """
            if e['code'] == -32000:
                pass
            else:
                raise
            """
            raise
        return data

    def _parse_event_batch_to_df(self, pool_label: str, data: dict, event_name: str):
        """This method is responsible to translate the abi event to dataframe.

        Args:
            pool_label (str): The name of the pool address.
            data (dict): The dictionary containing the data from the contract.
            event_name (str): The name of the event that we are currently looking.

        Returns:
            ps.DataFrame: Returns the dataframe containing the information for the event that we are listen.
        """        
        parsed = {}
        for i in range(len(data)):
            if event_name == "LogStake":
                event_log = {
                    "pool": pool_label,
                    "txhash": data[i]["transactionHash"].hex(),
                    "blockNumber": data[i]["blockNumber"],
                    "from": data[i]["args"]["from"],
                    "by": data[i]["args"]["by"],
                    "value": data[i]["args"]["value"],
                    "stakeId": data[i]["args"]["stakeId"],
                    "lockUntil": data[i]["args"]["lockUntil"]
                }
            elif event_name == "LogClaimYieldRewards":
                event_log = {
                    "pool": pool_label,
                    "txhash": data[i]["transactionHash"].hex(),
                    "blockNumber": data[i]["blockNumber"],
                    "from": data[i]['args']['from'],
                    "by": data[i]['args']['by'],
                    "value": data[i]['args']['value'],
                    "sILV": data[i]['args']['sILV']
                }
            #  _______________________This are for v1 events!!_______________________
            elif event_name == "Staked":
                event_log = {
                    "pool": pool_label,
                    "txhash": data[i]["transactionHash"].hex(),
                    "blockNumber": data[i]["blockNumber"],
                    "by": data[i]['args']['_by'],
                    "from": data[i]['args']['_from'],
                    "value": data[i]['args']['amount']
                }
            elif event_name == "Unstaked":
                event_log = {
                    "pool": pool_label,
                    "txhash": data[i]["transactionHash"].hex(),
                    "blockNumber": data[i]["blockNumber"],
                    "by": data[i]['args']['_by'],
                    "to": data[i]['args']['_to'],
                    "value": data[i]['args']['amount']
                }
            elif event_name == "Transfer":
                event_log = {
                    "pool": pool_label,
                    "txhash": data[i]["transactionHash"].hex(),
                    "blockNumber": data[i]["blockNumber"],
                    "from": data[i]['args']['from'],
                    "to": data[i]['args']['to'],
                    "value": data[i]['args']['value']
                }
            elif event_name == "StakeLockUpdated":
                event_log = {
                    "pool": pool_label,
                    "txhash": data[i]["transactionHash"].hex(),
                    "blockNumber": data[i]["blockNumber"],
                    "by": data[i]['args']['_by'],
                    "depositId": data[i]['args']['depositId'],
                    "lockedFrom": data[i]['args']['lockedFrom'],
                    "lockedUntil": data[i]['args']['lockedUntil']
                }
            parsed[i] = event_log
        df = pd.DataFrame.from_dict(parsed, orient='index')
        return df

    def _get_new_block_window(self, n_elements, start, end):
        """This method controls the flow of blocks that we are reading.

        Args:
            n_elements (_type_): Number of elements
            start (_type_): Block to start the reading
            end (_type_): Block to finish the reading

        Returns:
            int: New start block and new end block.
        """        
        window_size = end - start
        if n_elements > 5000:
            window_size /= 2
        elif n_elements < 50:
            window_size *= 2
        start = end + 1
        end = start + window_size - 1
        return int(start), int(end)

    def _get_claims_v2(self) -> pd.DataFrame:
        """This function returns everyone that has claimed or ilv or silv2 from staking v2.

        Returns:
            pd.DataFrame: Returns the people who have claimed ilv or silv2.
        """        
        # self.hard_block_start = self.dynamo.get_last_block(event='LogClaimYieldRewards', pool=self.pools_map['ilv_pool'])
        self.hard_block_start = 14443469
        claim_ilv = self._make_events_df_per_pool(pool_address=self.pools_map['ilv_pool'], pool_label='ilv_pool', event_name='LogClaimYieldRewards')
        # self.hard_block_start = self.dynamo.get_last_block(event='LogClaimYieldRewards', pool=self.pools_map['ilv-eth_pool'])
        self.hard_block_start = 14443478
        claim_ilv_eth = self._make_events_df_per_pool(pool_address=self.pools_map['ilv-eth_pool'], pool_label='ilv-eth_pool', event_name='LogClaimYieldRewards')
        claims = pd.concat([claim_ilv, claim_ilv_eth], axis=0)
        claims['value_ilv'] = claims.apply(lambda x: self.w3.fromWei(x['value'],'ether'), axis=1)

        list_of_files = os.listdir(DATA_PROCESSED)
        claims_files = len([file for file in list_of_files if f'claims_{pd.to_datetime(datetime.datetime.now()).date()}' in file])
        claims.to_csv(os.path.join(DATA_PROCESSED, f'claims_{pd.to_datetime(datetime.datetime.now()).date()}_{claims_files}.csv'), index=False)

        return claims.reset_index(drop=True)

    def _get_staked_v2(self) -> Tuple([pd.DataFrame, pd.DataFrame]):
        """This function returns the people who have staked in v2.

        Returns:
            pd.DataFrame: The people who have staked
        """        
        # self.hard_block_start = self.dynamo.get_last_block(event='LogStake', pool=self.pools_map['ilv_pool'])
        self.hard_block_start = 14443469
        stake_ilv = self._make_events_df_per_pool(pool_address=self.pools_map['ilv_pool'], pool_label='ilv_pool', event_name='LogStake')
        # self.hard_block_start = self.dynamo.get_last_block(event='LogStake', pool=self.pools_map['ilv-eth_pool'])
        self.hard_block_start = 14443478
        stake_ilv_eth = self._make_events_df_per_pool(pool_address=self.pools_map['ilv-eth_pool'], pool_label='ilv-eth_pool', event_name='LogStake')

        staked = pd.concat([stake_ilv, stake_ilv_eth], axis=0)
        staked['value_ilv'] = staked.apply(lambda x: self.w3.fromWei(x['value'],'ether'), axis=1)

        list_of_files = os.listdir(DATA_PROCESSED)
        stake_files = len([file for file in list_of_files if f'staked_{pd.to_datetime(datetime.datetime.now()).date()}' in file])
        staked.to_csv(os.path.join(DATA_PROCESSED, f'staked_{pd.to_datetime(datetime.datetime.now()).date()}_{stake_files}.csv'), index=False)

        return staked.reset_index(drop=True)

    def _get_staked_with_pending_rewards_for_v2(self, staked: pd.DataFrame) -> pd.DataFrame:
        """This method returns the people that have stacked and also have pending rewards in v2.

        Returns:
            pd.DataFrame: The people who have staked and also have pending rewards.
        """       
        # staked = self.dynamo.get_staked_dataframe() 
        stake_ilv = staked.loc[staked['pool'] == self.pools_map['ilv_pool']]
        stake_ilv_eth = staked.loc[staked['pool'] == self.pools_map['ilv-eth_pool']]

        staked_ilv_people = pd.DataFrame(columns=['address', 'pendingRewards', 'pendingRewardsRevDis', 'pool'])
        contract_ilv = self.w3.eth.contract(address=self.pools_map['ilv_pool'], abi=self.ABI)
        for address in stake_ilv[stake_ilv['stake_id'] == 0]['wallet'].tolist():
            try:
                result = contract_ilv.functions.pendingRewards(address).call()
            except HTTPError as err:
                self.w3 = Web3(Web3.HTTPProvider("https://eth-mainnet.alchemyapi.io/v2/uanCKV5LOP7NtaVUos3qtH-R-V1xy-A3"))
                contract_ilv = self.w3.eth.contract(address=self.pools_map['ilv_pool'], abi=self.ABI)
                result = contract_ilv.functions.pendingRewards(address).call()
            temp = pd.DataFrame([{'address': address, 'pendingRewards': result[0], 'pendingRewardsRevDis': result[1], 'pool': 'ilv_pool'}])
            staked_ilv_people = pd.concat([staked_ilv_people, temp], axis=0)

        staked_ilv_eth_people = pd.DataFrame(columns=['address', 'pendingRewards', 'pendingRewardsRevDis', 'pool'])
        contract_ilv_eth = self.w3.eth.contract(address=self.pools_map['ilv-eth_pool'], abi=self.ABI)
        for address in stake_ilv_eth[stake_ilv_eth['stake_id'] == 0]['wallet'].tolist():
            try:
                result = contract_ilv_eth.functions.pendingRewards(address).call()
            except HTTPError as err:
                self.w3 = Web3(Web3.HTTPProvider("https://eth-mainnet.alchemyapi.io/v2/uanCKV5LOP7NtaVUos3qtH-R-V1xy-A3"))
                contract_ilv_eth = self.w3.eth.contract(address=self.pools_map['ilv-eth_pool'], abi=self.ABI)
                result = contract_ilv_eth.functions.pendingRewards(address).call()
            temp = pd.DataFrame([{'address': address, 'pendingRewards': result[0], 'pendingRewardsRevDis': result[1], 'pool': 'ilv-eth_pool'}])
            staked_ilv_eth_people = pd.concat([staked_ilv_eth_people, temp], axis=0)

        staked_with_rewards = pd.concat([staked_ilv_people, staked_ilv_eth_people], axis=0)
        staked_with_rewards['pendingRewards_ilv'] = staked_with_rewards.apply(lambda x: self.w3.fromWei(x['pendingRewards'],'ether'), axis=1)
        staked_with_rewards.drop(columns=['pendingRewardsRevDis'], inplace=True) # Droping this column because is all empty.

        list_of_files = os.listdir(DATA_PROCESSED)
        staked_with_rewards_files = len([file for file in list_of_files if f'staked_with_rewards_{pd.to_datetime(datetime.datetime.now()).date()}' in file])
        staked_with_rewards.to_csv(os.path.join(DATA_PROCESSED, f'staked_with_rewards_{pd.to_datetime(datetime.datetime.now()).date()}_{staked_with_rewards_files}.csv'), index=False)

        return staked_with_rewards.reset_index(drop=True)

    def _get_staked_and_unstaked_v1(self) -> Tuple([pd.DataFrame, pd.DataFrame]):
        """_summary_

        Returns:
            (pd.DataFrame, pd.Dataframe): _description_
        """        
        with open(os.path.join(ABI_PATH, 'abi_v1.json'), 'r') as f:
            ABI = json.load(f)

        # self.hard_block_start = self.dynamo.get_last_block(event='Staked', pool=self.pools_map['ilv_pool_v1'])
        self.hard_block_start = 12736200
        staked_ilv_v1 = self._make_events_df_per_pool(pool_address=self.pools_map['ilv_pool_v1'], pool_label='ilv_pool_v1', event_name='Staked', abi=ABI)
        # self.hard_block_start = self.dynamo.get_last_block(event='Staked', pool=self.pools_map['ilv-eth_pool_v1'])
        self.hard_block_start = 12736201
        staked_ilv_eth_v1 = self._make_events_df_per_pool(pool_address=self.pools_map['ilv-eth_pool_v1'], pool_label='ilv-eth_pool_v1', event_name='Staked', abi=ABI)

        staked_v1 = pd.concat([staked_ilv_v1, staked_ilv_eth_v1], axis=0)
        staked_v1['value_ilv'] = staked_v1.apply(lambda x: self.w3.fromWei(x['value'],'ether'), axis=1)

        # self.hard_block_start = self.dynamo.get_last_block(event='Unstaked', pool=self.pools_map['ilv_pool_v1'])
        self.hard_block_start = 12736200
        unstaked_ilv_v1 = self._make_events_df_per_pool(pool_address=self.pools_map['ilv_pool_v1'], pool_label='ilv_pool_v1', event_name='Unstaked', abi=ABI)
        # self.hard_block_start = self.dynamo.get_last_block(event='Unstaked', pool=self.pools_map['ilv-eth_pool_v1'])
        self.hard_block_start = 12736201
        unstaked_ilv_eth_v1 = self._make_events_df_per_pool(pool_address=self.pools_map['ilv-eth_pool_v1'], pool_label='ilv-eth_pool_v1', event_name='Unstaked', abi=ABI)

        unstaked_v1 = pd.concat([unstaked_ilv_v1, unstaked_ilv_eth_v1], axis=0)
        unstaked_v1['value_ilv'] = unstaked_v1.apply(lambda x: self.w3.fromWei(x['value'],'ether'), axis=1)

        list_of_files = os.listdir(DATA_PROCESSED)
        staked_v1_files = len([file for file in list_of_files if f'staked_v1_{pd.to_datetime(datetime.datetime.now()).date()}' in file])
        staked_v1.to_csv(os.path.join(DATA_PROCESSED, f'staked_v1_{pd.to_datetime(datetime.datetime.now()).date()}_{staked_v1_files}.csv'), index=False)

        unstaked_v1_files = len([file for file in list_of_files if f'unstaked_v1_{pd.to_datetime(datetime.datetime.now()).date()}' in file])
        unstaked_v1.to_csv(os.path.join(DATA_PROCESSED, f'unstaked_v1_{pd.to_datetime(datetime.datetime.now()).date()}_{unstaked_v1_files}.csv'), index=False)

        return staked_v1, unstaked_v1

    def _get_transfer_event(self) -> pd.DataFrame:
        """_summary_

        Returns:
            pd.DataFrame: _description_
        """        
        with open(os.path.join(ABI_PATH, 'abi_v1.json'), 'r') as f:
            ABI = json.load(f)

        # self.hard_block_start = self.dynamo.get_last_block(event='Transfer', pool=self.pools_map['ilv'])
        self.hard_block_start = 12084124
        transfers_ilv = self._make_events_df_per_pool(pool_address=self.pools_map['ilv'], pool_label='ilv', event_name='Transfer', abi=ABI)

        transfers_ilv['value_ilv'] = transfers_ilv.apply(lambda x: self.w3.fromWei(x['value'],'ether'), axis=1)

        list_of_files = os.listdir(DATA_PROCESSED)
        transfers_ilv_files = len([file for file in list_of_files if f'transfers_ilv_{pd.to_datetime(datetime.datetime.now()).date()}' in file])
        transfers_ilv.to_csv(os.path.join(DATA_PROCESSED, f'transfers_ilv_{pd.to_datetime(datetime.datetime.now()).date()}_{transfers_ilv_files}.csv'), index=False)
        
        return transfers_ilv

    def _get_stake_lock_updated_v1(self, staked_v1: pd.DataFrame):

        with open(os.path.join(ABI_PATH, 'abi_v1.json'), 'r') as f:
            ABI = json.load(f)

        # staked_v1 = self.dynamo.get_staked_v1_dataframe()

        # _____________________ ILV POOL _____________________

        staked_ilv_v1_deposit = pd.DataFrame(columns=['address', 'tokenAmount', 'weight', 'lockedFrom', 'lockedUntil', 'isYield', 'pool', 'deposit_id'])
        contract_ilv_v1 = self.w3.eth.contract(address=self.pools_map['ilv_pool_v1'], abi=ABI)
        
        for address in staked_v1.loc[staked_v1['pool'] == self.pools_map['ilv_pool_v1']]['wallet'].unique().tolist():
            for i in range(contract_ilv_v1.functions.getDepositsLength(address).call()):
                try:
                    result = contract_ilv_v1.functions.getDeposit(address, i).call()
                except HTTPError as err:
                    self.w3 = Web3(Web3.HTTPProvider("https://eth-mainnet.alchemyapi.io/v2/uanCKV5LOP7NtaVUos3qtH-R-V1xy-A3"))
                    contract_ilv_v1 = self.w3.eth.contract(address=self.pools_map['ilv_pool_v1'], abi=ABI)
                    result = contract_ilv_v1.functions.getDeposit(address, i).call()
                temp = pd.DataFrame([{'address': address, 'tokenAmount': result[0], 'weight': result[1], 'lockedFrom': result[2], 'lockedUntil': result[3], 'isYield': result[4], 'pool': 'ilv_pool_v1', 'deposit_id': i}])
                staked_ilv_v1_deposit = pd.concat([staked_ilv_v1_deposit, temp], axis=0)

        list_of_files = os.listdir(DATA_PROCESSED)
        staked_ilv_v1_deposit_files = len([file for file in list_of_files if f'staked_ilv_v1_deposit_{pd.to_datetime(datetime.datetime.now()).date()}' in file])
        staked_ilv_v1_deposit.to_csv(os.path.join(DATA_PROCESSED, f'staked_ilv_v1_deposit_{pd.to_datetime(datetime.datetime.now()).date()}_{staked_ilv_v1_deposit_files}.csv'), index=False)

        # _____________________ ILV-ETH POOL _____________________

        staked_ilv_eth_v1_deposit = pd.DataFrame(columns=['address', 'tokenAmount', 'weight', 'lockedFrom', 'lockedUntil', 'isYield', 'pool', 'deposit_id'])
        contract_ilv_eth_v1 = self.w3.eth.contract(address=self.pools_map['ilv-eth_pool_v1'], abi=ABI)
        
        for address in staked_v1.loc[staked_v1['pool'] == self.pools_map['ilv-eth_pool_v1']]['wallet'].unique().tolist():
            for i in range(contract_ilv_eth_v1.functions.getDepositsLength(address).call()):
                try:
                    result = contract_ilv_eth_v1.functions.getDeposit(address, i).call()
                except HTTPError as err:
                    self.w3 = Web3(Web3.HTTPProvider("https://eth-mainnet.alchemyapi.io/v2/uanCKV5LOP7NtaVUos3qtH-R-V1xy-A3"))
                    contract_ilv_eth_v1 = self.w3.eth.contract(address=self.pools_map['ilv-eth_pool_v1'], abi=ABI)
                    result = contract_ilv_eth_v1.functions.getDeposit(address, i).call()
                temp = pd.DataFrame([{'address': address, 'tokenAmount': result[0], 'weight': result[1], 'lockedFrom': result[2], 'lockedUntil': result[3], 'isYield': result[4], 'pool': 'ilv-eth_pool_v1', 'deposit_id': i}])
                staked_ilv_eth_v1_deposit = pd.concat([staked_ilv_eth_v1_deposit, temp], axis=0)

        staked_ilv_eth_v1_deposit_files = len([file for file in list_of_files if f'staked_ilv_eth_v1_deposit_{pd.to_datetime(datetime.datetime.now()).date()}' in file])
        staked_ilv_eth_v1_deposit.to_csv(os.path.join(DATA_PROCESSED, f'staked_ilv_eth_v1_deposit_{pd.to_datetime(datetime.datetime.now()).date()}_{staked_ilv_eth_v1_deposit_files}.csv'), index=False)

        return staked_ilv_eth_v1_deposit

    def _get_ilv_rewards_by_time(self):
        """
        This function calculates the total amount of ILV emitted take into account the decrease of 3% every two weeks.
        The 
        The transction that deploy the contract is: 0x2ac476d324e928f846595bc442cb2ecfae7c236f49e3815ea5a6ce1c4bbf32c0

        Returns:
            _type_: _description_
        """
        with open(os.path.join(ABI_PATH, 'abi_master.json'), 'r') as f:
            ABI = json.load(f)

        contract = self.w3.eth.contract(address=self.w3.toChecksumAddress(self.pools_map['master']), abi=ABI)
        ilv_per_second = contract.functions.ilvPerSecond().call()
        ilv_per_second = self.w3.fromWei(ilv_per_second,'ether')

        delta_time = datetime.timedelta(weeks=2)
        rewards_start_time = pd.to_datetime('2022-03-31 13:00:00')

        # We get the total of 2 weeks within now and the start of rewards.
        total_updates = int(round((pd.to_datetime(datetime.datetime.now()) - rewards_start_time) / delta_time, 0))

        total_amount = 0

        # We calculated the ILV emitted taking into account the 3% decrease every 2 weeks, having the current ILV per second.
        for i in range(total_updates, -1, -1):
            ilv_of_period = float(ilv_per_second) / ((1 - 0.03) ** (i+1))
            amount_period = (((rewards_start_time + delta_time*i) - rewards_start_time).total_seconds() * ilv_of_period) 
            total_amount += amount_period
        
        # After the last decrease, we calculate the remaining ILV emitted to now.
        final_amount = (pd.to_datetime(datetime.datetime.now()) - (rewards_start_time + delta_time*(total_updates))).total_seconds() * float(ilv_per_second)
        total_amount += final_amount

        result = pd.DataFrame(columns=['total_amount'])
        result.loc[0, 'total_amount'] = total_amount

        list_of_files = os.listdir(DATA_PROCESSED)
        ilv_rewards_by_time_files = len([file for file in list_of_files if f'ilv_rewards_by_time_{pd.to_datetime(datetime.datetime.now()).date()}' in file])
        result.to_csv(os.path.join(DATA_PROCESSED, f'ilv_rewards_by_time_{pd.to_datetime(datetime.datetime.now()).date()}_{ilv_rewards_by_time_files}.csv'), index=False)
        result.to_csv(os.path.join(DATA_RAW, 'ilv_rewards_downloaded.csv'), index=False)
        
        return total_amount

    def _get_pool_global_weights(self):
        contract = self.w3.eth.contract(address=self.pools_map['ilv_pool'], abi=self.ABI)
        global_weight = contract.functions.globalWeight().call()
        v1_global_weight = contract.functions.v1GlobalWeight().call()
        print(global_weight)
        print(v1_global_weight)



    def execute(self) -> Tuple([pd.DataFrame, pd.DataFrame, pd.DataFrame]):
        """This function exetutes the complete data fetching pipeline from blockchain to database and local file.

        Returns:
            pd.DataFrame: Returns the claims dataframe.
            pd.DataFrame: Returns the staked with rewards dataframe.
            pd.DataFrame: Returns the staked dataframe.
        """  
        log.info(f"Getting claims in v2 dataframe")
        claims = self._get_claims_v2()
        log.info(f"Getting staked in v2 dataframe")
        staked = self._get_staked_v2()
        log.info(f"Getting staked in v2 with pending rewards dataframe")
        staked_with_rewards = self._get_staked_with_pending_rewards_for_v2(staked=staked)
        log.info(f"Getting transfer events dataframe")
        transfer = self._get_transfer_event()
        log.info(f"Getting staked_v1 and unstaked_v1 events dataframe")
        staked_v1, unstaked_v1 = self._get_staked_and_unstaked_v1()
        log.info(f"Getting staked_ilv_eth_v1_deposit events dataframe")
        staked_ilv_eth_v1_deposit = self._get_stake_lock_updated_v1(staked_v1=staked_v1)

        athena = Athena()
        athena.save_data_table(df=claims, table='blockchain_claims_data', folder='blockchain/dev', s3_path='s3://ds-analytics-silver')
        athena.save_data_table(df=staked, table='blockchain_staked_data', folder='blockchain/dev', s3_path='s3://ds-analytics-silver')
        athena.save_data_table(df=staked_with_rewards, table='blockchain_staked_with_rewards_data', folder='blockchain/dev', s3_path='s3://ds-analytics-silver')
        athena.save_data_table(df=transfer, table='blockchain_transfer_data', folder='blockchain/dev', s3_path='s3://ds-analytics-silver')

        # log.info(f"Including the claims dataframe into Dynamo DB")
        # self.dynamo.include_claims_data(claims=claims)
        # log.info(f"Including the staked dataframe into Dynamo DB")
        # self.dynamo.include_staked_data(staked=staked)
        # log.info(f"Including the staked with pending rewards dataframe into Dynamo DB")
        # self.dynamo.include_staked_with_pending_rewards_data(staked_with_pending_rewards=staked_with_rewards)
        # log.info(f"Including transfer events dataframe into Dynamo DB")
        # self.dynamo.include_transfer_ilv_data(transfered=transfer)

        

        log.info(f"Saving the ILV rewards by time")
        self._get_ilv_rewards_by_time()

        return claims, staked_with_rewards, staked