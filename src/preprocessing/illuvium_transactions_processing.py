import os
from typing import Dict, List

from src.utils.json_handdler import JsonHanddler
from src.utils.project_paths import DATA_RAW


class IlluviumTransactionsProcessing:

    def __init__(self, blockchain) -> None:
        self.blockchain = blockchain
        self.json_handdler = JsonHanddler()

    def remove_duplicates_by_field(self, field: str, data: List[Dict]) -> List[Dict]:
        """This funtion is responsible to take a field from a list of
        dicts and remove all dicts that have that field duplicated.

        Args:
            field (str): The features that couldn't be duplicate.
            data (List[Dict]): The list of dicts.

        Returns:
            List[Dict]: The list of dicts without the duplicated in the field.
        """    
        seen_ids = set()
        data = [seen_ids.add(x[field]) or x for x in data if x[field] not in seen_ids]
        return sorted(data, key=lambda x: x['blockNumber'])

    def create_ilv_value_field(self, data: List[Dict]) -> List[Dict]:
        """Transform the value in wei to ether.

        Args:
            data (List[Dict]): The transactions list.

        Returns:
            List[Dict]: The transactions list with the new field.
        """    
        data = [{**transaction, 'value_ilv': self.blockchain.w3.from_wei(transaction['value'],'ether')} for transaction in data]
        return data

    def create_timestamp_field(self, data: List[Dict], backup_file=os.path.join(DATA_RAW, "backup_transfer_timestamp.json")) -> List[Dict]:
        """Create a timestamp from the block number.

        Args:
            data (List[Dict]): The transactions list.

        Returns:
            List[Dict]: The transactions list with the new field.
        """
        # Load existing data if available
        processed_data = self.json_handdler.load_from_json(backup_file)
        
        # If data is available, find out where to start from
        start_idx = 0
        if processed_data:
            # Encontre a última transação com 'timestamp' no backup
            last_timestamped_transaction = next((trans for trans in reversed(processed_data) if 'timestamp' in trans), None)
            
            if last_timestamped_transaction:
                # Encontre o índice desta transação na lista 'data'
                start_idx = next((idx for idx, transaction in enumerate(data) if transaction['blockNumber'] == last_timestamped_transaction['blockNumber']), 0) + 1
                data = processed_data
            else:
                # Se nenhuma transação no backup tiver um 'timestamp', comece do início
                data = processed_data + data
        try:
            for idx, transaction in enumerate(data[start_idx:], start=start_idx):
                timestamp = self.blockchain.get_block_timestamp(transaction['blockNumber'])
                transaction['timestamp'] = timestamp
                # Optionally, save progress intermittently, e.g., every 100 transactions
                if idx % 100 == 0:
                    self.json_handdler.save_to_json(data, backup_file)
            # Save at the end
            self.json_handdler.save_to_json(data, backup_file)
        except KeyboardInterrupt:
            print("Interrupted. Saving progress...")
            self.json_handdler.save_to_json(data, backup_file)

        return data
