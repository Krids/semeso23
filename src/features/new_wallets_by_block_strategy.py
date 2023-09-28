from collections import Counter, defaultdict
from typing import Dict, List
from src.features.feature_strategy import FeatureStrategy


class NewWalletsByBlockStrategy(FeatureStrategy):

    def execute(self, data: List[Dict]) -> List[Dict]:
        output_data = []

        # List all addresses and the blocks in which they appear
        address_to_blocks = defaultdict(set)
        for transaction in data:
            address_to_blocks[transaction['from']].add(transaction['blockNumber'])
            address_to_blocks[transaction['to']].add(transaction['blockNumber'])

        # Determine the first appearance of each address
        address_first_appearance = {addr: min(blocks) for addr, blocks in address_to_blocks.items()}

        # Count the number of new addresses per block
        new_wallet_counts = Counter(address_first_appearance.values())

        for transaction in data:
            transaction_copy = transaction.copy()
            transaction_copy['new_wallets'] = new_wallet_counts[transaction['blockNumber']]
            output_data.append(transaction_copy)

        return output_data
