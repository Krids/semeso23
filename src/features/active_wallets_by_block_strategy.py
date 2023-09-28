from collections import defaultdict
from typing import Dict, List
from src.features.feature_strategy import FeatureStrategy


class ActiveWalletsByBlockStrategy(FeatureStrategy):

    def execute(self, data: List[Dict]) -> List[Dict]:
        output_data = []
        block_wallets = defaultdict(set)

        for transaction in data:
            block_wallets[transaction['blockNumber']].add(transaction['from'])
            block_wallets[transaction['blockNumber']].add(transaction['to'])

        for transaction in data:
            transaction_copy = transaction.copy()
            transaction_copy['active_wallets'] = len(block_wallets[transaction['blockNumber']])
            output_data.append(transaction_copy)

        return output_data
