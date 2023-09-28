from collections import defaultdict
from typing import Dict, List
from src.features.feature_strategy import FeatureStrategy


class BlockTotalTransferedStrategy(FeatureStrategy):

    def execute(self, data: List[Dict]) -> List[Dict]:
        output_data = []
        block_totals = defaultdict(int)
        for transaction in data:
            block_totals[transaction['blockNumber']] += transaction['value']

        for transaction in data:
            transaction_copy = transaction.copy()
            transaction_copy['total_coins'] = block_totals[transaction['blockNumber']]
            output_data.append(transaction_copy)

        return output_data
