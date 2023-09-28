from collections import defaultdict
from typing import Dict, List
from src.features.feature_strategy import FeatureStrategy


class BlockTransactionsCountStrategy(FeatureStrategy):

    def execute(self, data: List[Dict]) -> List[Dict]:
        output_data = []
        block_counts = defaultdict(int)
        for transaction in data:
            block_counts[transaction['blockNumber']] += 1

        for transaction in data:
            transaction_copy = transaction.copy()
            transaction_copy['num_transactions'] = block_counts[transaction['blockNumber']]
            output_data.append(transaction_copy)

        return output_data
