from collections import defaultdict
from typing import Dict, List
from src.features.feature_strategy import FeatureStrategy


class RatioValueBlockStrategy(FeatureStrategy):

    def execute(self, data: List[Dict]) -> List[Dict]:
        output_data = []
        
        # Aggregate values for each block
        block_values = defaultdict(int)
        for transaction in data:
            block_values[transaction['blockNumber']] += transaction['value']
        
        for transaction in data:
            transaction_copy = transaction.copy()
            transaction_copy['value_per_block'] = block_values[transaction['blockNumber']]
            output_data.append(transaction_copy)

        return output_data
