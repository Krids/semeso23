from typing import Dict, List
from src.features.feature_strategy import FeatureStrategy


class TransactionsDistribuitionSizeStrategy(FeatureStrategy):

    def execute(self, data: List[Dict]) -> List[Dict]:
        output_data = []
        for transaction in data:
            transaction_copy = transaction.copy()
            value = transaction['value']
            if value <= 500:
                transaction_copy['size_category'] = 'small'
            elif value <= 1500:
                transaction_copy['size_category'] = 'medium'
            else:
                transaction_copy['size_category'] = 'large'
            output_data.append(transaction_copy)
        return output_data
