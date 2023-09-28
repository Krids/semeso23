from typing import Dict, List
from src.features.feature_strategy import FeatureStrategy


class HighValueTransactionsStrategy(FeatureStrategy):

    def __init__(self, whale_limit: int):
        self.whale_limit = whale_limit

    def execute(self, data: List[Dict]) -> List[Dict]:
        output_data = []
        for transaction in data:
            transaction_copy = transaction.copy()
            transaction_copy['is_whale'] = transaction['value'] >= self.whale_limit
            output_data.append(transaction_copy)

        return output_data
