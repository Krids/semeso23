from collections import defaultdict
from typing import Dict, List
from src.features.feature_strategy import FeatureStrategy


class RecurringWalletTransactionsStrategy(FeatureStrategy):

    def execute(self, data: List[Dict]) -> List[Dict]:
        output_data = []
        
        # Count the transaction occurrences between pairs
        pair_count = defaultdict(int)
        for transaction in data:
            pair = (transaction['from'], transaction['to'])
            pair_count[pair] += 1
        
        for transaction in data:
            pair = (transaction['from'], transaction['to'])
            transaction_copy = transaction.copy()
            transaction_copy['recurring_transaction'] = pair_count[pair] > 1
            output_data.append(transaction_copy)

        return output_data
