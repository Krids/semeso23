from collections import defaultdict
from typing import Dict, List
from src.features.feature_strategy import FeatureStrategy


class ValueConcentrationStrategy(FeatureStrategy):

    def __init__(self, N):
        self.N = N

    def execute(self, data: List[Dict]) -> List[Dict]:
        output_data = []
        
        # Calculate total value for each address
        address_values = defaultdict(int)
        for transaction in data:
            address_values[transaction['from']] += transaction['value']
        
        # Identify top N addresses
        sorted_addresses = sorted(address_values.items(), key=lambda x: x[1], reverse=True)
        top_addresses = set([address[0] for address in sorted_addresses[:self.N]])
        
        for transaction in data:
            transaction_copy = transaction.copy()
            transaction_copy['value_concentration_from'] = transaction['from'] in top_addresses
            output_data.append(transaction_copy)

        return output_data
