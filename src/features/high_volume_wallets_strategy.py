from collections import defaultdict
from typing import Dict, List
from src.features.feature_strategy import FeatureStrategy


class HighVolumeWalletsStrategy(FeatureStrategy):

    def execute(self, data: List[Dict]) -> List[Dict]:
        output_data = []

        # Aggregate volumes for each address
        address_volumes = defaultdict(int)
        for transaction in data:
            address_volumes[transaction['from']] -= transaction['value']  # For 'from', subtract the value (sent out)
            address_volumes[transaction['to']] += transaction['value']    # For 'to', add the value (received)

        # Let's say the top holders are the top 10% of all unique addresses
        top_percentile = 0.10
        sorted_addresses = sorted(address_volumes.keys(), key=lambda k: address_volumes[k], reverse=True)
        top_holders = set(sorted_addresses[:int(len(sorted_addresses) * top_percentile)])

        for transaction in data:
            transaction_copy = transaction.copy()
            transaction_copy['top_holder_from'] = transaction['from'] in top_holders
            transaction_copy['top_holder_to'] = transaction['to'] in top_holders
            output_data.append(transaction_copy)

        return output_data
