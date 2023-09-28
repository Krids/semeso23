import pytest
from src.features.high_volume_wallets_strategy import HighVolumeWalletsStrategy

@pytest.mark.parametrize("input_data, expected_output", [
    ([{'blockNumber': 1, 'from': 'A', 'to': 'B', 'value': 1000}, {'blockNumber': 2, 'from': 'B', 'to': 'C', 'value': 500}, {'blockNumber': 3, 'from': 'A', 'to': 'D', 'value': 1500}],
     [{'blockNumber': 1, 'from': 'A', 'to': 'B', 'value': 1000, 'top_holder_from': True, 'top_holder_to': False},
      {'blockNumber': 2, 'from': 'B', 'to': 'C', 'value': 500, 'top_holder_from': False, 'top_holder_to': False},
      {'blockNumber': 3, 'from': 'A', 'to': 'D', 'value': 1500, 'top_holder_from': True, 'top_holder_to': False}]),
])
def test_top_holders_strategy(input_data, expected_output):
    strategy = HighVolumeWalletsStrategy()
    result = strategy.execute(input_data)
    assert result == expected_output
