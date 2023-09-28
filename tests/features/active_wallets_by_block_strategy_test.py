import pytest
from src.features.active_wallets_by_block_strategy import ActiveWalletsByBlockStrategy

@pytest.mark.parametrize("input_data, expected_output", [
    ([{'blockNumber': 1, 'from': 'A', 'to': 'B'}, {'blockNumber': 1, 'from': 'B', 'to': 'C'}, {'blockNumber': 2, 'from': 'A', 'to': 'D'}],
     [{'blockNumber': 1, 'from': 'A', 'to': 'B', 'active_wallets': 3}, {'blockNumber': 1, 'from': 'B', 'to': 'C', 'active_wallets': 3}, {'blockNumber': 2, 'from': 'A', 'to': 'D', 'active_wallets': 2}]),
])
def test_active_wallets_per_block(input_data, expected_output):
    strategy = ActiveWalletsByBlockStrategy()
    result = strategy.execute(input_data)
    assert result == expected_output
