import pytest
from src.features.new_wallets_by_block_strategy import NewWalletsByBlockStrategy

@pytest.mark.parametrize("input_data, expected_output", [
    ([{'blockNumber': 1, 'from': 'A', 'to': 'B'}, {'blockNumber': 2, 'from': 'B', 'to': 'C'}, {'blockNumber': 3, 'from': 'A', 'to': 'D'}],
     [{'blockNumber': 1, 'from': 'A', 'to': 'B', 'new_wallets': 2}, {'blockNumber': 2, 'from': 'B', 'to': 'C', 'new_wallets': 1}, {'blockNumber': 3, 'from': 'A', 'to': 'D', 'new_wallets': 1}]),
])
def test_new_wallets_per_block(input_data, expected_output):
    strategy = NewWalletsByBlockStrategy()
    result = strategy.execute(input_data)
    assert result == expected_output
