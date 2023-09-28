import pytest
from src.features.block_total_transfered_strategy import BlockTotalTransferedStrategy

@pytest.mark.parametrize("input_data, expected_output", [
    ([{'blockNumber': 1, 'value': 5}, {'blockNumber': 1, 'value': 5}, {'blockNumber': 2, 'value': 10}],
     [{'blockNumber': 1, 'value': 5, 'total_coins': 10}, {'blockNumber': 1, 'value': 5, 'total_coins': 10}, {'blockNumber': 2, 'value': 10, 'total_coins': 10}]),
])
def test_total_coins_per_block(input_data, expected_output):
    strategy = BlockTotalTransferedStrategy()
    result = strategy.execute(input_data)
    assert result == expected_output
