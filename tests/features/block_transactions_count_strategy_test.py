import pytest
from src.features.block_transactions_count_strategy import BlockTransactionsCountStrategy

@pytest.mark.parametrize("input_data, expected_output", [
    ([{'blockNumber': 1}, {'blockNumber': 1}, {'blockNumber': 2}],
     [{'blockNumber': 1, 'num_transactions': 2}, {'blockNumber': 1, 'num_transactions': 2}, {'blockNumber': 2, 'num_transactions': 1}]),
])
def test_transactions_per_block(input_data, expected_output):
    strategy = BlockTransactionsCountStrategy()
    result = strategy.execute(input_data)
    assert result == expected_output
