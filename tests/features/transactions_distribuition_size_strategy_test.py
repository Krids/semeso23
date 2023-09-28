import pytest
from src.features.transactions_distribuition_size_strategy import TransactionsDistribuitionSizeStrategy

@pytest.mark.parametrize("input_data, expected_output", [
    ([{'blockNumber': 1, 'value': 300}, {'blockNumber': 1, 'value': 1000}, {'blockNumber': 2, 'value': 2000}],
     [{'blockNumber': 1, 'value': 300, 'size_category': 'small'}, {'blockNumber': 1, 'value': 1000, 'size_category': 'medium'}, {'blockNumber': 2, 'value': 2000, 'size_category': 'large'}]),
])
def test_transaction_size_distribution(input_data, expected_output):
    strategy = TransactionsDistribuitionSizeStrategy()
    result = strategy.execute(input_data)
    assert result == expected_output
