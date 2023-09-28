import pytest
from src.features.high_value_transactions_strategy import HighValueTransactionsStrategy

@pytest.mark.parametrize("input_data, whale_limit, expected_output", [
    ([{'blockNumber': 1, 'value': 500}, {'blockNumber': 1, 'value': 1500}, {'blockNumber': 2, 'value': 2000}],
     1000,
     [{'blockNumber': 1, 'value': 500, 'is_whale': False}, {'blockNumber': 1, 'value': 1500, 'is_whale': True}, {'blockNumber': 2, 'value': 2000, 'is_whale': True}]),
])
def test_whale_transactions(input_data, whale_limit, expected_output):
    strategy = HighValueTransactionsStrategy(whale_limit)
    result = strategy.execute(input_data)
    assert result == expected_output
