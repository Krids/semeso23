import pytest
from src.features.recurring_wallet_transactions_strategy import RecurringWalletTransactionsStrategy

@pytest.mark.parametrize("input_data, expected_output", [
    ([{'blockNumber': 1, 'from': 'A', 'to': 'B', 'value': 1000}, 
      {'blockNumber': 2, 'from': 'A', 'to': 'B', 'value': 2000},
      {'blockNumber': 3, 'from': 'A', 'to': 'D', 'value': 1500}],
     [{'blockNumber': 1, 'from': 'A', 'to': 'B', 'value': 1000, 'recurring_transaction': True},
      {'blockNumber': 2, 'from': 'A', 'to': 'B', 'value': 2000, 'recurring_transaction': True},
      {'blockNumber': 3, 'from': 'A', 'to': 'D', 'value': 1500, 'recurring_transaction': False}]),
])
def test_recurring_transactions_strategy(input_data, expected_output):
    strategy = RecurringWalletTransactionsStrategy()
    result = strategy.execute(input_data)
    assert result == expected_output
