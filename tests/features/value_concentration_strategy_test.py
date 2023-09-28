import pytest
from src.features.value_concentration_strategy import ValueConcentrationStrategy

@pytest.mark.parametrize("input_data, N, expected_output", [
    ([{'blockNumber': 1, 'from': 'A', 'to': 'B', 'value': 1000},
      {'blockNumber': 2, 'from': 'A', 'to': 'C', 'value': 1500},
      {'blockNumber': 3, 'from': 'B', 'to': 'C', 'value': 500}],
     1,
     [{'blockNumber': 1, 'from': 'A', 'to': 'B', 'value': 1000, 'value_concentration_from': True},
      {'blockNumber': 2, 'from': 'A', 'to': 'C', 'value': 1500, 'value_concentration_from': True},
      {'blockNumber': 3, 'from': 'B', 'to': 'C', 'value': 500, 'value_concentration_from': False}]),
])
def test_value_concentration_strategy(input_data, N, expected_output):
    strategy = ValueConcentrationStrategy(N)
    result = strategy.execute(input_data)
    assert result == expected_output
