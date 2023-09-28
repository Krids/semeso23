import pytest
from src.features.ratio_value_block_strategy import RatioValueBlockStrategy

@pytest.mark.parametrize("input_data, expected_output", [
    ([{'blockNumber': 1, 'from': 'A', 'to': 'B', 'value': 1000}, {'blockNumber': 1, 'from': 'B', 'to': 'C', 'value': 2000}, {'blockNumber': 2, 'from': 'A', 'to': 'D', 'value': 1500}],
     [{'blockNumber': 1, 'from': 'A', 'to': 'B', 'value': 1000, 'value_per_block': 3000},
      {'blockNumber': 1, 'from': 'B', 'to': 'C', 'value': 2000, 'value_per_block': 3000},
      {'blockNumber': 2, 'from': 'A', 'to': 'D', 'value': 1500, 'value_per_block': 1500}]),
])
def test_value_per_block_strategy(input_data, expected_output):
    strategy = RatioValueBlockStrategy()
    result = strategy.execute(input_data)
    assert result == expected_output
