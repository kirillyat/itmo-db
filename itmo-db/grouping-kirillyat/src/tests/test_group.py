import pytest
from group import aggregate_csv

def test_sum_aggregation():
    # Проверка агрегации суммы
    result = aggregate_csv("./src/tests/static/data.csv", 1, 2, "sum")
    expected = {1: 18.299999999999997, 2: 22.8, 3: 16.2}
    assert result == expected

def test_max_aggregation():
    # Проверка агрегации максимума
    result = aggregate_csv("./src/tests/static/data.csv", 1, 2, "max")
    expected = {1: 7.8, 2: 9.6, 3: 6.7}
    assert result == expected

def test_min_aggregation():
    # Проверка агрегации минимума
    result = aggregate_csv("./src/tests/static/data.csv", 1, 2, "min")
    expected = {1: 2.2, 2: 3.1, 3: 2.5}
    assert result == expected


def test_count_aggregation():
    # Проверка агрегации счетчика
    result = aggregate_csv("./src/tests/static/data.csv", 1, 2, "count")
    expected = {1: 4, 2: 4, 3: 4}
    assert result == expected

# def test_big_min_aggregation():
#     # Проверка агрегации минимума на большом файле
#     result = aggregate_csv("./src/tests/static/bigdata.csv", 1, 2, "min")
#     expected = {1: 1}
#     assert result == expected

