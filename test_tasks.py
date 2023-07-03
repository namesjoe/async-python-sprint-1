import pytest
import pandas as pd
import numpy as np
from tasks import DataCalculationTask, DataAggregationTask, DataAnalyzingTask


@pytest.fixture
def task_data():
    data = {
        "MOSCOW": {
            "forecasts": [
                {
                    "date": "2023-07-02",
                    "hours": [
                        {"hour": "9", "condition": "partly-cloud", "temp": 25},
                        {"hour": "10", "condition": "clear", "temp": 28},
                        {"hour": "11", "condition": "clear", "temp": 28},
                        {"hour": "12", "condition": "clear", "temp": 28},
                        {"hour": "13", "condition": "clear", "temp": 28},
                        {"hour": "14", "condition": "clear", "temp": 28},
                        {"hour": "15", "condition": "clear", "temp": 28},
                        {"hour": "16", "condition": "clear", "temp": 23},
                        {"hour": "17", "condition": "clear", "temp": 33},
                        {"hour": "18", "condition": "clear", "temp": 28},
                        {"hour": "19", "condition": "clear", "temp": 28},
                        {"hour": "20", "condition": "clear", "temp": 28},
                        {"hour": "21", "condition": "partly-cloud", "temp": 25},
                        {"hour": "22", "condition": "clear", "temp": 23},
                        {"hour": "23", "condition": "clear", "temp": 28},
                        {"hour": "8", "condition": "clear", "temp": 28},
                        {"hour": "7", "condition": "clear", "temp": 11},
                        {"hour": "6", "condition": "clear", "temp": 28},
                        {"hour": "5", "condition": "clear", "temp": 12},
                        {"hour": "4", "condition": "clear", "temp": 33},
                        {"hour": "3", "condition": "clear", "temp": 23},
                        {"hour": "2", "condition": "clear", "temp": 28},
                        {"hour": "1", "condition": "clear", "temp": 23},
                        {"hour": "0", "condition": "clear", "temp": 28},
                    ],
                },
                {
                    "date": "2023-07-01",
                    "hours": [
                        {"hour": "10", "condition": "clear", "temp": 28},
                    ],
                },
            ]
        },
        "PARIS": {
            "forecasts": [
                {
                    "date": "2023-07-01",
                    "hours": [
                        {"hour": "9", "condition": "partly-cloud", "temp": 26},
                    ],
                },
                {
                    "date": "2023-07-02",
                    "hours": [
                        {"hour": "9", "condition": "partly-cloud", "temp": 25},
                        {"hour": "10", "condition": "rainy", "temp": 22},
                        {"hour": "11", "condition": "clear", "temp": 28},
                        {"hour": "12", "condition": "clear", "temp": 28},
                        {"hour": "13", "condition": "clear", "temp": 28},
                        {"hour": "14", "condition": "rainy", "temp": 28},
                        {"hour": "15", "condition": "clear", "temp": 28},
                        {"hour": "16", "condition": "clear", "temp": 23},
                        {"hour": "17", "condition": "rainy", "temp": 33},
                        {"hour": "18", "condition": "clear", "temp": 28},
                        {"hour": "19", "condition": "rainy", "temp": 28},
                        {"hour": "20", "condition": "clear", "temp": 28},
                        {"hour": "21", "condition": "partly-cloud", "temp": 25},
                        {"hour": "22", "condition": "clear", "temp": 23},
                        {"hour": "23", "condition": "clear", "temp": 28},
                        {"hour": "8", "condition": "rainy", "temp": 28},
                        {"hour": "7", "condition": "clear", "temp": 11},
                        {"hour": "6", "condition": "clear", "temp": 28},
                        {"hour": "5", "condition": "clear", "temp": 12},
                        {"hour": "4", "condition": "rainy", "temp": 33},
                        {"hour": "3", "condition": "clear", "temp": 23},
                        {"hour": "2", "condition": "clear", "temp": 28},
                        {"hour": "1", "condition": "clear", "temp": 23},
                        {"hour": "0", "condition": "clear", "temp": 28},
                    ],
                },
            ]
        },
    }
    return data


def test_get_city_temp(task_data):

    task = DataCalculationTask(task_data)

    result = task.get_city_temp("MOSCOW", forecast_hours=[9, 10, 11])
    expected_one = {
        "2023-07-02": [
            {"condition": "partly-cloud", "temp": 25},
            {"condition": "clear", "temp": 28},
            {"condition": "clear", "temp": 28},
        ]
    }
    assert result == expected_one

    expected_two = {
        "2023-07-02": [
            {"condition": "partly-cloud", "temp": 25},
            {"condition": "rainy", "temp": 22},
            {"condition": "clear", "temp": 28},
        ]
    }
    result = task.get_city_temp("PARIS", forecast_hours=[9, 10, 11])
    print(f"{result=}")
    assert result == expected_two


def test_good_conditions_counter_n2():
    task = DataCalculationTask(None)
    hours_data = [
        {"condition": "partly-cloud", "temp": 25},
        {"condition": "clear", "temp": 28},
        {"condition": "rainy", "temp": 23},
    ]

    count = task.good_conditions_counter(hours_data)

    assert count == 2


def test_good_conditions_counter_n3():
    task = DataCalculationTask(None)
    hours_data = [
        {"condition": "partly-cloud", "temp": 25},
        {"condition": "clear", "temp": 28},
        {"condition": "clear", "temp": 23},
    ]

    count = task.good_conditions_counter(hours_data)

    assert count == 3


def test_avg_temp():
    task = DataCalculationTask(None)
    hours_data = [
        {"condition": "partly-cloud", "temp": 25},
        {"condition": "clear", "temp": 28},
        {"condition": "overcast", "temp": 23},
    ]

    avg_temp = task.avg_temp(hours_data)
    assert avg_temp == 25.333333333333332


def test_summarize_weather(task_data):
    task = DataCalculationTask(task_data)

    result = task.summarize_weather("MOSCOW")
    expected = {
        "MOSCOW": [
            {
                "date": "2023-07-02",
                "weather_data": {
                    "avg_temp": 27.727272727272727,
                    "n_hours_good_weather": 11,
                },
            }
        ]
    }
    assert result == expected


@pytest.fixture
def task_data_2():
    data = {
        "MOSCOW": [
            {
                "date": "2023-07-02",
                "weather_data": {
                    "avg_temp": 27.0,
                    "n_hours_good_weather": 3,
                },
            },
            {
                "date": "2023-07-03",
                "weather_data": {
                    "avg_temp": 25.0,
                    "n_hours_good_weather": 4,
                },
            },
        ],
        "PARIS": [
            {
                "date": "2023-07-02",
                "weather_data": {
                    "avg_temp": 26.0,
                    "n_hours_good_weather": 2,
                },
            },
            {
                "date": "2023-07-03",
                "weather_data": {
                    "avg_temp": 24.0,
                    "n_hours_good_weather": 5,
                },
            },
        ],
    }
    return data


def test_process_chunk(task_data_2):
    task = DataAggregationTask(None)
    chunk = [("MOSCOW", task_data_2["MOSCOW"]), ("PARIS", task_data_2["PARIS"])]

    df = task.process_chunk(chunk)

    assert isinstance(df, pd.DataFrame)
    assert len(df) == 2
    assert df.columns.tolist() == [
        "Город",
        "Температура, средняя (2023-07-02)",
        "Температура, средняя (2023-07-03)",
        "Без осадков, часов (2023-07-02)",
        "Без осадков, часов (2023-07-03)",
    ]
    assert df.values.tolist() == [
        ["MOSCOW", 27.0, 25.0, 3.0, 4.0],
        ["PARIS", 26.0, 24.0, 2.0, 5.0],
    ]


def test_process_data(task_data_2):
    task = DataAggregationTask(task_data_2)

    task.process_data(workers=1)

    assert isinstance(task.df, pd.DataFrame)
    assert len(task.df) == 2
    assert task.df.columns.tolist() == [
        "Город",
        "Температура, средняя (2023-07-02)",
        "Температура, средняя (2023-07-03)",
        "Без осадков, часов (2023-07-02)",
        "Без осадков, часов (2023-07-03)",
        "Среднее количество часов без осадков",
        "Средняя Температура",
    ]
    assert task.df.values.tolist() == [
        ["MOSCOW", 27.0, 25.0, 3.0, 4.0, 3.5, 26.0],
        ["PARIS", 26.0, 24.0, 2.0, 5.0, 3.5, 25.0],
    ]


@pytest.fixture
def task_df():
    data = [
        ["MOSCOW", 27.0, 25.0, 3.0, 4.0, 3.5, 26.0],
        ["PARIS", 26.0, 24.0, 2.0, 5.0, 3.5, 25.0],
    ]
    columns = [
        "Город",
        "Температура, средняя (2023-07-02)",
        "Температура, средняя (2023-07-03)",
        "Без осадков, часов (2023-07-02)",
        "Без осадков, часов (2023-07-03)",
        "Среднее количество часов без осадков",
        "Средняя Температура",
    ]
    df = pd.DataFrame(data, columns=columns)
    return df


def test_analyze_data_single_city(task_df):
    task = DataAnalyzingTask(task_df)

    result = task.analyze_data()

    assert result == "MOSCOW"


def test_analyze_data_multiple_cities(task_df):
    task_df.at[1, "Среднее количество часов без осадков"] = 3.5
    task = DataAnalyzingTask(task_df)

    result = task.analyze_data()

    assert result == "MOSCOW"
