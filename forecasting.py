from tasks import (
    DataFetchingTask,
    DataCalculationTask,
    DataAggregationTask,
    DataAnalyzingTask,
)
from utils import CITIES


def forecast_weather():
    """
    Анализ погодных условий по городам
    """
    cities = list(CITIES.keys())

    # Task 1: Получите информацию о погодных условиях для указанного списка городов
    data_fetch = DataFetchingTask()
    data_fetch.get_all_cities_weather()
    all_weather_dics = (
        data_fetch.weather_results
    )  # Словарь с информацией о погоде по городам

    # Task 2: Вычислите среднюю температуру и проанализируйте информацию об осадках за указанный период для всех городов
    # Передаем результаты из первой таски, это странная архитектура но так подразумевается задачей. В целом можно было
    # добавить четкое описание архитектуры
    calculation_task = DataCalculationTask(data=all_weather_dics)
    calculation_task.run_concurrent(cities=cities)
    weather_summary = calculation_task.weather_summary  # средняя температура и осадки

    # Task 3: Объедините полученные данные и сохраните результат в текстовом файле
    agg_task = DataAggregationTask(data=weather_summary)
    agg_task.process_data()
    agg_task.save_to_csv(filename="task3.csv")  # сохраняю в файл
    df_result = agg_task.df  # результат в pd.DataFrame для таски 4

    # Task 4: Проанализируйте результат и сделайте вывод, какой из городов наиболее благоприятен для поездки.
    data_analysis = DataAnalyzingTask(df=df_result)
    best_city = data_analysis.analyze_data()
    print(f"Для путешествий {best_city=}")


if __name__ == "__main__":
    forecast_weather()
