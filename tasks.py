import pandas as pd
from queue import Queue
import numpy as np
from threading import Thread
from utils import CITIES
from external.client import YandexWeatherAPI
import concurrent.futures
from my_logger import logger
from typing import List, Dict, Union


class DataFetchingTask:
    def __init__(self):
        self.queue = Queue()
        self.weather_results = {}

    @staticmethod
    def get_weather(url) -> Dict:
        data = YandexWeatherAPI.get_forecasting(url)
        return data

    def worker(self):
        while True:
            task = self.queue.get()
            city, url = task
            try:
                weather_data = self.get_weather(url)
                self.weather_results[city] = weather_data
            except BaseException as e:
                logger.error(f"Ошибка по городоу {city}: {str(e)}")
            finally:
                self.queue.task_done()

    def get_all_cities_weather(self, *args, **kwargs):
        workers = 10

        for _ in range(workers):
            thread = Thread(target=self.worker)
            thread.daemon = True
            thread.start()

        for city, url in CITIES.items():
            self.queue.put((city, url))

        self.queue.join()


class DataCalculationTask:
    def __init__(self, data: Dict):
        self.all_data = data
        self.weather_summary = {}

    def get_city_temp(self, city: str, forecast_hours=tuple(range(9, 20))) -> Dict:
        result = {}
        try:
            city_data = self.all_data[city]
        except KeyError:
            logger.error(f"Не удалось получить прогноз по {city}")
            return result

        try:
            forecasts = city_data["forecasts"]
        except KeyError:
            logger.error(f"По городу {city} отсутсвует в данных поле forecasts")
            return result

        for forecast_ in forecasts:
            date = forecast_["date"]
            if len(forecast_["hours"]) < 24:
                continue
            result[date] = []
            for hour_data in forecast_["hours"]:
                if int(hour_data["hour"]) in forecast_hours:
                    result[date].append(
                        {"condition": hour_data["condition"], "temp": hour_data["temp"]}
                    )
        return result

    @staticmethod
    def good_conditions_counter(hours_data: List) -> int:
        right_conditions = ("partly-cloud", "clear", "cloudy", "overcast")
        count = 0
        for hour_data in hours_data:
            if hour_data["condition"] in right_conditions:
                count += 1
        return count

    @staticmethod
    def avg_temp(hours_data: List) -> Union[int, float]:
        avg_temp = sum([i["temp"] for i in hours_data]) / len(hours_data)
        return avg_temp

    def summarize_weather(self, city: str) -> Dict:
        """считаем среднюю температуру по городу и количество часов без осадков"""
        result = []
        try:
            city_data = self.get_city_temp(city)
        except KeyError:
            logger.error(f"Ошибка с подсчетом средней температуры в городе {city}")
            return {}

        for dt, forecast in city_data.items():
            n_hours_with_good_weather = self.good_conditions_counter(
                hours_data=forecast
            )
            avg_temp = self.avg_temp(hours_data=forecast)
            weather_data = {
                "avg_temp": avg_temp,
                "n_hours_good_weather": n_hours_with_good_weather,
            }
            result.append({"date": dt, "weather_data": weather_data})

        return {city: result}

    def run_concurrent(self, cities: List):
        with concurrent.futures.ProcessPoolExecutor() as executor:
            results = executor.map(self.summarize_weather, cities)

        for result in results:
            self.weather_summary.update(result)


class DataAggregationTask:
    def __init__(self, data: Dict):
        self.data = data

    @staticmethod
    def process_chunk(chunk: List) -> pd.DataFrame:
        data = []
        dates = set()

        for city, forecasts in chunk:
            avg_temps = []
            good_weather_hours = []
            for forecast in forecasts:
                date = forecast["date"]
                avg_temp = forecast["weather_data"]["avg_temp"]
                n_hours_with_good_weather = forecast["weather_data"][
                    "n_hours_good_weather"
                ]
                avg_temps.append(avg_temp)
                good_weather_hours.append(n_hours_with_good_weather)
                dates.add(date)
            data.append([city] + avg_temps + good_weather_hours)

        dates = sorted(list(dates))
        columns = (
            ["Город"]
            + [f"Температура, средняя ({date})" for date in dates]
            + [f"Без осадков, часов ({date})" for date in dates]
        )
        df = pd.DataFrame(data, columns=columns)
        return df

    def process_data(self, workers=4):
        items = list(self.data.items())
        chunk_size = len(items) // workers
        chunks = [items[i : i + chunk_size] for i in range(0, len(items), chunk_size)]

        with concurrent.futures.ProcessPoolExecutor(max_workers=workers) as executor:
            results = list(executor.map(self.process_chunk, chunks))

        merged_results = pd.concat(results, ignore_index=True).fillna("")

        without_precipitation_columns = [
            col for col in merged_results.columns if "Без осадков" in col
        ]
        merged_results["Среднее количество часов без осадков"] = (
            merged_results[without_precipitation_columns]
            .replace("", np.nan)
            .mean(axis=1)
        )
        mean_temp_cols = [col for col in merged_results.columns if "Температура" in col]
        merged_results["Средняя Температура"] = (
            merged_results[mean_temp_cols].replace("", np.nan).mean(axis=1)
        )
        self.df = merged_results

    def save_to_csv(self, filename):
        self.df.to_csv(filename, index=False)


class DataAnalyzingTask:
    def __init__(self, df: pd.DataFrame):
        self.df = df

    def analyze_data(self) -> str:
        max_avg_temp = self.df["Средняя Температура"].max()

        cities_with_max_temp = self.df[self.df["Средняя Температура"] == max_avg_temp]

        if len(cities_with_max_temp) == 1:
            best_city = cities_with_max_temp["Город"].tolist()[0]
        else:
            max_precipitation_free_days = cities_with_max_temp[
                "Среднее количество часов без осадков"
            ].max()
            best_cities = cities_with_max_temp[
                cities_with_max_temp["Среднее количество часов без осадков"]
                == max_precipitation_free_days
            ]["Город"].tolist()
            best_city = ", ".join(best_cities)

        return best_city
