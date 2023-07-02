import logging
import os
import pandas as pd
from queue import Queue
import numpy as np
from threading import Thread
from utils import CITIES
from external.client import YandexWeatherAPI
from datetime import date
import concurrent.futures


today = date.today().isoformat()  # YYYY-MM-DD

logs_dir = f'./logs/{today}'
if not os.path.exists(logs_dir):
    os.makedirs(logs_dir)

log_filename = f'{logs_dir}/tasks.log'
logging.basicConfig(filename=log_filename, level=logging.INFO,
                    format='%(asctime)s %(levelname)s %(name)s %(message)s')

logger = logging.getLogger(__name__)


class DataFetchingTask:
    def __init__(self):
        self.queue = Queue()
        self.weather_results = {}

    @staticmethod
    #@timeout_decorator.timeout(5)  # таймаут 3 сек
    def get_weather(url):
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
                #logger.error(f"Ошибка по городоу {city}: {str(e)}")
                pass
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


class DataCalculationTask():
    def __init__(self, data):
        self.all_data = data
        self.weather_summary = {}  # self.manager.list()

    def get_city_temp(self,
        city, forecast_hours: list = list(range(9, 20))
    ):
        result = {}
        try:
            city_data = self.all_data[city]
        except KeyError:
            return result

        try:
            forecasts = city_data["forecasts"]
        except KeyError:
            return result

        for forecast_ in forecasts:
            date = forecast_["date"]
            if len(forecast_["hours"]) < 24:
                continue
            result[date] = []
            for hour_data in forecast_["hours"]:
                if int(hour_data["hour"]) in forecast_hours:
                    result[date].append({"condition": hour_data["condition"],
                                      "temp": hour_data["temp"]})
        return result

    @staticmethod
    def good_conditions_counter(hours_data: list):
        right_conditions = ('partly-cloud', 'clear', 'cloudy', 'overcast')
        count = 0
        for hour_data in hours_data:
            if hour_data["condition"] in right_conditions:
                count += 1
        return count

    @staticmethod
    def avg_temp(hours_data: list):
        avg_temp = sum([i['temp'] for i in hours_data])/len(hours_data)
        return avg_temp


    def summarize_weather(self, city) -> list[dict]:
        """ считаем среднюю температуру по городу и количество часов без осадков
        Возвращает список словарей типа
        -> [{'date': date, 'weather_data': {'avg_temp':N, 'n_hours_good_weather':N}}]"""
        result = []
        try:
            city_data = self.get_city_temp(city)
        except KeyError:
            return result

        for dt, forecast in city_data.items():
            n_hours_with_good_weather = self.good_conditions_counter(hours_data=forecast)
            avg_temp = self.avg_temp(hours_data=forecast)
            weather_data = {
                "avg_temp": avg_temp,
                "n_hours_good_weather": n_hours_with_good_weather
            }
            result.append({"date": dt, "weather_data": weather_data})

        return {city: result}

    def run_concurrent(self, cities):
        with concurrent.futures.ProcessPoolExecutor() as executor:
            results = executor.map(self.summarize_weather, cities)

        for result in results:
            self.weather_summary.update(result)


class DataAggregationTask:

    def __init__(self, data):
        self.data = data

    @staticmethod
    def process_chunk(chunk):
        data = []
        dates = set()

        for city, forecasts in chunk:
            avg_temps = []
            good_weather_hours = []
            for forecast in forecasts:
                date = forecast['date']
                avg_temp = forecast['weather_data']['avg_temp']
                n_hours_with_good_weather = forecast['weather_data']['n_hours_good_weather']
                avg_temps.append(avg_temp)
                good_weather_hours.append(n_hours_with_good_weather)
                dates.add(date)
            data.append([city] + avg_temps + good_weather_hours)

        dates = sorted(list(dates))
        columns = ['Город'] + [f'Температура, средняя ({date})' for date in dates] + [f'Без осадков, часов ({date})' for
                                                                                      date in dates]

        df = pd.DataFrame(data, columns=columns)
        return df

    def process_data(self, workers=4):
        items = list(self.data.items())
        chunk_size = len(items) // workers
        chunks = [items[i:i + chunk_size] for i in range(0, len(items), chunk_size)]

        with concurrent.futures.ProcessPoolExecutor(max_workers=workers) as executor:
            results = list(executor.map(self.process_chunk, chunks))

        merged_results = pd.concat(results, ignore_index=True).fillna("")

        without_precipitation_columns = [col for col in merged_results.columns if "Без осадков" in col]
        merged_results["Среднее количество часов без осадков"] = merged_results[without_precipitation_columns].replace("", np.nan).mean(axis=1)
        mean_temp_cols = [col for col in merged_results.columns if "Температура" in col]
        merged_results["Средняя Температура"] = merged_results[mean_temp_cols].replace("", np.nan).mean(
            axis=1)
        self.df = merged_results

    def save_to_csv(self, filename):
        self.df.to_csv(filename, index=False)

class DataAnalyzingTask:
    def __init__(self, df):
        self.df = df

    def analyze_data(self):
        max_avg_temp = self.df["Средняя Температура"].max()

        cities_with_max_temp = self.df[self.df["Средняя Температура"] == max_avg_temp]

        if len(cities_with_max_temp) == 1:
            best_city = cities_with_max_temp["Город"].tolist()[0]
        else:
            max_precipitation_free_days = cities_with_max_temp["Среднее количество часов без осадков"].max()
            best_cities = cities_with_max_temp[
                cities_with_max_temp["Среднее количество часов без осадков"] == max_precipitation_free_days][
                "Город"].tolist()
            best_city = ", ".join(best_cities)

        return best_city


if __name__ == "__main__":
    dft = DataFetchingTask()
    dft.get_all_cities_weather()
    calc_task = DataCalculationTask(data=dft.weather_results)
    moscow = calc_task.get_city_temp("MOSCOW")
    # for city in CITIES:
    #     city_data = calc_task.get_city_temp(city=city)
    cities = list(CITIES.keys())
    calc_task.run_concurrent(cities=cities)
    summary_data = calc_task.weather_summary
    chunk = [('MOSCOW', [{'date': '2022-05-26', 'weather_data': {'avg_temp': 17.727272727272727, 'n_hours_good_weather': 7}}, {'date': '2022-05-27', 'weather_data': {'avg_temp': 13.090909090909092, 'n_hours_good_weather': 0}}, {'date': '2022-05-28', 'weather_data': {'avg_temp': 12.181818181818182, 'n_hours_good_weather': 0}}]), ('PARIS', [{'date': '2022-05-26', 'weather_data': {'avg_temp': 17.636363636363637, 'n_hours_good_weather': 9}}, {'date': '2022-05-27', 'weather_data': {'avg_temp': 17.363636363636363, 'n_hours_good_weather': 11}}, {'date': '2022-05-28', 'weather_data': {'avg_temp': 17.0, 'n_hours_good_weather': 11}}]), ('LONDON', [{'date': '2022-05-26', 'weather_data': {'avg_temp': 17.363636363636363, 'n_hours_good_weather': 11}}, {'date': '2022-05-27', 'weather_data': {'avg_temp': 16.272727272727273, 'n_hours_good_weather': 11}}, {'date': '2022-05-28', 'weather_data': {'avg_temp': 14.636363636363637, 'n_hours_good_weather': 11}}]), ('BERLIN', [{'date': '2022-05-26', 'weather_data': {'avg_temp': 19.272727272727273, 'n_hours_good_weather': 9}}, {'date': '2022-05-27', 'weather_data': {'avg_temp': 16.0, 'n_hours_good_weather': 6}}, {'date': '2022-05-28', 'weather_data': {'avg_temp': 13.636363636363637, 'n_hours_good_weather': 0}}])]
    agg_task = DataAggregationTask(data=summary_data)
    res = agg_task.process_chunk(chunk)
    agg_task.process_data()
    agg_task.save_to_csv(filename="./sprint_1.csv")
    df = agg_task.df
    analyzer = DataAnalyzingTask(df)
    best_city = analyzer.analyze_data()

    print("Favorable cities:", best_city)
