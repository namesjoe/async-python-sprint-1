import logging
import json
from http import HTTPStatus
from urllib.request import urlopen
from queue import Queue
from threading import Thread
from utils import CITIES
from external.client import YandexWeatherAPI

logger = logging.getLogger()


class DataFetchingTask:
    def __init__(self):
        self.queue = Queue()
        self.weather_results = {}

    @staticmethod
    def get_weather(city, url):
        try:
            data = YandexWeatherAPI.get_forecasting(url)
            return data
        except Exception as er:
            logger.error(f"Failed to get {city}. error: {er}")

    def worker(self):
        while True:
            city, url = self.queue.get()
            self.get_weather(city, url)
            self.queue.task_done()

    def worker(self):
        while True:
            city, url = self.queue.get()
            weather_data = self.get_weather(city, url)
            self.weather_results[city] = weather_data
            self.queue.task_done()
    def __call__(self, *args, **kwargs):
        workers = 10

        for _ in range(workers):
            thread = Thread(target=self.worker)
            thread.daemon = True
            thread.start()

        for city, url in CITIES.items():
            self.queue.put((city, url))

        self.queue.join()

class DataCalculationTask:
    pass


class DataAggregationTask:
    pass


class DataAnalyzingTask:
    pass


if __name__ == "__main__":
    dft = DataFetchingTask()
    dft()
    print(1)