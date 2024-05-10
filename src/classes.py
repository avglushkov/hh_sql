import json
import requests
import re


from abc import ABC, abstractmethod
from typing import Any
from operator import itemgetter


class Abs_APIVacancy(ABC):
    """ Абстрактный класс для одъектов класса вакансия и его наследников """
    @abstractmethod
    def __init__(self) -> None:
        pass

class From_hh_api_vacancies(Abs_APIVacancy):
    """ Класс для запроса данных по вакансиям с сайта hh.ru """

    def __init__(self) -> None:
        self.api_url = 'https://api.hh.ru/vacancies'

    def get_vacancies(self, search_text, vacancies_number, raw_file_path) -> None:
        """ метод позволяющий запрашивать записи с сайта hh, содержащие текст search_text и записывать его в файл в формате json """

        response = requests.get(self.api_url, params={'text': search_text, 'per_page': vacancies_number})
        print(response)
        print(response.status_code)

        vacancies = response.json()

        with open(raw_file_path, 'wt', encoding='utf-8') as data_file:
            json.dump(vacancies['items'], data_file, ensure_ascii=False)

class Abs_APIEmployer(ABC):
    """ Абстрактный класс для одъектов класса работадатель и его наследников """
    @abstractmethod
    def __init__(self) -> None:
        pass
class From_hh_api_employers(Abs_APIEmployer):
    """ Класс для запроса данных по работодателям с сайта hh.ru """

    def __init__(self) -> None:
        self.api_url = 'https://api.hh.ru/employers'

    def get_employers(self, search_text) -> list:
        """ метод позволяющий запрашивать записи с сайта hh, содержащие текст search_text и записывать его в файл в формате json """

        response = requests.get(self.api_url, params={'text': search_text})
        print(response)
        print(response.status_code)

        employers = response.json()

        return employers

        # with open(raw_employers_file_path, 'wt', encoding='utf-8') as data_file:
        #     json.dump(employers['items'], data_file, ensure_ascii=False)
        #

# class Employers_File():
#
#     def __init__(self, raw_employers_file_path, source_employers_file_path) -> None:
#
#         self.raw_employers_file_path = raw_employers_file_path
#         self.source_employers_file_path = source_employers_file_path
#
#     def from_raw_file(self) -> None:
#         """  метод, который позволяет из исходного файла c работадателями, полученного импортом с сайта hh.ru,
#         """
#
#         vacancies_to_source = []
#
#         with open(self.raw_file_path, 'rt', encoding='utf-8') as raw_file:
#             vacancies = json.load(raw_file)
#
#             for vacancy in vacancies:
#                 added_position = {}
#                 added_position['id'] = vacancy['id']
#                 added_position['name'] = vacancy['name']
#                 added_position['url'] = vacancy['url']
#                 added_position['address'] = (vacancy['address'])
#                 if vacancy['salary'] == None:
#                     added_position['salary'] = {'from': 0, 'to': 0, "currency": "RUR", "gross": True}
#                 elif vacancy['salary']['from'] == None:
#                     added_position['salary'] = {'from': 0, 'to': vacancy['salary']['to'], "currency": "RUR", "gross": True}
#                 else:
#                     added_position['salary'] = vacancy['salary']
#                 added_position['employer'] = vacancy['employer']
#                 added_position['snippet'] = vacancy['snippet']
#
#                 vacancies_to_source.append(added_position)
#
#         with open(self.source_file_path, 'wt', encoding='utf-8') as source_file:
#             json.dump(vacancies_to_source, source_file, ensure_ascii=False)
