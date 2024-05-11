import json
import csv
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

    def get_vacancies(self, employer_id, employer_name) -> None:
        """ метод позволяющий запрашивать записи с сайта hh, содержащие текст search_text и записывать его в файл в формате json """

        response = requests.get(self.api_url, params={'employer_id': employer_id, 'per_page': 100})
        print(response)
        print(f'Загружены данные по {employer_name}')

        vacancies = response.json()

        with open('../data/raw_vacancies.json', 'a', encoding='utf-8') as data_file:
            json.dump(vacancies['items'], data_file, ensure_ascii=False)
        #
        # with open("../data/vacancies.csv", 'a', encoding='utf-8') as f:
        #     writer = csv.writer(f)
        #     writer.writerow(vacancies['items'])






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
        """ метод позволяющий запрашивать записи с сайта hh, содержащие
        текст search_text и записывать его в файл в формате json """

        response = requests.get(self.api_url, params={'text': search_text})
        print(response)
        print(response.status_code)

        employers = response.json()

        return employers

        # with open(raw_employers_file_path, 'wt', encoding='utf-8') as data_file:
        #     json.dump(employers['items'], data_file, ensure_ascii=False)
        #

class Vacancies_File():

    def __init__(self, raw_file, vacancy_file, employer_file) -> None:

        self.raw_file = raw_file
        self.vacancy_file = vacancy_file
        self.employer_file = employer_file

    def from_raw_file(self) -> None:
        """  метод, который позволяет из исходного файла c вакансиями,
            полученного импортом с сайта hh.ru, сделать файл с нужными данными
        """

        vacancies_to_source = []
        employers_to_sourse = []


        with (open(self.raw_file, 'rt', encoding='utf-8') as rf):
            vacancies = json.load(rf)

            for vacancy in vacancies:
                added_position = {}
                added_employer = {}
                added_address = {}

                added_position['vacancy_id'] = vacancy['id']
                added_position['vacancy_name'] = vacancy['name']
                added_position['vacancy_url'] = vacancy['url']
                if vacancy['address'] == None:
                    added_position['address'] = '-nd-'
                elif vacancy['address']['city'] == None:
                    added_position['address'] = '-nd-'
                else:
                    added_position['address'] = (vacancy['address']['city'])
                if vacancy['salary'] == None:
                    added_position['salary_from'] = 0
                    added_position['salary_to'] = 0
                    added_position['currency'] = "RUR"
                    added_position['gross'] = True

                elif vacancy['salary']['from'] == None:
                    added_position['salary_from'] = 0
                elif vacancy['salary']['to'] == None:
                    added_position['salary_to'] = 0
                else:
                    added_position['salary_from'] = vacancy['salary']['from']
                    added_position['salary_to'] = vacancy['salary']['to']
                    added_position['currency'] = "RUR"
                    added_position['gross'] = True

                added_position['employer_id'] = vacancy['employer']['id']
                added_position['snippet_requirement'] = vacancy['snippet']['requirement']
                added_position['snippet_responsibility'] = vacancy['snippet']['responsibility']

                vacancies_to_source.append(added_position) #собираем данные по вакансиям

                added_employer['employer_id'] = vacancy['employer']['id']
                added_employer['employer_name'] = vacancy['employer']['name']
                added_employer['employer_url'] = vacancy['employer']['url']

                employers_to_sourse.append(added_employer) #собираем данные по работодателям


        with open(self.vacancy_file, 'wt', encoding='utf-8') as vf:
            json.dump(vacancies_to_source, vf, ensure_ascii=False)

        with open(self.employer_file, 'wt', encoding='utf-8') as ef:
            json.dump(vacancies_to_source, ef, ensure_ascii=False)
