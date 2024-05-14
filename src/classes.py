import json
import csv
import requests
import re
import psycopg2


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

        host_name = 'localhost'
        port_num = '5432'
        database_name = 'hh_db'
        user_name = 'hh_bd_user'
        pwd = '654321'

        conn = psycopg2.connect(
            host=host_name,
            port=port_num,
            database=database_name,
            user=user_name,
            password=pwd)

        cur = conn.cursor()




        response = requests.get(self.api_url, params={'employer_id': employer_id, 'per_page': 100})
        print(response)
        print(f'Загружены данные по {employer_name}')

        vacancies = response.json()

        for vacancy in vacancies['items']:
            added_position = {}
            position = []

            position.append(int(vacancy['id']))
            position.append(vacancy['name'])
            position.append(vacancy['url'])

            if vacancy['salary'] == None:
                position.append(0)
                position.append(0)
                position.append("RUR")
                position.append(True)
            elif vacancy['salary']['from'] == None:
                position.append(0)
                position.append(int(vacancy['salary']['to']))
                position.append(vacancy['salary']['currency'])
                position.append(bool(vacancy['salary']['gross']))

            elif vacancy['salary']['to'] == None:
                position.append(int(vacancy['salary']['from']))
                position.append(0)
                position.append(vacancy['salary']['currency'])
                position.append(bool(vacancy['salary']['gross']))
            else:
                position.append(int(vacancy['salary']['from']))
                position.append(int(vacancy['salary']['to']))
                position.append(vacancy['salary']['currency'])
                position.append(bool(vacancy['salary']['gross']))
            if vacancy['address'] == None:
                position.append('-nd-')
            elif vacancy['address']['city'] == None:
                position.append('-nd-')
            else:
                position.append(vacancy['address']['city'])
            position.append(int(vacancy['employer']['id']))
            position.append(vacancy['snippet']['requirement'])
            position.append(vacancy['snippet']['responsibility'])

            position = tuple(position)
            print(position)

            cur.execute("""INSERT INTO vacancies (vacancy_id, vacancy_name, vacancy_url, salary_from, salary_to, currency, gross, address, employer_id, snippet_requirement, snippet_responsibility) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s) returning *""", position)
            conn.commit()


        cur.close



        # with open("../data/vacancies.csv", 'a', encoding='utf-8') as f:
        #     writer = csv.writer(f)
        #     writer.writerow(vacancies['items'])

        # with open("../data/vacancies.csv", 'w') as csv_file:
        #     writer = csv.writer(csv_file)
        #     for k, v in vacancies.items():
        #         writer.writerow([k, v])

# Откройте файл JSON и загрузите его содержимое в переменную с помощью open('test.json', 'r')
# в качестве json_file: json_data = json.load(json_file)
# Создайте новый CSV-файл и запишите заголовки используя
# open('test.csv', 'w', newline=") как csv_file:
# writer = csv.writer(csv_file)
# writer.C(json_data[0].keys())
# Записать каждую строку данных из файла JSON в файл CSV для строки в json_data: writer.writerow(row.values())



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

        response = requests.get(self.api_url, params={'text': search_text, 'per_page': 100})
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


        with open(self.raw_file, 'rt', encoding='utf-8') as rf:
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

                # added_employer['employer_id'] = vacancy['employer']['id']
                # added_employer['employer_name'] = vacancy['employer']['name']
                # added_employer['employer_url'] = vacancy['employer']['url']
                #
                # employers_to_sourse.append(added_employer) #собираем данные по работодателям


        with open(self.vacancy_file, 'wt', encoding='utf-8') as vf:
            json.dump(vacancies_to_source, vf, ensure_ascii=False)

        # with open(self.employer_file, 'wt', encoding='utf-8') as ef:
        #     json.dump(employers_to_sourse, ef, ensure_ascii=False)

class Vacancy():
    """класс объекта Вакансия"""

    id: int
    name: str
    url: str
    salary: dict
    address: dict
    employer: dict
    snippet: dict

    def __init__(self, id, name, url, salary, address, employer_id, snippet) -> None:

        self.id = id
        self.name = name
        self.url = url
        if address == None:
            self.address = {'city': '-'}
        else:
            self.address = address

        self.employer = employer
        self.snippet = snippet
        if salary == None:
            self.salary = {'from': 0, 'to': 0, "currency": "RUR", "gross": True}
        else:
            self.salary = salary

    def __repr__(self) -> str:
        return f'{self.id}, {self.name}, {self.url}, {self.salary}, {self.address},{self.employer},{self.snippet}'

    def __str__(self) -> str:

        return f'{self.id}, {self.name} в "{self.employer['name']}" с доходом от {self.salary['from']} до {self.salary['to']} в городе {self.address['city']}. Ссылка: {self.url}. Требования: {self.snippet['requirement']}'

class DBManager():
    """ Класс для работы с данными в БД """

    def __init__(self):
        pass

    def get_companies_and_vacancies_count():
        """получает список всех компаний и количество вакансий у каждой компании"""

        pass

    def get_all_vacancies():
        """получает список всех вакансий с указанием названия компании,
            названия вакансии и зарплаты и ссылки на вакансию."""
        pass

    def get_avg_salary():
        """получает среднюю зарплату по вакансиям."""

        pass

    def get_vacancies_with_higher_salary():
        """получает список всех вакансий, у которых зарплата выше средней по всем вакансиям."""

        pass

    def get_vacancies_with_keyword():
        """получает список всех вакансий, в названии которых содержатся
            переданные в метод слова, например python."""
        pass
