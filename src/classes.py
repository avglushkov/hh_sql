import json
import csv
import requests
import re
import psycopg2


from abc import ABC, abstractmethod
from typing import Any
from operator import itemgetter

# # Параметры подключения к БД работодателей и вакансий
# host_name = 'localhost'
# port_num = '5432'
# database_name = 'hh_db'
# user_name = 'hh_bd_user'
# pwd = '654321'

class Abs_APIVacancy(ABC):
    """ Абстрактный класс для одъектов класса вакансия и его наследников """
    @abstractmethod
    def __init__(self) -> None:
        pass

class From_hh_api_vacancies(Abs_APIVacancy):
    """ Класс для запроса данных по вакансиям с сайта hh.ru """


    def __init__(self, host_name, port_num, database_name, user_name, pwd) -> None:
        self.api_url = 'https://api.hh.ru/vacancies'
        self.host_name = host_name
        self.port_num = port_num
        self.database_name = database_name
        self.user_name = user_name
        self.pwd = pwd

    def get_vacancies(self, employer_id, employer_name, search_text) -> None:
        """ метод позволяющий запрашивать записи с сайта hh по ID работодателя и записываем их в БД """

        conn = psycopg2.connect(
            host = self.host_name,
            port = self.port_num,
            database = self.database_name,
            user = self.user_name,
            password = self.pwd)

        cur = conn.cursor()
        # Забираем данные с сайта hh.ru для работодателя по его ID
        response = requests.get(self.api_url, params={'text': search_text,'employer_id': employer_id, 'per_page': 100})
        print(response)
        print(f'Загружены данные по {employer_name}')

        vacancies = response.json()

        # вакансии построчно считываем, переводим в нужный формат и грузим в таблицу БД vacancies

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




class Abs_APIEmployer(ABC):
    """ Абстрактный класс для одъектов класса работадатель и его наследников """
    @abstractmethod
    def __init__(self) -> None:
        pass
class From_hh_api_employers(Abs_APIEmployer):
    """ Класс для запроса списка работодателей с сайта hh.ru и загрузки в БД  """

    def __init__(self, host_name, port_num, database_name, user_name, pwd) -> None:
        self.api_url = 'https://api.hh.ru/employers'
        self.host_name = host_name
        self.port_num = port_num
        self.database_name = database_name
        self.user_name = user_name
        self.pwd = pwd


    def get_employers(self, search_text) -> list:
        """ метод запрашивающий список работодателей, содержащих в наименовании
        текст search_text """

        response = requests.get(self.api_url, params={'text': search_text, 'per_page': 100})
        print(response)
        print(response.status_code)

        employers = response.json()

        return employers

    def write_employers_into_db(self) -> None:
        """ Метод записи данных о работодателей в БД из списка, сформированного пользователем"""

        conn = psycopg2.connect(
            host = self.host_name,
            port = self.port_num,
            database = self.database_name,
            user = self.user_name,
            password = self.pwd)

        cur = conn.cursor()

        cur.execute("TRUNCATE TABLE employers RESTART IDENTITY CASCADE")

        with open('../data/employers_list.csv', 'rt', encoding='utf-8') as f:
            reader = csv.reader(f)
            for row in reader:
                cur.execute(
                    "INSERT INTO employers (employer_id, employer_name, employer_url) VALUES (%s, %s, %s) returning *",
                    row)

        conn.commit()
        cur.close


class Vacancies_File():

    def __init__(self, raw_file, vacancy_file, employer_file) -> None:

        self.raw_file = raw_file
        self.vacancy_file = vacancy_file
        self.employer_file = employer_file


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

    def __init__(self, host_name, port_num, database_name, user_name, pwd) -> None:

        self.host_name = host_name
        self.port_num = port_num
        self.database_name = database_name
        self.user_name = user_name
        self.pwd = pwd

    def truncate_vacancies_table(self) -> None:
        """ очищает таблицу вакансий"""

        conn = psycopg2.connect(
            host = self.host_name,
            port = self.port_num,
            database = self.database_name,
            user = self.user_name,
            password = self.pwd)

        cur = conn.cursor()

        cur.execute("TRUNCATE TABLE vacancies RESTART IDENTITY CASCADE")
        conn.commit()
        cur.close
    def get_companies_and_vacancies_count(self) -> list:
        """получает список всех компаний и количество вакансий у каждой компании"""

        emp_count = []

        conn = psycopg2.connect(
            host = self.host_name,
            port = self.port_num,
            database = self.database_name,
            user = self.user_name,
            password = self.pwd)

        cur = conn.cursor()

        cur.execute("""select employers.employer_name, COUNT(*) from vacancies
                    join employers on vacancies.employer_id = employers.employer_id
                    group by employer_name;""")

        emp_count = cur.fetchall()

        cur.execute("""select count(*) from vacancies;""")
        vacancies_count = cur.fetchall()

        conn.commit()
        cur.close

        return emp_count , vacancies_count

    def get_all_vacancies(self) -> list:
        """получает список всех вакансий с указанием названия компании,
            названия вакансии и зарплаты и ссылки на вакансию."""

        vacancies_list = []

        conn = psycopg2.connect(
            host = self.host_name,
            port = self.port_num,
            database = self.database_name,
            user = self.user_name,
            password = self.pwd)

        cur = conn.cursor()

        cur.execute("""SELECT 
                            employers.employer_name, 
                            vacancy_name,
                            salary_from, 
                            salary_to,
                            currency,
                            gross,
                            vacancy_url
                        FROM vacancies
                        JOIN employers ON vacancies.employer_id = employers.employer_id;""")

        vacancies_list = cur.fetchall()

        conn.commit()
        cur.close

        return vacancies_list

    def get_avg_salary(self):
        """получает среднюю зарплату по вакансиям."""

        salary_list = []

        conn = psycopg2.connect(
            host = self.host_name,
            port = self.port_num,
            database = self.database_name,
            user = self.user_name,
            password = self.pwd)

        cur = conn.cursor()

        cur.execute("""SELECT salary_from, salary_to FROM vacancies;""")

        salary_list = cur.fetchall()

        conn.commit()
        cur.close

        salary_from_sum = 0
        salary_to_sum = 0
        salary_from_count = 0
        salary_to_count = 0

        for salary in salary_list:
            if salary[0] != 0:
                salary_from_sum += salary[0]
                salary_from_count += 1
            if salary[1] != 0:
                salary_to_sum += salary[1]
                salary_to_count += 1

        if salary_from_count == 0:
            average_salary_from = 0
        else:
            average_salary_from = salary_from_sum / salary_from_count

        if salary_to_count == 0:
            average_salary_to = 0
        else:
            average_salary_to = salary_to_sum / salary_to_count

        return average_salary_from, average_salary_to


    def get_vacancies_with_higher_salary(self, average_salary_from, average_salary_to) -> list:
        """получает список всех вакансий, у которых зарплата выше средней по всем вакансиям."""

        vacancies = []
        vacancies_from = []
        vacancies_to = []

        conn = psycopg2.connect(
            host = self.host_name,
            port = self.port_num,
            database = self.database_name,
            user = self.user_name,
            password = self.pwd)

        cur = conn.cursor()

        cur.execute("""SELECT       employers.employer_name, 
                                    vacancy_name,
                                    salary_from, 
                                    salary_to,
                                    currency,
                                    gross,
                                    vacancy_url
                                FROM vacancies
                                JOIN employers ON vacancies.employer_id = employers.employer_id
                                ;""")

        vacancies = cur.fetchall()
        conn.commit()
        cur.close

        for vacancy in vacancies:
            if vacancy[2] > average_salary_from:
                vacancies_from.append(vacancy)
            if vacancy[3] > average_salary_to:
                vacancies_to.append(vacancy)

        return vacancies_from, vacancies_to

    def get_vacancies_with_keyword(self, key_word):
        """получает список всех вакансий, в названии которых содержатся
            переданные в метод слова, например python."""


        vacancies_with_word = []

        conn = psycopg2.connect(
            host=self.host_name,
            port=self.port_num,
            database=self.database_name,
            user=self.user_name,
            password=self.pwd)

        cur = conn.cursor()

        cur.execute("""SELECT       employers.employer_name, 
                                            vacancy_name,
                                            salary_from, 
                                            salary_to,
                                            currency,
                                            gross,
                                            vacancy_url,
                                            snippet_requirement,
                                            snippet_responsibility
                                        FROM vacancies
                                        JOIN employers ON vacancies.employer_id = employers.employer_id
                                        ;""")

        vacancies = cur.fetchall()
        conn.commit()
        cur.close

        for vacancy in vacancies:
            if key_word.lower() in vacancy[1].lower():
                vacancies_with_word.append(vacancy)

        return vacancies_with_word

# employer = DBManager()
# print(employer.get_avg_salary())
