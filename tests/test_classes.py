""" Модуль тестирования классов """

from src.classes import FromHHAPIEmployers, FromHHAPIVacancies, DBManager
from configparser import ConfigParser

import pytest
import json
import psycopg2
import psycopg2.extras
import csv
import os

filename='../src/database.ini'
section='postgresql'
def load_test_config(filename, section):
    """ Функция загрузки параметров подключения к БД """

    parser = ConfigParser()
    parser.read(filename)
    config = []
    params = parser.items(section)
    for param in params:
        config.append(param[1])

    return config

host_name = load_test_config(filename,section)[0]
port_num = load_test_config(filename,section)[1]
database_name = load_test_config(filename,section)[2]
user_name = load_test_config(filename,section)[3]
pwd = load_test_config(filename,section)[4]


def test_get_vacancies():
    """проверяем работоспособность загрузки данных """

    truncate_table = DBManager(host_name, port_num, database_name, user_name, pwd)
    truncate_table.truncate_vacancies_table()

    get_api = FromHHAPIVacancies(host_name, port_num, database_name, user_name, pwd)
    vacancies = get_api.get_vacancies('3776', 'МТС', 'SRE')

    assert len(vacancies) > 0

def test_get_employers():
    """ Проверка запроса списка работодателей"""

    emp = FromHHAPIEmployers(host_name, port_num, database_name, user_name, pwd)
    employers = emp.get_employers('SRE')

    assert len(employers) > 0

def test_write_employers_into_db():
    """ Проверяем загрузку данных по работодателям в таблицу БД.
        !!!Обратите внимание, что в файле employers_list.csv должна быть хотя бы одна строка """

    emp = DBManager(host_name, port_num, database_name, user_name, pwd)
    employers = emp.write_employers_into_db()

    conn = psycopg2.connect(
        host = host_name,
        port = port_num,
        database = database_name,
        user = user_name,
        password = pwd)

    cur = conn.cursor()
    cur.execute(
        """SELECT COUNT(*) FROM employers""")
    result = cur.fetchall()
    conn.commit()
    cur.close

    assert result[0][0] > 0

@pytest.fixture()
def data_vacancies():

    vacancies = [(1,'Вакансия1 DevOps', 'http://1.hh.ru', 100, 200, 'RUR', True, 'Москва', 3776, 'Требования', 'Ответственность'),
                (2,'Вакансия2 SRE', 'http://2.hh.ru', 0, 0, 'RUR', True, 'Москва', 3776, 'Требования', 'Ответственность'),
                (3,'Вакансия3 Agile', 'http://3.hh.ru', 200, 0, 'RUR', True, 'Москва', 3776, 'Требования', 'Ответственность')]

    return vacancies

def test_get_companies_and_vacancies_count(data_vacancies):
    """ Проверка метода вывода количества загруженных вакансий по работодателям"""

    truncate_table = DBManager(host_name, port_num, database_name, user_name, pwd)
    truncate_table.truncate_vacancies_table()

    conn = psycopg2.connect(
        host = host_name,
        port = port_num,
        database = database_name,
        user = user_name,
        password = pwd)

    cur = conn.cursor()
    for row in data_vacancies:
        cur.execute("""INSERT INTO vacancies 
                                (vacancy_id, 
                                vacancy_name, 
                                vacancy_url, 
                                salary_from, 
                                salary_to, 
                                currency, 
                                gross, 
                                address, 
                                employer_id, 
                                snippet_requirement, 
                                snippet_responsibility) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s) returning *""", row)
        conn.commit()
    cur.close


    vac = DBManager(host_name, port_num, database_name, user_name, pwd)
    vacancies = vac.get_companies_and_vacancies_count()

    assert vacancies[0][0][0] == 'МТС'
    assert vacancies[0][0][1] > 0

def test_get_all_vacancies(data_vacancies):
    """ Проверка метода вывода всех вакансий из таблицы БД """

    truncate_table = DBManager(host_name, port_num, database_name, user_name, pwd)
    truncate_table.truncate_vacancies_table()

    conn = psycopg2.connect(
        host = host_name,
        port = port_num,
        database = database_name,
        user = user_name,
        password = pwd)

    cur = conn.cursor()
    for row in data_vacancies:
        cur.execute("""INSERT INTO vacancies 
                                (vacancy_id, 
                                vacancy_name, 
                                vacancy_url, 
                                salary_from, 
                                salary_to, 
                                currency, 
                                gross, 
                                address, 
                                employer_id, 
                                snippet_requirement, 
                                snippet_responsibility) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s) returning *""", row)
        conn.commit()
    cur.close

    vac = DBManager(host_name, port_num, database_name, user_name, pwd)
    vacancies = vac.get_all_vacancies()

    assert vacancies[0][0] == 'МТС'

def test_get_avg_salary(data_vacancies):
    """ Проверка метода вывода среднего уровня ЗП по вакансиям в БД """

    truncate_table = DBManager(host_name, port_num, database_name, user_name, pwd)
    truncate_table.truncate_vacancies_table()

    conn = psycopg2.connect(
        host = host_name,
        port = port_num,
        database = database_name,
        user = user_name,
        password = pwd)

    cur = conn.cursor()
    for row in data_vacancies:
        cur.execute("""INSERT INTO vacancies 
                                (vacancy_id, 
                                vacancy_name, 
                                vacancy_url, 
                                salary_from, 
                                salary_to, 
                                currency, 
                                gross, 
                                address, 
                                employer_id, 
                                snippet_requirement, 
                                snippet_responsibility) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s) returning *""", row)
        conn.commit()
    cur.close

    vac = DBManager(host_name, port_num, database_name, user_name, pwd)
    salary = vac.get_avg_salary()
    print(salary)

    assert int(salary[0]) == 150
    assert int(salary[1]) == 200

def test_get_vacancies_with_higher_salary(data_vacancies):
    """ Проверка метода вывода вакансий с ЗП выше среднего """

    truncate_table = DBManager(host_name, port_num, database_name, user_name, pwd)
    truncate_table.truncate_vacancies_table()

    conn = psycopg2.connect(
        host = host_name,
        port = port_num,
        database = database_name,
        user = user_name,
        password = pwd)

    cur = conn.cursor()
    for row in data_vacancies:
        cur.execute("""INSERT INTO vacancies 
                                (vacancy_id, 
                                vacancy_name, 
                                vacancy_url, 
                                salary_from, 
                                salary_to, 
                                currency, 
                                gross, 
                                address, 
                                employer_id, 
                                snippet_requirement, 
                                snippet_responsibility) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s) returning *""", row)
        conn.commit()
    cur.close

    vac = DBManager(host_name, port_num, database_name, user_name, pwd)
    salary = vac.get_avg_salary()

    vac = DBManager(host_name, port_num, database_name, user_name, pwd)
    vacancies = vac.get_vacancies_with_higher_salary(salary[0],salary[1])
    print(salary)

    assert len(vacancies[0]) == 1
    assert len(vacancies[1]) == 1


def test_get_vacancies_with_keyword(data_vacancies):
    """ Проверка метода вывода вакансий по кодовому слову в названии """

    truncate_table = DBManager(host_name, port_num, database_name, user_name, pwd)
    truncate_table.truncate_vacancies_table()

    conn = psycopg2.connect(
        host = host_name,
        port = port_num,
        database = database_name,
        user = user_name,
        password = pwd)

    cur = conn.cursor()
    for row in data_vacancies:
        cur.execute("""INSERT INTO vacancies 
                                (vacancy_id, 
                                vacancy_name, 
                                vacancy_url, 
                                salary_from, 
                                salary_to, 
                                currency, 
                                gross, 
                                address, 
                                employer_id, 
                                snippet_requirement, 
                                snippet_responsibility) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s) returning *""", row)
        conn.commit()
    cur.close

    vac = DBManager(host_name, port_num, database_name, user_name, pwd)
    vacancies = vac.get_vacancies_with_keyword('DevOps')
    print(vacancies)

    assert vacancies[0][1] == 'Вакансия1 DevOps'
