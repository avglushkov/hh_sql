from classes import From_hh_api_employers, From_hh_api_vacancies, DBManager
import psycopg2
import json
import csv
import os
import psycopg2.extras

host_name='localhost'
port_num='5432'
database_name='hh_db'
user_name='hh_bd_user'
pwd='654321'


def main_menu() -> None:
    """ функция выбора пункта основного меню """

    print('\nОСНОВНОЕ МЕНЮ:\n'
          '    1. Действия со списком компаний-работодателей\n'
          '    2. Действия со списком вакансий\n'          
          '    3. Создание / очистка структуры базы данных вакансий и работодателей\n'          
          '    4. Завершение работы')

    selected_point = int(input('ВВЕДИТЕ НОМЕР ПУНКТА МЕНЮ: '))

    match selected_point:
        case 1:
            sub_menu_get_employers()
        case 2:
            sub_menu_get_vacancies()
        case 3:
            menu_db_tables_create()
        case 4:
            print('МЫ ЗАКОНЧИЛИ. ПОКА!')
        case _:
            print('В МЕНЮ НЕТ ТАКОГО ПУНКТА')

def menu_db_tables_create() -> None:
    """ функция создания таблиц с в Базе данных"""

    conn = psycopg2.connect(
    host = host_name,
    port = port_num,
    database = database_name,
    user = user_name,
    password= pwd)

    # Открытие курсора
    cur = conn.cursor()

    # если таблицы уже есть в БД, то мы их удаляем
    cur.execute("DROP TABLE IF EXISTS vacancies CASCADE")
    cur.execute("DROP TABLE IF EXISTS employers CASCADE")

    # создаем таблицы с нужной структурой и связями
    cur.execute(
        "CREATE TABLE  employers(employer_id INTEGER PRIMARY KEY, employer_name VARCHAR(50), employer_url VARCHAR(100))")
    cur.execute(
        "CREATE TABLE  vacancies(vacancy_id INTEGER PRIMARY KEY, vacancy_name VARCHAR(100), vacancy_url VARCHAR(100), salary_from INTEGER, salary_to INTEGER, currency VARCHAR(10), gross BOOL, address VARCHAR(40), employer_id INTEGER REFERENCES employers(employer_id) NOT NULL, snippet_requirement TEXT, snippet_responsibility TEXT)")

    # Фиксируем изменения в базе данных
    conn.commit()

    # Закрываем курсор
    cur.close

    print('Структра БД создана/очищена')
    main_menu()

def sub_menu_get_employers() -> None:
    """ функция меню добавления работодателей в список"""

    print('\nДЕЙСТИЯ СО СПИСКОМ РАБОТОДАТЕЛЕЙ:\n'
          '    1. Вывод списка работодателей, добавленных ранее в поиск \n'
          '    2. Добавление работодателя в список\n'
          '    3. Возврат в основное меню')

    selected_point = int(input('ВВЕДИТЕ НОМЕР ПУНКТА МЕНЮ: '))

    match selected_point:
        case 1:
            get_employers_list()
        case 2:
            get_employers()
        case 3:
            main_menu()
        case _:
            print('В МЕНЮ НЕТ ТАКОГО ПУНКТА')
def sub_menu_get_vacancies() -> None:
    """ функция меню добавления работодателей в список"""

    print('\nДЕЙСТВИЯ СО СПИСКОМ ВАКАНСИЙ:\n'
          '    1. Загрузка вакансии по выбранным работодателям\n'
          '    2. Вывод количества вакансий, загруженных по работодателям\n'
          '    3. Вывод списка вакансий\n'
          '    4. Вывод среднего уровня ЗП по загруженным вакансиям\n'
          '    5. Вывод списка вакансий с ЗП выше среднего\n'
          '    6. Вывод списка вакансий по ключевому слову\n'
          '    7. Возврат в основное меню')

    selected_point = int(input('ВВЕДИТЕ НОМЕР ПУНКТА МЕНЮ: '))

    match selected_point:
        case 1:
            menu_get_vacancies()
        case 2:
            menu_get_companies_vacancies_count()
        case 3:
            menu_get_all_vacancies()
        case 4:
            menu_get_average_salary()
        case 5:
            menu_get_vacancies_with_higher_salary()
        case 6:
            menu_get_vacancies_with_keyword()
        case 7:
            main_menu()
        case _:
            print('В МЕНЮ НЕТ ТАКОГО ПУНКТА')
def get_employers_list() -> None:

    file_name = "../data/employers_list.csv"

    if os.path.isfile('../data/employers_list.csv'):
        file_size = os.path.getsize('../data/employers_list.csv')
        if file_size == 0:
            print('Компании работодатели пока не были добавлены')
            sub_menu_get_employers()
        else:
            print('\nРаботодатели, добавленные в список:')
            with open('../data/employers_list.csv', 'rt', encoding='utf-8') as f:
                reader = csv.reader(f)

                for row in reader:
                    print(f'{row[0]} - {row[1]} - {row[2]}')
            sub_menu_get_employers()

    else:
        print("Компании работодатели пока не были добавлены")
        sub_menu_get_employers()

def get_employers() -> None:
    """ функция загрузки работадателей по указанию их наименований"""

    print('\nЗАПРАШИВАЕМ ДАННЫЕ ПО РАБОТОДАТЕЛЯМ ИЗ БАЗЫ HEAD HUNTER\n')

    search_word = input('Введите наименование работодателя : ')

    hh_api = From_hh_api_employers(host_name, port_num, database_name, user_name, pwd)
    employers_list = hh_api.get_employers(search_word)
    employers_id_list = []

    for employer in employers_list['items']:
        employers_id_list.append(employer['id'])
        print(f'{employer['id']} - {employer['name']},  {employer['url']}' )

    print(employers_id_list)

    choice = input('Вы хотите добавить компанию из списка? Y/N: ')

    if choice == 'Y' or choice == 'y':
        while True:

            employer_id = input('Выберете идентификатор компании для добавления в список: ')

            try:
                employer_id in employers_id_list

            except ValueError:
                #print('Введенный идентификатор отсутствует в списке')
                continue
            if employer_id in employers_id_list:
                add_employer = []

                for employer_for_add in employers_list['items']:
                    if employer_for_add['id'] == employer_id:

                        if os.path.isfile("../data/employers_list.csv"):
                            early_added_employers = []

                            with open("../data/employers_list.csv", 'rt', encoding='utf-8') as f:
                                reader = csv.reader(f)
                                for row in reader:
                                    early_added_employers.append(row[0])
                            if employer_for_add['id'] in early_added_employers:
                                print('Работодатель ранее уже был добавлен. начните поиск заново')
                                sub_menu_get_employers()
                            else:
                                add_employer.append(employer_for_add['id'])
                                add_employer.append(employer_for_add['name'])
                                add_employer.append(employer_for_add['url'])
                                with open("../data/employers_list.csv", 'a', encoding='utf-8') as f:
                                    writer = csv.writer(f)
                                    writer.writerow(add_employer)
                                print('Работодатель добавлен в список')

                        else:
                            add_employer.append(employer_for_add['id'])
                            add_employer.append(employer_for_add['name'])
                            add_employer.append(employer_for_add['url'])
                            with open("../data/employers_list.csv", 'a', encoding='utf-8') as f:
                                writer = csv.writer(f)
                                writer.writerow(add_employer)
                            print('Работодатель добавлен в список')
                break
        # Записываем данные о работодателях в БД
        hh_api = From_hh_api_employers(host_name, port_num, database_name, user_name, pwd)
        employers_list = hh_api.write_employers_into_db()
        sub_menu_get_employers()
    else:
        print('Выполните новый поиск')
        sub_menu_get_employers()

def menu_get_vacancies() -> None:
    """ функция загрузки вакансий по работодателям, выбранным пользователем"""

    search_text = input('Введите ключевое слово для поиска вакансий: ')

    truncate_table = DBManager(host_name, port_num, database_name, user_name, pwd)
    truncate_table.truncate_vacancies_table()

    if os.path.isfile('../data/employers_list.csv'):
        with open('../data/employers_list.csv', 'rt', encoding='utf-8') as f:
            reader = csv.reader(f)
            for row in reader:
                hh_api = From_hh_api_vacancies(host_name, port_num, database_name, user_name, pwd)
                hh_api.get_vacancies(row[0], row[1], search_text)
        sub_menu_get_vacancies()
    else:
        print('\nСначала нужно выбрать работодателей. Сейчас их список пуст')
        main_menu()

def menu_get_companies_vacancies_count():
    """ Функция вывода количества вакансий, загруженных по работодателям"""


    employer = DBManager(host_name, port_num, database_name, user_name, pwd)
    employer.get_companies_and_vacancies_count()

    print(f'\nВ базу всего загружено {employer.get_companies_and_vacancies_count()[1][0][0]} вакансий: ')

    for emp in employer.get_companies_and_vacancies_count()[0]:
        print(f'{emp[0]} - {emp[1]} шт.')

    sub_menu_get_vacancies()

def menu_get_all_vacancies() -> None:
    """ Функция вывода всех загруженных вакансий"""

    vacancies_list = []

    vacancies = DBManager(host_name, port_num, database_name, user_name, pwd)
    vacancies_list = vacancies.get_all_vacancies()

    for vacancy in vacancies_list:
        if vacancy[2] == 0:
            if vacancy[3] == 0:
                print(f'{vacancy[0]} - {vacancy[1]} ЗП не указана {vacancy[6]}')
            else:
                print(f'{vacancy[0]} - {vacancy[1]} ЗП до {vacancy[3]} {vacancy[4]} {vacancy[6]}')
        else:
            if vacancy[3] == 0:
                print(f'{vacancy[0]} - {vacancy[1]} ЗП от {vacancy[2]} {vacancy[4]} {vacancy[6]}')
            else:
                print(f'{vacancy[0]} - {vacancy[1]} ЗП от {vacancy[2]} до {vacancy[3]} {vacancy[4]} {vacancy[6]}')
    sub_menu_get_vacancies()

def menu_get_average_salary() -> None:
    """ Функция вывода всех загруженных вакансий"""

    average_salary = []

    vacancies = DBManager(host_name, port_num, database_name, user_name, pwd)
    average_salary = vacancies.get_avg_salary()

    print(f'\nСредний уровень ЗП "ОТ", указанный для загруженных вакансий: {int(average_salary[0])} RUR')
    print(f'\nСредний уровень ЗП "ДО", указанный для загруженных вакансий: {int(average_salary[1])} RUR')

    sub_menu_get_vacancies()

def menu_get_vacancies_with_higher_salary():
    """ Функция вывода списка вакансий с ЗП выше среднего """


    vacancies = DBManager(host_name, port_num, database_name, user_name, pwd)
    average_salary = vacancies.get_avg_salary()
    vacancies_with_higher_salary_from = vacancies.get_vacancies_with_higher_salary(average_salary[0], average_salary[1])[0]
    vacancies_with_higher_salary_to = vacancies.get_vacancies_with_higher_salary(average_salary[0], average_salary[1])[1]

    print(f'\nВакансии с уровнем ЗП "ОТ" выше среднего {average_salary[0]} RUR')
    for vacancy in vacancies_with_higher_salary_from:
        print(f'{vacancy[0]} - {vacancy[1]} ЗП от {vacancy[2]} до {vacancy[3]} {vacancy[4]} {vacancy[6]}')

    print(f'\nВакансии с уровнем ЗП "ДО" выше среднего {average_salary[0]} RUR')
    for vacancy in vacancies_with_higher_salary_to:
        print(f'{vacancy[0]} - {vacancy[1]} ЗП от {vacancy[2]} до {vacancy[3]} {vacancy[4]} {vacancy[6]}')

    sub_menu_get_vacancies()
def menu_get_vacancies_with_keyword() -> None:
    """ Функция вывода вакансий по ключевому слову """

    key_word = input('Введите слово для поиска вакансий из загруженного списка: ')

    vacancies = DBManager(host_name, port_num, database_name, user_name, pwd)
    vacancies_with_keyword = vacancies.get_vacancies_with_keyword(key_word)

    for vacancy in vacancies_with_keyword:
        print(f'{vacancy[0]} - {vacancy[1]} ЗП от {vacancy[2]} до {vacancy[3]} {vacancy[4]} {vacancy[6]}')

    sub_menu_get_vacancies()




