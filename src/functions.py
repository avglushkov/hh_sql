from classes import From_hh_api_employers, From_hh_api_vacancies, Vacancies_File
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
          
          '    7. Завершение работы')

    selected_point = int(input('ВВЕДИТЕ НОМЕР ПУНКТА МЕНЮ: '))

    match selected_point:
        case 1:
            sub_menu_get_employers()
        case 2:
            sub_menu_get_vacancies()
        case 3:
            menu_db_tables_create(host_name, port_num, database_name, user_name, pwd)
        case 7:
            print('МЫ ЗАКОНЧИЛИ. ПОКА!')
        case _:
            print('В МЕНЮ НЕТ ТАКОГО ПУНКТА')

def menu_db_tables_create(host_name, port_num, database_name, user_name, pwd) -> None:
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
          '    2. -\n'
          '    3. -\n'
          '    4. Возврат в основное меню')

    selected_point = int(input('ВВЕДИТЕ НОМЕР ПУНКТА МЕНЮ: '))

    match selected_point:
        case 1:
            menu_get_vacancies()
        case 2:
            main_menu()
        case 3:
            main_menu()
        case 4:
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

    hh_api = From_hh_api_employers()
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

        conn = psycopg2.connect(
            host=host_name,
            port=port_num,
            database=database_name,
            user=user_name,
            password=pwd)
        # Открытие курсора
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

        sub_menu_get_employers()
    else:
        print('Выполните новый поиск')
        sub_menu_get_employers()


def menu_get_vacancies() -> None:
    """ функция загрузки вакансий по работодателям, выбранным пользователем"""

    conn = psycopg2.connect(
        host=host_name,
        port=port_num,
        database=database_name,
        user=user_name,
        password=pwd)

    cur = conn.cursor()
    cur.execute("TRUNCATE TABLE vacancies RESTART IDENTITY CASCADE")
    conn.commit()
    cur.close

    if os.path.isfile('../data/employers_list.csv'):
        with open('../data/employers_list.csv', 'rt', encoding='utf-8') as f:
            reader = csv.reader(f)
            for row in reader:
                hh_api = From_hh_api_vacancies()
                hh_api.get_vacancies(row[0], row[1])
        sub_menu_get_vacancies()
               #  # file = Vacancies_File('../data/raw_vacancies.json', '../data/vacancies.json', '../data/employers.json')
               #  # file.from_raw_file()
               #
               #  conn = psycopg2.connect(
               #      host=host_name,
               #      port=port_num,
               #      database=database_name,
               #      user=user_name,
               #      password=pwd)
               #
               #  cur = conn.cursor()
               #
               # # os.chmod('/home/avgl/Projects/hh_sql/data/raw_vacancies.json', 0o444)
               #
               #  cur.execute("DROP TABLE IF EXISTS json_data_table CASCADE;")
               #  cur.execute("CREATE TABLE json_data_table(data jsonb);")
               #  cur.execute("COPY json_data_table(data) FROM '/tmp/raw_vacancies.json';")
               #
               #  cur.execute("CREATE TABLE json_records AS SELECT * FROM json_populate_recordset(null::JSONB, (SELECT raw_data FROM json_data_table));")
               #
               #  # INSERT INTO vacancies (vacancy_id,
               #  #                        vacancy_name,
               #  #                        vacancy_url,
               #  #                        salary_from,
               #  #                        salary_to,
               #  #                        currancy,
               #  #                        gross,
               #  #                        address,
               #  #                        employer_id,
               #  #                        snippet_requirement,
               #  #                        snippet_responsibility)
               #  #
               #  #
               #  #
               #  # SELECT raw_data-> > 'key1', raw_data->'nested'-> > 'key2'
               #  # FROM
               #  # staging_table;
               #
               #  conn.commit()
               #  cur.close

    else:
        print('\nСначала нужно выбрать работодателей. Сейчас их список пуст')
        main_menu()

# def add_employers_into_table() -> None:
#     """ функция записи данных в таблицы"""
#
#     conn = psycopg2.connect(
#     host = host_name,
#     port = port_num,
#     database = database_name,
#     user = user_name,
#     password= pwd)
#     #Открытие курсора
#     cur = conn.cursor()
#
#     cur.execute("TRUNCATE TABLE employers RESTART IDENTITY CASCADE")
#
#     with open('../data/employers_list.csv', 'rt', encoding='utf-8') as f:
#         reader = csv.reader(f)
#
#         for row in reader:
#             cur.execute(
#                 "INSERT INTO employers (employer_id, employer_name, employer_url) VALUES (%s, %s, %s) returning *", row)
#
#     conn.commit()
#     cur.close
#     print('Данные по работодателям добавлены в БД')
#
#     sub_menu_get_employers()


main_menu()



