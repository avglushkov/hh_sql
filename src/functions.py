from classes import From_hh_api_employers, From_hh_api_vacancies
import psycopg2
import json
import csv

host_name='localhost'
port_num='5432'
database_name='hh_db'
user_name='hh_bd_user'
pwd='654321'
def db_tables_create(host_name, port_num, database_name, user_name, pwd):
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
    cur.execute("DROP TABLE IF EXISTS addresses CASCADE")

    # создаем таблицы с нужной структурой и связями
    cur.execute(
        "CREATE TABLE  employers(employer_id INTEGER PRIMARY KEY, employer_name VARCHAR(50), employer_url VARCHAR(100))")
    cur.execute(
        "CREATE TABLE addresses(address_id INTEGER PRIMARY KEY, city VARCHAR(30), street VARCHAR(50), building VARCHAR(20), description VARCHAR(100), raw VARCHAR(50), metro VARCHAR(50), metro_stations VARCHAR(100))")
    cur.execute(
        "CREATE TABLE  vacancies(vacancy_id INTEGER PRIMARY KEY, vacancy_name VARCHAR(100), vacancy_url VARCHAR(100), salary_from INTEGER, salary_to INTEGER, currancy VARCHAR(10), gross BOOL, address_id INTEGER REFERENCES addresses(address_id) NOT NULL, employer_id INTEGER REFERENCES employers(employer_id) NOT NULL, snippet_requirement TEXT, snippet_responsibility TEXT)")

    # Фиксируем изменения в базе данных
    conn.commit()

    # Закрываем курсор
    cur.close

# db_tables_create(host_name, port_num, database_name, user_name, pwd)

def main_menu() -> None:
    """ функция выбора пункта основного меню """

    print('\nОСНОВНОЕ МЕНЮ:\n'
          '1. Выбор списка компаний-работодателей\n'
          '2. _______\n'
          '7. Закончить работу')

    selected_point = int(input('ВВЕДИТЕ НОМЕР ПУНКТА МЕНЮ: '))

    match selected_point:
        case 1:
            menu_get_employers()
        case 2:
            pass

        case 7:
            print('МЫ ЗАКОНЧИЛИ. ПОКА!')
        case _:
            print('В МЕНЮ НЕТ ТАКОГО ПУНКТА')



def menu_get_employers() -> None:
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

    while True:
        employer_id = input('Выберете идентификатор компании для добавления в список: ')
        try:
            employer_id in employers_id_list

        except ValueError:
            print('Введенный идентификатор отсутствует в списке')
            continue
        if employer_id in employers_id_list:
            add_employer=[]

            for employer_for_add in employers_list['items']:
                if employer_for_add['id'] == employer_id:
                    add_employer.append(employer_for_add['id'])
                    add_employer.append(employer_for_add['name'])
                    with open("../data/employers_list.csv", 'a', encoding='utf-8') as f:
                        writer = csv.writer(f)
                        writer.writerow(add_employer)

            print('Работадатель добавлен в список')

            break
    main_menu()


    # if employer_id in employers_list['items']:
    #     print('Работадатель добавлен в список')
    # else:
    #     print('Введенный идентификатор отсутствует в списке')




    # user_input = input("Do you want to continue? (yes/no): ")
    # if user_input.lower() == "yes":
    #     print("Continuing...")
    # else:
    #     print("Exiting...")
    #
    # file = Vacancies_File(raw_file_path, source_file_path)
    # file.from_raw_file()
    # print_vacancies(source_file_path)
    # main_menu()

main_menu()

# def db_tables_insert_data(host_name, port_num, database_name, user_name, pwd):
#     """ функция записи данных в таблицы"""
#
#     conn = psycopg2.connect(
#     host = host_name,
#     port = port_num,
#     database = database_name,
#     user = user_name,
#     password= pwd)

    # Открытие курсора
    # cur = conn.cursor()


# # Теперь приступаем к операциям вставок данных
# # Запустите цикл по списку customers_data и выполните запрос формата
# # INSERT INTO table (column1, column2, ...) VALUES (%s, %s, ...) returning *", data)
# # В конце каждого INSERT-запроса обязательно должен быть оператор returning *
# for customer in customers_data:
#     cur.execute("INSERT INTO itresume3616.customers (customer_id, company_name, contact_name) VALUES (%s, %s, %s) returning *",customer)

# # Не меняйте и не удаляйте эти строки - они нужны для проверки
# conn.commit()
# res_customers = cur.fetchall()
#
# # Запустите цикл по списку employees_data и выполните запрос формата
# # INSERT INTO itresume3616.table (column1, column2, ...) VALUES (%s, %s, ...) returning *", data)
# # В конце каждого INSERT-запроса обязательно должен быть оператор returning *
# for employee in employees_data:
#     cur.execute("INSERT INTO itresume3616.employees (employee_id, first_name, last_name, title, birth_date, notes) VALUES (%s, %s, %s, %s, %s, %s) returning *", employee)
#
# # Не меняйте и не удаляйте эти строки - они нужны для проверки
# conn.commit()
# res_employees = cur.fetchall()
#
# # Запустите цикл по списку orders_data и выполните запрос формата
# # INSERT INTO itresume3616.table (column1, column2, ...) VALUES (%s, %s, ...) returning *", data)
# # В конце каждого INSERT-запроса обязательно должен быть оператор returning *
# for order in orders_data:
#     cur.execute("INSERT INTO itresume3616.orders (order_id, customer_id , employee_id , order_date , ship_city) VALUES (%s, %s, %s, %s, %s) returning *", order)
#
# # Не меняйте и не удаляйте эти строки - они нужны для проверки
# conn.commit()
# res_orders = cur.fetchall()
#
# # Закрытие курсора
# cur.close


