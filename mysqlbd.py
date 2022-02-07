import mysql.connector
from mysql.connector import Error
from config import db_config 

def create_connection_mysql_db(db_host, user_name, user_password, db_name = None):
    connection_db = None
    try:
        connection_db = mysql.connector.connect(
            host = db_host,
            user = user_name,
            passwd = user_password,
            database = db_name
        )
        print("Подключение к MySQL успешно выполнено")
    except Error as db_connection_error:
        print("Возникла ошибка: ", db_connection_error)
    return connection_db


conn = create_connection_mysql_db(db_config["mysql"]["host"], 
                                  db_config["mysql"]["user"], 
                                  db_config["mysql"]["pass"])
cursor = conn.cursor()
create_db_sql_query = 'CREATE DATABASE {}'.format('Test')
cursor.execute(create_db_sql_query)
cursor.close()
conn.close()

conn = create_connection_mysql_db(db_config["mysql"]["host"], 
                                  db_config["mysql"]["user"], 
                                  db_config["mysql"]["pass"],
                                  "Test")
try:
    # создание таблицы
    cursor = conn.cursor()
    create_table_query = '''
    CREATE TABLE IF NOT EXISTS users (
    id INT AUTO_INCREMENT, 
    name TEXT NOT NULL, 
    age INT, 
    gender TEXT, 
    nationality TEXT, 
    PRIMARY KEY (id)
    ) ENGINE = InnoDB'''
    cursor.execute(create_table_query)
    conn.commit()

    # вставка данных в таблицу
    insert_users_table_query = '''
    INSERT INTO
    `users` (`name`, `age`, `gender`, `nationality`)
    VALUES
    ('James', 25, 'male', 'USA'),
    ('Leila', 32, 'female', 'France'),
    ('Brigitte', 35, 'female', 'England');'''
    cursor.execute(insert_users_table_query)
    conn.commit()

    # изблечение данных из бд
    select_users_female_query = '''
    SELECT name, age, nationality FROM users WHERE gender = 'female';
    '''
    cursor.execute(select_users_female_query)
    query_result = cursor.fetchall()
    for user in query_result:
        print(user)

    # редактирование записей
    update_user_gender_query = '''
    UPDATE users SET gender = 'deer' WHERE gender = 'male';
    '''
    cursor.execute(update_user_gender_query)
    conn.commit()

    # удаление записей 
    delete_Usa_users_query = '''
    DELETE FROM users WHERE nationality = 'USA';
    '''
    cursor.execute(delete_Usa_users_query)
    conn.commit()

except Error as error:
    print(error)
finally:
    cursor.close()
    conn.close()                                 