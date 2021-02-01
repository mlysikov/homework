# ----------------------------------------------------------------------------------------------------------------------
# 1. На любом языке программирования реализовать реконсиляцию транзакций клиентов банка из двух источников
# (как минимум один - таблица БД, второй на выбор).
# ----------------------------------------------------------------------------------------------------------------------

# Немного не понял этот пункт "для разных типов данных (дата, текст, числа)".
# Если с числами все ясно, там толеранс, то с датами и текстом не ясно, какое допустимое отклонение.
# Ясно, что вообще в идеальном случае, реконсиляция должна рассматривать транзакции как объект со своими свойствами и
# мы должны сравнивать эти объекты целиком.

# Чтобы я усовершенствовал в текущем коде:
# * Вставки и чтения сделал бы через хранимые процедуры на стороне базы, не писал бы SQL конструкции здесь
# * Попробовал чисто Python подход. Сделал бы так, чтобы adapter_db, adapter_file отдавали итераторы,
#   а processor выполнял вставку в базу данных
# * Выполнял бы чтения из двух адаптеров параллельно (2 потока)
# * Выполнял бы чтения из двух адаптеров параллельно и чанками (например, 4 потока на каждый адаптер). Что-то наподобие
#   DBMS_PARALLEL_EXECUTE в Oracle

import cx_Oracle
import csv
import datetime
import hashlib
import time

# Создание глобального подключения к базе данных.
cx_Oracle.init_oracle_client(lib_dir="/Users/mlysikov/Downloads/instantclient_19_8")
conn = cx_Oracle.connect("admin", "***", "db_high")
conn.autocommit = False

# Константы.
batch_size = 10000
tolerance_value = 10
adapter_db_id = 1
adapter_file_id = 2

# Создание необходимых таблиц в базе данных.
def create_tables():
    #print("create tables")

    cursor = conn.cursor()

    cursor.execute("""BEGIN
                        EXECUTE IMMEDIATE 'DROP TABLE admin.transactions PURGE';
                        EXCEPTION
                          WHEN OTHERS THEN
                            IF sqlcode <> -942 THEN
                              RAISE;
                            END IF;
                        END;""")
    cursor.execute("""CREATE TABLE admin.transactions (
                        id               NUMBER(10)   NOT NULL
                       ,client_id        NUMBER(10)   NOT NULL
                       ,account_number   VARCHAR2(20) NOT NULL
                       ,transaction_date DATE         NOT NULL
                       ,amount           NUMBER(10,4) NOT NULL)""")

    cursor.execute("""BEGIN
                        EXECUTE IMMEDIATE 'DROP TABLE admin.src_data PURGE';
                        EXCEPTION
                          WHEN OTHERS THEN
                            IF sqlcode <> -942 THEN
                              RAISE;
                            END IF;
                        END;""")
    cursor.execute("""CREATE TABLE admin.src_data (
                        id               NUMBER(10)   NOT NULL
                       ,adapterid        NUMBER(10)   NOT NULL
                       ,client_id        NUMBER(10)   NOT NULL
                       ,account_number   VARCHAR2(20) NOT NULL
                       ,transaction_date DATE         NOT NULL
                       ,amount           NUMBER(10,4) NOT NULL
                       ,hash_val         RAW(20)      NOT NULL)""")

    cursor.execute("""BEGIN
                        EXECUTE IMMEDIATE 'DROP TABLE admin.recon_data PURGE';
                        EXCEPTION
                          WHEN OTHERS THEN
                            IF sqlcode <> -942 THEN
                              RAISE;
                            END IF;
                        END;""")
    cursor.execute("""CREATE TABLE admin.recon_data (
                        id               NUMBER(10)   NOT NULL
                       ,client_id        NUMBER(10)   NOT NULL
                       ,account_number   VARCHAR2(20) NOT NULL
                       ,transaction_date DATE         NOT NULL
                       ,amount           NUMBER(10,4) NOT NULL)""")

# Создание тестовых данных.
def generate_test_data():
    #print("create generate_test_data")

    cursor = conn.cursor()

    # Тестовые данные, которые полностью совпадают с данными в CSV файле,
    # за исключением 2-х записей (ID=99, 100):
    # * ID=99 - не совпадет с amount, в CSV файле значительно меньше.
    # * ID=100 - совпадет, если запускать реконсиляцию с толерансом.
    cursor.execute("""INSERT INTO admin.transactions
                      SELECT level AS id
                            ,level AS client_id
                            ,'40817810099910004' || TO_CHAR(100 + level) AS account_number
                            ,TO_DATE('2020-01-01', 'YYYY-MM-DD') - 1 + level AS transaction_date
                            ,100 * level AS amount
                      FROM   dual
                      CONNECT BY level <= 100""")

    # Еще 99900 рандомных записей.
    cursor.execute("""INSERT INTO admin.transactions
                      SELECT 100 + level AS id
                            ,TRUNC(DBMS_RANDOM.value(1,100)) AS client_id
                            ,'40817810099910004' || TO_CHAR(TRUNC(DBMS_RANDOM.value(1,100))) AS account_number
                            ,TO_DATE('2020-01-01', 'YYYY-MM-DD') - 1 + TRUNC(DBMS_RANDOM.value(1,100)) AS transaction_date
                            ,TRUNC((10000 * TRUNC(DBMS_RANDOM.value(1,5))) / TRUNC(DBMS_RANDOM.value(5,10))) AS amount
                      FROM   dual
                      CONNECT BY level <= 99900""")

    conn.commit()

# Преобразование данных и вставку в таблицу.
def process_data(source_list, adapter_id):
    #print("process_data", "adapter_id=", adapter_id)

    cursor = conn.cursor()

    new_list = [(x[0],
                 adapter_id,
                 x[1],
                 x[2],
                 x[3],
                 x[4],
                 hashlib.sha1((str(x[1]) + x[2] + x[3].strftime('%Y-%m-%d') + str(x[4])).encode('utf-8')).hexdigest()) for x in source_list]

    # Предопределить области памяти в соответствии с определением таблицы.
    cursor.setinputsizes(None, None, None, 20, None, None, None)

    # Массовая вставка в таблицу-приемник.
    cursor.executemany("""INSERT INTO admin.src_data(id, adapterid, client_id, account_number, transaction_date, amount, hash_val)
                          VALUES (:1, :2, :3, :4, :5, :6, :7)""", new_list)

# Адаптер для источника данных в Oracle.
# Для каждого источника данных свой адаптер.
# Адаптер пакетно считывает данные и вызывает функцию преобразования и вставки данных в таблицу в базе данных.
# Пакетные чтения, считывания только необходимых столбцов, пакетные вставки позволят сократить объем используемой памяти.
def adapter_db():
    #print("adapter_db")

    cursor = conn.cursor()

    cursor.execute("""SELECT *
                      FROM   admin.transactions""")

    # Пакетно считать и вставить в таблицу в базе данных.
    while True:
        rows = cursor.fetchmany(batch_size)

        process_data(rows, adapter_db_id)

        if not rows:
            break

    conn.commit()

    return 1

# Адаптер для источника данных в CSV файле.
def adapter_file():
    #print("adapter_file")

    with open("/Users/mlysikov/Repo/homework/transactions.csv", 'r') as file:
        read = csv.reader(file, delimiter=',')

        # Исключить заголовок CSV файла.
        next(read, None)

        rows = []

        # Пакетно считать и вставить в таблицу в базе данных.
        for line in read:
            rows.append((int(line[0]),
                         int(line[1]),
                         line[2],
                         datetime.datetime.strptime(line[3], '%Y-%m-%d'),
                         float(line[4])))

            if len(rows) % batch_size == 0:
                process_data(rows, adapter_file_id)
                rows = []

        if rows:
            process_data(rows, adapter_file_id)

    conn.commit()

    return 1

# Процессор.
def processor(*adapters):
    #print("processor")

    cursor = conn.cursor()

    # Очистить таблицу-приемник.
    cursor.execute("TRUNCATE TABLE admin.src_data")

    # Обойти все источники данных.
    for idx, source in enumerate(adapters):
        ret_val = source()

# Реконсилированными считаем данными, если
#   * Совпали по ID + HASH_VAL
#   * Совпали по ID + CLIENT_ID + ACCOUNT_NUMBER + TRANSACTION_DATE + tolerance между суммами из двух источников по формуле ( ( N2 - N1 ) / N2 ) * 100,
#       где N2 - большее из двух чисел, N1 - меньшее
def recon(tolerance=None):
    #print("recon", "tolerance=", tolerance)

    cursor = conn.cursor()

    cursor.execute("""INSERT INTO admin.recon_data
                      SELECT x.*
                      FROM   (SELECT t1.id
                                    ,t1.client_id
                                    ,t1.account_number
                                    ,t1.transaction_date
                                    ,t1.amount
                              FROM   admin.src_data t1
                              JOIN   admin.src_data t2
                              ON     t1.id = t2.id
                                     AND t1.adapterid != t2.adapterid
                                     AND t1.hash_val = t2.hash_val
                              WHERE  t1.adapterid = 1
                              UNION ALL
                              SELECT t1.id
                                    ,t1.client_id
                                    ,t1.account_number
                                    ,t1.transaction_date
                                    ,t1.amount
                              FROM   admin.src_data t1
                              JOIN   admin.src_data t2
                              ON     t1.id = t2.id
                                     AND t1.adapterid != t2.adapterid
                                     AND t1.hash_val != t2.hash_val
                                     AND t1.client_id = t2.client_id
                                     AND t1.account_number = t2.account_number
                                     AND t1.transaction_date = t2.transaction_date
                                     AND ( ( GREATEST(t1.amount, t2.amount) - LEAST(t1.amount, t2.amount)) / GREATEST(t1.amount, t2.amount ) ) * 100 <= :p1
                              WHERE  :p2 IS NOT NULL
                                      AND t1.adapterid = 1) x
                      WHERE  NOT EXISTS (SELECT 1
                                         FROM   admin.recon_data t
                                         WHERE  x.id = t.id)""", p1=tolerance, p2=tolerance)

    conn.commit()

def start_recon():
    create_tables()
    generate_test_data()

    start = time.time()

    processor(adapter_db, adapter_file)
    recon(tolerance_value)
    conn.close()

    elapsed = (time.time() - start)
    print("Reconciliation completed in", elapsed, "seconds")

start_recon()