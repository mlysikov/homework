-- Задание выполнялось на Oracle Database 19c.

--------------------------------------------------------------------------------
-- 1. Создать структуру БД, наполнить тестовыми данными.
--------------------------------------------------------------------------------

-- 1. Oracle Database это единая база данных. Когда создается пользователь - 
-- создается схема с аналогичным именем. Это эквивалент базы данных в MySQL.
-- 2. Чтобы бы я добавил / изменил в схеме данных:
--    * Типы счетов и операций выделить в отдельные справочники;
--    * Добавить суррогатные ключи в таблицы tb_logins, tb_operations, tb_orders.

DROP USER db_default CASCADE;
DROP USER db_billing CASCADE;
DROP USER db_orderstat CASCADE;

-- Создание необходимых схем.
CREATE USER db_default IDENTIFIED BY Pass12345678;
GRANT CREATE SESSION, CREATE TABLE TO db_default;
GRANT UNLIMITED TABLESPACE TO db_default;

CREATE USER db_billing IDENTIFIED BY Pass12345678;
GRANT CREATE SESSION, CREATE TABLE TO db_billing;
GRANT UNLIMITED TABLESPACE TO db_billing;

CREATE USER db_orderstat IDENTIFIED BY Pass12345678;
GRANT CREATE SESSION, CREATE TABLE TO db_orderstat;
GRANT UNLIMITED TABLESPACE TO db_orderstat;

-- Создание таблиц.
CREATE TABLE db_default.tb_users
(
  uuid              NUMBER(10)  -- UID - Зарезервированное слово в Oracle. Пришлось поменять на UUID.
 ,registration_date DATE NOT NULL
 ,country           VARCHAR2(100) NOT NULL
 ,CONSTRAINT tb_users_pk PRIMARY KEY (uuid)
);

CREATE TABLE db_default.tb_logins
(
  user_uid     NUMBER(10) NOT NULL
 ,login        VARCHAR2(30)
 ,account_type VARCHAR2(4) NOT NULL
 ,CONSTRAINT tb_logins_pk PRIMARY KEY (login)
 ,CONSTRAINT tb_logins_check CHECK (account_type IN ('real', 'demo'))
 ,CONSTRAINT tb_logins_fk FOREIGN KEY (user_uid) REFERENCES db_default.tb_users(uuid)
);

GRANT REFERENCES ON db_default.tb_logins TO db_billing;
GRANT REFERENCES ON db_default.tb_logins TO db_orderstat;

CREATE TABLE db_billing.tb_operations
(
  operation_type VARCHAR2(100) NOT NULL
 ,operation_date DATE NOT NULL
 ,login          VARCHAR2(30) NOT NULL
 ,amount         NUMBER(10,4) NOT NULL
 ,CONSTRAINT tb_operations_fk FOREIGN KEY (login) REFERENCES db_default.tb_logins(login)
 ,CONSTRAINT tb_operations_chk CHECK (operation_type IN ('deposit', 'withdrawal'))
 ,CONSTRAINT tb_operations_ch2 CHECK (amount > 0) -- Ясно, что нулевых депозитов и снятий не может быть,
                                                  -- но в случае bulk-овых загрузок, чисто теоретически, 
                                                  -- без проведенного data quality, может быть amount = 0.
);

CREATE TABLE db_orderstat.tb_orders
(
  login            VARCHAR2(30) NOT NULL
 ,order_close_date DATE NOT NULL
 ,CONSTRAINT tb_orders_fk FOREIGN KEY (login) REFERENCES db_default.tb_logins(login)
);

-- Создание тестовых данных, не прибегая к генераторам (hierarchical queries, DBMS_RANDOM), 
-- чтобы обдумать и сгенерить разные комбинации данных.

-- Данные для Russia.
INSERT INTO db_default.tb_users VALUES (1, TO_DATE('2020-12-01 00:00:00', 'YYYY-MM-DD HH24:MI:SS'), 'Russia');

INSERT INTO db_default.tb_users VALUES (2, TO_DATE('2020-12-02 00:00:00', 'YYYY-MM-DD HH24:MI:SS'), 'Russia');
INSERT INTO db_default.tb_logins VALUES (2, 'smith_demo', 'demo');

INSERT INTO db_default.tb_users VALUES (3, TO_DATE('2020-12-03 00:00:00', 'YYYY-MM-DD HH24:MI:SS'), 'Russia');
INSERT INTO db_default.tb_logins VALUES (3, 'johnson', 'real');

INSERT INTO db_default.tb_users VALUES (4, TO_DATE('2020-12-04 00:00:00', 'YYYY-MM-DD HH24:MI:SS'), 'Russia');
INSERT INTO db_default.tb_logins VALUES (4, 'williams_demo', 'demo');
INSERT INTO db_default.tb_logins VALUES (4, 'williams', 'real');

INSERT INTO db_default.tb_users VALUES (5, TO_DATE('2020-12-05 00:00:00', 'YYYY-MM-DD HH24:MI:SS'), 'Russia');
INSERT INTO db_default.tb_logins VALUES (5, 'brown_demo', 'demo');
INSERT into db_billing.tb_operations VALUES ('deposit', TO_DATE('2020-12-06 00:00:00', 'YYYY-MM-DD HH24:MI:SS'), 'brown_demo', 900);

INSERT INTO db_default.tb_users VALUES (6, TO_DATE('2020-12-06 00:00:00', 'YYYY-MM-DD HH24:MI:SS'), 'Russia');
INSERT INTO db_default.tb_logins VALUES (6, 'jones', 'real');
INSERT into db_billing.tb_operations VALUES ('deposit', TO_DATE('2020-12-07 00:00:00', 'YYYY-MM-DD HH24:MI:SS'), 'jones', 1000);

INSERT INTO db_default.tb_users VALUES (7, TO_DATE('2020-12-07 00:00:00', 'YYYY-MM-DD HH24:MI:SS'), 'Russia');
INSERT INTO db_default.tb_logins VALUES (7, 'garcia_demo', 'demo');
INSERT INTO db_default.tb_logins VALUES (7, 'garcia', 'real');
INSERT into db_billing.tb_operations VALUES ('deposit', TO_DATE('2020-12-08 00:00:00', 'YYYY-MM-DD HH24:MI:SS'), 'garcia_demo', 1000);
INSERT into db_billing.tb_operations VALUES ('deposit', TO_DATE('2020-12-09 00:00:00', 'YYYY-MM-DD HH24:MI:SS'), 'garcia', 1000);

INSERT INTO db_default.tb_users VALUES (8, TO_DATE('2020-12-08 00:00:00', 'YYYY-MM-DD HH24:MI:SS'), 'Russia');
INSERT INTO db_default.tb_logins VALUES (8, 'miller_demo', 'demo');
INSERT into db_billing.tb_operations VALUES ('deposit', TO_DATE('2020-12-09 00:00:00', 'YYYY-MM-DD HH24:MI:SS'), 'miller_demo', 1000);
INSERT into db_billing.tb_operations VALUES ('withdrawal', TO_DATE('2020-12-10 00:00:00', 'YYYY-MM-DD HH24:MI:SS'), 'miller_demo', 500);

INSERT INTO db_default.tb_users VALUES (9, TO_DATE('2020-12-09 00:00:00', 'YYYY-MM-DD HH24:MI:SS'), 'Russia');
INSERT INTO db_default.tb_logins VALUES (9, 'davis', 'real');
INSERT into db_billing.tb_operations VALUES ('deposit', TO_DATE('2020-12-10 00:00:00', 'YYYY-MM-DD HH24:MI:SS'), 'davis', 1000);
INSERT into db_billing.tb_operations VALUES ('withdrawal', TO_DATE('2020-12-11 00:00:00', 'YYYY-MM-DD HH24:MI:SS'), 'davis', 500);

INSERT INTO db_default.tb_users VALUES (10, TO_DATE('2020-12-10 00:00:00', 'YYYY-MM-DD HH24:MI:SS'), 'Russia');
INSERT INTO db_default.tb_logins VALUES (10, 'rodriguez_demo', 'demo');
INSERT INTO db_default.tb_logins VALUES (10, 'rodriguez', 'real');
INSERT into db_billing.tb_operations VALUES ('deposit', TO_DATE('2020-12-11 00:00:00', 'YYYY-MM-DD HH24:MI:SS'), 'rodriguez_demo', 1000);
INSERT into db_billing.tb_operations VALUES ('withdrawal', TO_DATE('2020-12-12 00:00:00', 'YYYY-MM-DD HH24:MI:SS'), 'rodriguez_demo', 500);
INSERT into db_billing.tb_operations VALUES ('deposit', TO_DATE('2020-12-11 00:00:00', 'YYYY-MM-DD HH24:MI:SS'), 'rodriguez', 1000);
INSERT into db_billing.tb_operations VALUES ('withdrawal', TO_DATE('2020-12-12 00:00:00', 'YYYY-MM-DD HH24:MI:SS'), 'rodriguez', 500);

INSERT INTO db_default.tb_users VALUES (11, TO_DATE('2020-12-11 00:00:00', 'YYYY-MM-DD HH24:MI:SS'), 'Russia');
INSERT INTO db_default.tb_logins VALUES (11, 'martinez_demo', 'demo');
INSERT INTO db_billing.tb_operations VALUES ('deposit', TO_DATE('2020-12-12 00:00:00', 'YYYY-MM-DD HH24:MI:SS'), 'miller_demo', 1000);
INSERT INTO db_orderstat.tb_orders VALUES ('martinez_demo', TO_DATE('2020-12-13 00:00:00', 'YYYY-MM-DD HH24:MI:SS'));

INSERT INTO db_default.tb_users VALUES (12, TO_DATE('2020-12-12 00:00:00', 'YYYY-MM-DD HH24:MI:SS'), 'Russia');
INSERT INTO db_default.tb_logins VALUES (12, 'hernandez', 'real');
INSERT INTO db_billing.tb_operations VALUES ('deposit', TO_DATE('2020-12-13 00:00:00', 'YYYY-MM-DD HH24:MI:SS'), 'hernandez', 1000);
INSERT INTO db_orderstat.tb_orders VALUES ('hernandez', TO_DATE('2020-12-14 00:00:00', 'YYYY-MM-DD HH24:MI:SS'));

INSERT INTO db_default.tb_users VALUES (13, TO_DATE('2020-12-13 00:00:00', 'YYYY-MM-DD HH24:MI:SS'), 'Russia');
INSERT INTO db_default.tb_logins VALUES (13, 'lopez_demo', 'demo');
INSERT INTO db_default.tb_logins VALUES (13, 'lopez', 'real');
INSERT INTO db_billing.tb_operations VALUES ('deposit', TO_DATE('2020-12-14 00:00:00', 'YYYY-MM-DD HH24:MI:SS'), 'lopez_demo', 1000);
INSERT INTO db_orderstat.tb_orders VALUES ('lopez_demo', TO_DATE('2020-12-15 00:00:00', 'YYYY-MM-DD HH24:MI:SS'));
INSERT INTO db_billing.tb_operations VALUES ('deposit', TO_DATE('2020-12-14 00:00:00', 'YYYY-MM-DD HH24:MI:SS'), 'lopez', 1000);
INSERT INTO db_orderstat.tb_orders VALUES ('lopez', TO_DATE('2020-12-15 00:00:00', 'YYYY-MM-DD HH24:MI:SS'));

-- Данные для Argentina.
INSERT INTO db_default.tb_users VALUES (14, TO_DATE('2020-12-14 00:00:00', 'YYYY-MM-DD HH24:MI:SS'), 'Argentina');
INSERT INTO db_default.tb_logins VALUES (14, 'gonzales', 'real');
INSERT INTO db_billing.tb_operations VALUES ('deposit', TO_DATE('2020-12-15 00:00:00', 'YYYY-MM-DD HH24:MI:SS'), 'gonzales', 5000);
INSERT INTO db_orderstat.tb_orders VALUES ('gonzales', TO_DATE('2020-12-16 00:00:00', 'YYYY-MM-DD HH24:MI:SS'));
INSERT INTO db_orderstat.tb_orders VALUES ('gonzales', TO_DATE('2020-12-17 00:00:00', 'YYYY-MM-DD HH24:MI:SS'));
INSERT INTO db_orderstat.tb_orders VALUES ('gonzales', TO_DATE('2020-12-18 00:00:00', 'YYYY-MM-DD HH24:MI:SS'));

INSERT INTO db_default.tb_users VALUES (15, TO_DATE('2020-12-15 00:00:00', 'YYYY-MM-DD HH24:MI:SS'), 'Argentina');
INSERT INTO db_default.tb_logins VALUES (15, 'wilson', 'real');
INSERT INTO db_billing.tb_operations VALUES ('deposit', TO_DATE('2020-12-16 00:00:00', 'YYYY-MM-DD HH24:MI:SS'), 'wilson', 6000);
INSERT INTO db_orderstat.tb_orders VALUES ('wilson', TO_DATE('2020-12-17 00:00:00', 'YYYY-MM-DD HH24:MI:SS'));
INSERT INTO db_orderstat.tb_orders VALUES ('wilson', TO_DATE('2020-12-18 00:00:00', 'YYYY-MM-DD HH24:MI:SS'));
INSERT INTO db_orderstat.tb_orders VALUES ('wilson', TO_DATE('2020-12-19 00:00:00', 'YYYY-MM-DD HH24:MI:SS'));
INSERT INTO db_billing.tb_operations VALUES ('withdrawal', TO_DATE('2020-12-20 00:00:00', 'YYYY-MM-DD HH24:MI:SS'), 'wilson', 3000);

INSERT INTO db_default.tb_users VALUES (16, TO_DATE('2020-12-16 00:00:00', 'YYYY-MM-DD HH24:MI:SS'), 'Argentina');
INSERT INTO db_default.tb_logins VALUES (16, 'anderson', 'real');
INSERT INTO db_billing.tb_operations VALUES ('deposit', TO_DATE('2020-12-17 00:00:00', 'YYYY-MM-DD HH24:MI:SS'), 'anderson', 7000);
INSERT INTO db_orderstat.tb_orders VALUES ('anderson', TO_DATE('2020-12-18 00:00:00', 'YYYY-MM-DD HH24:MI:SS'));
INSERT INTO db_billing.tb_operations VALUES ('deposit', TO_DATE('2020-12-19 00:00:00', 'YYYY-MM-DD HH24:MI:SS'), 'anderson', 1000);
INSERT INTO db_orderstat.tb_orders VALUES ('anderson', TO_DATE('2020-12-20 00:00:00', 'YYYY-MM-DD HH24:MI:SS'));
INSERT INTO db_billing.tb_operations VALUES ('deposit', TO_DATE('2020-12-21 00:00:00', 'YYYY-MM-DD HH24:MI:SS'), 'anderson', 2000);
INSERT INTO db_orderstat.tb_orders VALUES ('anderson', TO_DATE('2020-12-22 00:00:00', 'YYYY-MM-DD HH24:MI:SS'));

INSERT INTO db_default.tb_users VALUES (17, TO_DATE('2020-12-17 00:00:00', 'YYYY-MM-DD HH24:MI:SS'), 'Argentina');
INSERT INTO db_default.tb_logins VALUES (17, 'thomas', 'real');
INSERT INTO db_billing.tb_operations VALUES ('deposit', TO_DATE('2020-12-18 00:00:00', 'YYYY-MM-DD HH24:MI:SS'), 'thomas', 8000);
INSERT INTO db_orderstat.tb_orders VALUES ('thomas', TO_DATE('2020-12-19 00:00:00', 'YYYY-MM-DD HH24:MI:SS'));
INSERT INTO db_billing.tb_operations VALUES ('deposit', TO_DATE('2020-12-20 00:00:00', 'YYYY-MM-DD HH24:MI:SS'), 'thomas', 500);
INSERT INTO db_orderstat.tb_orders VALUES ('thomas', TO_DATE('2020-12-21 00:00:00', 'YYYY-MM-DD HH24:MI:SS'));
INSERT INTO db_billing.tb_operations VALUES ('deposit', TO_DATE('2020-12-22 00:00:00', 'YYYY-MM-DD HH24:MI:SS'), 'thomas', 1000);
INSERT INTO db_orderstat.tb_orders VALUES ('thomas', TO_DATE('2020-12-23 00:00:00', 'YYYY-MM-DD HH24:MI:SS'));
INSERT INTO db_billing.tb_operations VALUES ('withdrawal', TO_DATE('2020-12-20 00:00:00', 'YYYY-MM-DD HH24:MI:SS'), 'thomas', 7000);

-- Данные для Belgium.
INSERT INTO db_default.tb_users VALUES (18, TO_DATE('2020-12-18 00:00:00', 'YYYY-MM-DD HH24:MI:SS'), 'Belgium');
INSERT INTO db_default.tb_logins VALUES (18, 'taylor', 'real');
INSERT INTO db_billing.tb_operations VALUES ('deposit', TO_DATE('2020-12-18 08:00:00', 'YYYY-MM-DD HH24:MI:SS'), 'taylor', 500);
INSERT INTO db_orderstat.tb_orders VALUES ('taylor', TO_DATE('2020-12-18 18:00:00', 'YYYY-MM-DD HH24:MI:SS'));
INSERT INTO db_orderstat.tb_orders VALUES ('taylor', TO_DATE('2020-12-19 00:00:00', 'YYYY-MM-DD HH24:MI:SS'));
INSERT INTO db_orderstat.tb_orders VALUES ('taylor', TO_DATE('2020-12-20 00:00:00', 'YYYY-MM-DD HH24:MI:SS'));

INSERT INTO db_default.tb_users VALUES (19, TO_DATE('2020-12-19 00:00:00', 'YYYY-MM-DD HH24:MI:SS'), 'Belgium');
INSERT INTO db_default.tb_logins VALUES (19, 'moore', 'real');
INSERT INTO db_billing.tb_operations VALUES ('deposit', TO_DATE('2020-12-20 10:00:00', 'YYYY-MM-DD HH24:MI:SS'), 'moore', 600);
INSERT INTO db_orderstat.tb_orders VALUES ('moore', TO_DATE('2020-12-21 20:00:00', 'YYYY-MM-DD HH24:MI:SS'));
INSERT INTO db_orderstat.tb_orders VALUES ('moore', TO_DATE('2020-12-22 00:00:00', 'YYYY-MM-DD HH24:MI:SS'));
INSERT INTO db_orderstat.tb_orders VALUES ('moore', TO_DATE('2020-12-23 00:00:00', 'YYYY-MM-DD HH24:MI:SS'));
INSERT INTO db_billing.tb_operations VALUES ('withdrawal', TO_DATE('2020-12-24 00:00:00', 'YYYY-MM-DD HH24:MI:SS'), 'moore', 300);

INSERT INTO db_default.tb_users VALUES (20, TO_DATE('2020-12-20 00:00:00', 'YYYY-MM-DD HH24:MI:SS'), 'Belgium');
INSERT INTO db_default.tb_logins VALUES (20, 'jackson', 'real');
INSERT INTO db_billing.tb_operations VALUES ('deposit', TO_DATE('2020-12-22 12:00:00', 'YYYY-MM-DD HH24:MI:SS'), 'jackson', 700);
INSERT INTO db_orderstat.tb_orders VALUES ('jackson', TO_DATE('2020-12-24 22:00:00', 'YYYY-MM-DD HH24:MI:SS'));
INSERT INTO db_billing.tb_operations VALUES ('deposit', TO_DATE('2020-12-25 00:00:00', 'YYYY-MM-DD HH24:MI:SS'), 'jackson', 100);
INSERT INTO db_orderstat.tb_orders VALUES ('jackson', TO_DATE('2020-12-26 00:00:00', 'YYYY-MM-DD HH24:MI:SS'));
INSERT INTO db_billing.tb_operations VALUES ('deposit', TO_DATE('2020-12-27 00:00:00', 'YYYY-MM-DD HH24:MI:SS'), 'jackson', 200);
INSERT INTO db_orderstat.tb_orders VALUES ('jackson', TO_DATE('2020-12-28 00:00:00', 'YYYY-MM-DD HH24:MI:SS'));

INSERT INTO db_default.tb_users VALUES (21, TO_DATE('2020-12-21 00:00:00', 'YYYY-MM-DD HH24:MI:SS'), 'Belgium');
INSERT INTO db_default.tb_logins VALUES (21, 'martin', 'real');
INSERT INTO db_billing.tb_operations VALUES ('deposit', TO_DATE('2020-12-21 14:00:00', 'YYYY-MM-DD HH24:MI:SS'), 'martin', 800);
INSERT INTO db_orderstat.tb_orders VALUES ('martin', TO_DATE('2020-12-21 16:00:00', 'YYYY-MM-DD HH24:MI:SS'));
INSERT INTO db_billing.tb_operations VALUES ('deposit', TO_DATE('2020-12-22 00:00:00', 'YYYY-MM-DD HH24:MI:SS'), 'martin', 50);
INSERT INTO db_orderstat.tb_orders VALUES ('martin', TO_DATE('2020-12-23 00:00:00', 'YYYY-MM-DD HH24:MI:SS'));
INSERT INTO db_billing.tb_operations VALUES ('deposit', TO_DATE('2020-12-24 00:00:00', 'YYYY-MM-DD HH24:MI:SS'), 'martin', 100);
INSERT INTO db_orderstat.tb_orders VALUES ('martin', TO_DATE('2020-12-25 00:00:00', 'YYYY-MM-DD HH24:MI:SS'));
INSERT INTO db_billing.tb_operations VALUES ('withdrawal', TO_DATE('2020-12-26 00:00:00', 'YYYY-MM-DD HH24:MI:SS'), 'martin', 700);

-- Данные для Cyprus.
INSERT INTO db_default.tb_users VALUES (22, TO_DATE('2020-12-22 00:00:00', 'YYYY-MM-DD HH24:MI:SS'), 'Cyprus');
INSERT INTO db_default.tb_logins VALUES (22, 'lee', 'real');
INSERT INTO db_billing.tb_operations VALUES ('deposit', TO_DATE('2020-12-22 02:00:00', 'YYYY-MM-DD HH24:MI:SS'), 'lee', 1500);
INSERT INTO db_orderstat.tb_orders VALUES ('lee', TO_DATE('2020-12-22 04:00:00', 'YYYY-MM-DD HH24:MI:SS'));
INSERT INTO db_orderstat.tb_orders VALUES ('lee', TO_DATE('2020-12-23 00:00:00', 'YYYY-MM-DD HH24:MI:SS'));
INSERT INTO db_orderstat.tb_orders VALUES ('lee', TO_DATE('2020-12-23 00:00:00', 'YYYY-MM-DD HH24:MI:SS'));

INSERT INTO db_default.tb_users VALUES (23, TO_DATE('2020-07-23 00:00:00', 'YYYY-MM-DD HH24:MI:SS'), 'Cyprus');
INSERT INTO db_default.tb_logins VALUES (23, 'perez', 'real');
INSERT INTO db_billing.tb_operations VALUES ('deposit', TO_DATE('2020-07-24 00:00:00', 'YYYY-MM-DD HH24:MI:SS'), 'perez', 600);
INSERT INTO db_orderstat.tb_orders VALUES ('perez', TO_DATE('2020-07-25 00:00:00', 'YYYY-MM-DD HH24:MI:SS'));
INSERT INTO db_orderstat.tb_orders VALUES ('perez', TO_DATE('2020-07-26 00:00:00', 'YYYY-MM-DD HH24:MI:SS'));
INSERT INTO db_orderstat.tb_orders VALUES ('perez', TO_DATE('2020-07-28 00:00:00', 'YYYY-MM-DD HH24:MI:SS'));
INSERT INTO db_billing.tb_operations VALUES ('withdrawal', TO_DATE('2020-07-29 00:00:00', 'YYYY-MM-DD HH24:MI:SS'), 'perez', 300);

INSERT INTO db_default.tb_users VALUES (24, TO_DATE('2020-12-24 00:00:00', 'YYYY-MM-DD HH24:MI:SS'), 'Cyprus');
INSERT INTO db_default.tb_logins VALUES (24, 'thompson', 'real');
INSERT INTO db_billing.tb_operations VALUES ('deposit', TO_DATE('2020-12-24 01:00:00', 'YYYY-MM-DD HH24:MI:SS'), 'thompson', 1700);
INSERT INTO db_orderstat.tb_orders VALUES ('thompson', TO_DATE('2020-12-24 06:00:00', 'YYYY-MM-DD HH24:MI:SS'));
INSERT INTO db_billing.tb_operations VALUES ('deposit', TO_DATE('2020-12-25 00:00:00', 'YYYY-MM-DD HH24:MI:SS'), 'thompson', 100);
INSERT INTO db_orderstat.tb_orders VALUES ('thompson', TO_DATE('2020-12-26 00:00:00', 'YYYY-MM-DD HH24:MI:SS'));
INSERT INTO db_billing.tb_operations VALUES ('deposit', TO_DATE('2020-12-27 00:00:00', 'YYYY-MM-DD HH24:MI:SS'), 'thompson', 200);
INSERT INTO db_orderstat.tb_orders VALUES ('thompson', TO_DATE('2020-12-28 00:00:00', 'YYYY-MM-DD HH24:MI:SS'));

INSERT INTO db_default.tb_users VALUES (25, TO_DATE('2020-06-25 00:00:00', 'YYYY-MM-DD HH24:MI:SS'), 'Cyprus');
INSERT INTO db_default.tb_logins VALUES (25, 'white', 'real');
INSERT INTO db_billing.tb_operations VALUES ('deposit', TO_DATE('2020-06-26 00:00:00', 'YYYY-MM-DD HH24:MI:SS'), 'white', 800);
INSERT INTO db_orderstat.tb_orders VALUES ('white', TO_DATE('2020-06-27 00:00:00', 'YYYY-MM-DD HH24:MI:SS'));
INSERT INTO db_billing.tb_operations VALUES ('deposit', TO_DATE('2020-06-28 00:00:00', 'YYYY-MM-DD HH24:MI:SS'), 'white', 50);
INSERT INTO db_orderstat.tb_orders VALUES ('white', TO_DATE('2020-06-29 00:00:00', 'YYYY-MM-DD HH24:MI:SS'));
INSERT INTO db_billing.tb_operations VALUES ('deposit', TO_DATE('2020-06-30 00:00:00', 'YYYY-MM-DD HH24:MI:SS'), 'white', 100);
INSERT INTO db_orderstat.tb_orders VALUES ('white', TO_DATE('2020-07-01 00:00:00', 'YYYY-MM-DD HH24:MI:SS'));
INSERT INTO db_billing.tb_operations VALUES ('withdrawal', TO_DATE('2020-07-02 00:00:00', 'YYYY-MM-DD HH24:MI:SS'), 'white', 700);

-- Данные для Estonia. Здесь я создаю одного пользователя (некая трейдинговая компания) с несколькими счетами.
-- Я допускаю вероятность того, что на двух счетах сделали депозит в одно и то же время.
INSERT INTO db_default.tb_users VALUES (26, TO_DATE('2020-12-26 00:00:00', 'YYYY-MM-DD HH24:MI:SS'), 'Estonia');
INSERT INTO db_default.tb_logins VALUES (26, 'trading_company_john', 'real');
INSERT INTO db_default.tb_logins VALUES (26, 'trading_company_liam', 'real');
INSERT INTO db_billing.tb_operations VALUES ('deposit', TO_DATE('2020-12-26 12:00:00', 'YYYY-MM-DD HH24:MI:SS'), 'trading_company_john', 700);
INSERT INTO db_billing.tb_operations VALUES ('deposit', TO_DATE('2020-12-26 12:00:00', 'YYYY-MM-DD HH24:MI:SS'), 'trading_company_liam', 3500);
INSERT INTO db_billing.tb_operations VALUES ('deposit', TO_DATE('2020-12-27 13:00:00', 'YYYY-MM-DD HH24:MI:SS'), 'trading_company_john', 100);
INSERT INTO db_billing.tb_operations VALUES ('deposit', TO_DATE('2020-12-27 14:00:00', 'YYYY-MM-DD HH24:MI:SS'), 'trading_company_liam', 4200);
INSERT INTO db_billing.tb_operations VALUES ('deposit', TO_DATE('2020-12-28 15:00:00', 'YYYY-MM-DD HH24:MI:SS'), 'trading_company_john', 200);
INSERT INTO db_billing.tb_operations VALUES ('deposit', TO_DATE('2020-12-28 16:00:00', 'YYYY-MM-DD HH24:MI:SS'), 'trading_company_liam', 4900);
INSERT INTO db_billing.tb_operations VALUES ('deposit', TO_DATE('2020-12-29 17:00:00', 'YYYY-MM-DD HH24:MI:SS'), 'trading_company_john', 300);
INSERT INTO db_billing.tb_operations VALUES ('deposit', TO_DATE('2020-12-29 18:00:00', 'YYYY-MM-DD HH24:MI:SS'), 'trading_company_liam', 5600);
INSERT INTO db_orderstat.tb_orders VALUES ('trading_company_liam', TO_DATE('2020-12-27 10:00:00', 'YYYY-MM-DD HH24:MI:SS'));

COMMIT;

--------------------------------------------------------------------------------
-- 2. Написать запрос, который отобразит среднее время перехода пользователей между этапами воронки:
--    - От регистрации до внесения депозита
--    - От внесения депозита до первой сделки на реальном счёте
--    Только реальные счета
--    Учесть, что у пользователя может быть депозит, но не быть торговых операций
--    Период - последние 90 дней
--    Группировка - по странам
--    Сортировка - по убыванию количества пользователей
--------------------------------------------------------------------------------

-- Первый вариант решения. Даты из трех таблиц транспонируются в строки и далее анализируется массив строк
-- с использованием аналитических функций.

-- Аналитические функции - очень удобные, но делают много работы - сортируют,
-- используют временное пространство для хранения результатов работы. Поэтому в случае
-- аналитических функций, нужно максимально отфильтровать исходный набор данных
-- до тех записей, которые реально нужны, что ниже и делается -
-- фильтр по датам, счетам, операциям.

WITH t1 AS
(
SELECT u.uuid
      ,u.registration_date AS dtm
      ,1 AS flg
      ,1 AS rn
FROM   db_default.tb_users u
WHERE  u.uuid IN (SELECT l.user_uid
                  FROM   db_default.tb_logins l
                  WHERE  l.account_type != 'demo') -- Исключить demo счета.
       AND u.registration_date > TRUNC(SYSDATE - 90) -- Последние 90 дней.
                                                     -- TRUNC - отбросить секунды и смотреть с начала дня (с 00:00:00).
),

t2 AS
(
SELECT l.user_uid
      ,op.operation_date
      ,2 AS flg
      ,ROW_NUMBER() OVER (PARTITION BY l.user_uid ORDER BY op.operation_date) AS rn
FROM   db_default.tb_logins l
JOIN   db_billing.tb_operations op
ON     l.login = op.login
WHERE  l.account_type != 'demo' -- Исключить demo счета.
       AND op.operation_type = 'deposit' -- Только операции deposit.
       AND op.operation_date > TRUNC(SYSDATE - 90)
),

t3 AS
(
SELECT l.user_uid
      ,od.order_close_date
      ,3 AS flg
      ,ROW_NUMBER() OVER (PARTITION BY l.user_uid ORDER BY od.order_close_date) AS rn
FROM   db_default.tb_logins l
JOIN   db_orderstat.tb_orders od
ON     l.login = od.login
WHERE  l.account_type != 'demo' -- Исключить demo счета.
       AND od.order_close_date > TRUNC(SYSDATE - 90)
),

t4 AS
(
SELECT t1.*
FROM   t1
UNION ALL
SELECT t2.*
FROM   t2
WHERE  t2.rn = 1 -- Отобрать только первую операцию, остальные не нужны.
UNION ALL
SELECT t3.*
FROM   t3
WHERE  t3.rn = 1 -- Тоже самое. Отобрать только первую операцию, остальные не нужны.
),

t5 AS
(
SELECT t4.*
      ,COUNT(t4.flg) OVER (PARTITION BY t4.uuid) AS cnt
FROM   t4
),

t6 AS
(
SELECT t5.uuid
      ,t5.dtm
      ,t5.flg
FROM   t5
WHERE  t5.cnt = 3 -- За последние 90 дней должна быть регистрация -> операция -> сделка.
                  -- Обязательно все три операции.
),

t7 AS
(
SELECT t6.*
      ,LEAD(t6.dtm) OVER (PARTITION BY t6.uuid ORDER BY t6.dtm) AS next_dtm -- Определяется следующая дата.
FROM   t6
),

t8 AS
(
SELECT t7.uuid
      ,(t7.next_dtm - t7.dtm) * 24 * 60 * 60 AS diff_sec -- Преобразовать разницу между датами в секунды, 
                                                         -- чтобы далее посчитать среднее значение.
FROM   t7
WHERE  t7.flg = 2 -- По сути, сейчас нужны только записи с флагом 2. Именно эти записи определяют "воронку".
),

t9 AS
(
SELECT u.country
      ,TRUNC(AVG(t8.diff_sec)) AS avg_diff_sec
      ,COUNT(t8.uuid) AS number_of_customers
FROM   t8
JOIN   db_default.tb_users u
ON     t8.uuid = u.uuid
GROUP  BY u.country
)

SELECT t9.country
      ,FLOOR(t9.avg_diff_sec / 86400) || 'd ' || 
       TO_CHAR(TO_DATE(MOD(t9.avg_diff_sec, 86400), 'sssss'), 'hh24"h" mi"m" ss"s"') AS avg_diff -- Преобразуем секунды в формат день:часы:минуты:секунды.
FROM   t9
ORDER  BY t9.number_of_customers DESC;

-- Второй вариант решения. Вспомнил, что в Oracle Database 12c появилась
-- SQL конструкция CROSS APPLY, которая может упростить решение задачи.
-- Чтобы оценить, что быстрее, аналитические функции vs CROSS APPLY,
-- необходимо провести performance test. Результаты на разных БД могут отличаться -
-- разная архитектора - MPP / не MPP, внутренняя имплементация SQL функций и конструкций.

WITH t1 AS
(
SELECT u.*
      ,x.operation_date
      ,xx.order_close_date
      ,(xx.order_close_date - x.operation_date) * 24 * 60 * 60 AS diff_sec -- Преобразовать разницу между датами в секунды, 
                                                                           -- чтобы далее посчитать среднее значение.
FROM   db_default.tb_users u
CROSS  APPLY (SELECT op.operation_date
              FROM   db_default.tb_logins l
              JOIN   db_billing.tb_operations op
              ON     l.login = op.login
              WHERE  l.account_type != 'demo' -- Исключить demo счета.
                     AND op.operation_type = 'deposit' -- Только операции deposit.
                     AND l.user_uid = u.uuid
              ORDER  BY op.operation_date
              FETCH FIRST 1 ROWS ONLY) x
CROSS  APPLY (SELECT od.order_close_date
              FROM   db_default.tb_logins l
              JOIN   db_orderstat.tb_orders od
              ON     l.login = od.login
              WHERE  l.account_type != 'demo' -- Исключить demo счета.
                     AND l.user_uid = u.uuid
              ORDER  BY od.order_close_date
              FETCH FIRST 1 ROWS ONLY) xx
WHERE  u.registration_date > TRUNC(SYSDATE - 90) -- Достаточно один раз отфильтровать по дате в таблице tb_users.
                                                 -- Даты в tb_operations, tb_orders должны быть >= registration_date.
                                                 -- Также данный запрос выводит информацию только по тем, у кого были все три операции: регистрация -> операция -> сделка.
),

t2 AS
(
SELECT t1.country
      ,TRUNC(AVG(t1.diff_sec)) AS avg_diff_sec
      ,COUNT(t1.uuid) AS number_of_customers
FROM   t1
GROUP  BY t1.country
)

SELECT t2.country
      ,FLOOR(t2.avg_diff_sec / 86400) || 'd ' || 
       TO_CHAR(TO_DATE(MOD(t2.avg_diff_sec, 86400), 'sssss'), 'hh24"h" mi"m" ss"s"') AS avg_diff -- Преобразовать секунды в формат день:часы:минуты:секунды.
FROM   t2
ORDER  BY t2.number_of_customers DESC;

--------------------------------------------------------------------------------
-- 3. Написать запрос, который отобразит количество всех клиентов по странам, у которых средний депозит >=1000.
-- Вывод: country, количество клиентов в стране, количество клиентов у которых депозит >=1000
--------------------------------------------------------------------------------

-- Здесь простой запрос с группировкой и последующим LEFT JOIN.
-- Да, отбирается большая часть из таблицы tb_operations, но запрос без сложной логики, множественный соединений с другими
-- таблицами, без явной сортировки, с группировкой по числовому столбцу (подзапрос t1), плюс такого рода запросы
-- на комплексах с архитектурой MPP должны выполняться достаточно быстро.

WITH t1 AS
(
SELECT l.user_uid
FROM   db_default.tb_logins l 
JOIN   db_billing.tb_operations o
ON     l.login = o.login
WHERE  o.operation_type = 'deposit' -- Только операции deposit.
       AND l.account_type != 'demo' -- Исключаются demo счета. В условиях этого пункта про это не сказано, но думаю
                                    -- здесь тоже будет уместно, как и в пункте 1.
GROUP  BY l.user_uid
HAVING AVG(o.amount) >= 1000
)

SELECT u.country
      ,COUNT(u.uuid) AS number_of_customers
      ,NVL(COUNT(t1.user_uid), 0) AS number_of_customers_with_1000
FROM   db_default.tb_users u
LEFT   JOIN t1
ON     u.uuid = t1.user_uid
GROUP  BY u.country;

--------------------------------------------------------------------------------
-- 4. Написать запрос, который выводит первые 3 депозита каждого клиента.
--    Вывод: uuid, login, operation_date, порядковый номер депозита
--------------------------------------------------------------------------------

-- Требуется уточнение: Выводить клиентов, у которых вообще не было депозитов?
-- По-умолчанию, посчитал, что нет.

-- Здесь простой запрос с функцией ранжирования.
-- Да, отбирается большая часть из таблицы tb_operations и
-- присутствует функция ранжирования, но функция группирует и сортирует по двум столбцам,
-- плюс на комплексах с архитектурой MPP такого рода запросы должны выполняться достаточно быстро.

WITH t1 AS
(
SELECT l.user_uid
      ,l.login
      ,o.operation_date
      ,DENSE_RANK() OVER(PARTITION BY l.user_uid ORDER BY o.operation_date) AS rn -- Сначала хотел использовать ROW_NUMBER(),
                                                                                  -- но изменил на DENSE_RANK(),
                                                                                  -- т.к. есть вероятность внесения
                                                                                  -- депозитов на 2 и более счетов одновременно.
FROM   db_default.tb_logins l 
JOIN   db_billing.tb_operations o
ON     l.login = o.login
WHERE  o.operation_type = 'deposit' -- Только операции deposit.
       AND l.account_type != 'demo' -- Исключить demo счета. В условиях этого пункта про это не сказано, но думаю
                                    -- здесь тоже будет уместно, как в пункте 1.
)

SELECT t1.*
FROM   t1
WHERE  t1.rn <= 3;