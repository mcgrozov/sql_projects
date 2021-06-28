import sqlite3
import pandas as pd
import shutil
import os


conn = sqlite3.connect('project.db')
cursor = conn.cursor()
passport_files = ['passport_blacklist_01032021.xlsx',
                   'passport_blacklist_02032021.xlsx',
                   'passport_blacklist_03032021.xlsx']

terminal_files = ['terminals_01032021.xlsx',
                  'terminals_02032021.xlsx',
                  'terminals_03032021.xlsx']

transaction_files = ['transactions_01032021.txt',
                     'transactions_02032021.txt',
                     'transactions_03032021.txt']

csv_transactions = ['transactions_01032021.csv',
                    'transactions_02032021.csv',
                    'transactions_03032021.csv']


def create_scd(filepath):
    with open(filepath) as file:
        for line in file.readlines():
            cursor.execute(line)


def create_scd2():
    cursor.executescript("""
    create table STG_PASSPORT_BLACKLIST(
        passport_num varchar(64),
        entry_dt date,
        effective_from datetime default current_timestamp,
        effective_to datetime default (datetime('2999-12-31 23:59:59')));
    
    create table STG_TERMINALS(
        terminal_id varchar(64),
        terminal_type varchar(64),
        terminal_city varchar(64),
        terminal_address varchar(64),
        effective_from datetime default current_timestamp,
        effective_to datetime default (datetime('2999-12-31 23:59:59')););
    
    create table STG_TRANSACTIONS(
        trans_id varchar(64),
        trans_date date,
        card_num varchar(64),
        oper_type varchar(64),
        amt decimal,
        oper_result varchar(64),
        terminal varchar(64),
        effective_from datetime default current_timestamp,
        effective_to datetime default (datetime('2999-12-31 23:59:59')));
    """)
    conn.commit()


def csv2sql(dir, table):
    for i in dir:
        read_file = pd.read_csv('/Users/mcgrozov/Desktop/Sber/Проект/' + i)
        read_file.to_csv('/Users/mcgrozov/Desktop/Sber/Проект/', index=None)
        df = pd.read_csv('/Users/mcgrozov/Desktop/Sber/Проект/' + i)
        df.to_sql(table, con=conn, if_exists='append')
    conn.commit()

def txt2csv(dir,table):
    for i in transaction_files:
        with open('/Users/mcgrozov/Desktop/Sber/Проект/' + i, "r+") as file:
            read_file = pd.read_csv('/Users/mcgrozov/Desktop/Sber/Проект/' + i)
            read_file.to_csv("/Users/mcgrozov/Desktop/Sber/Проект/used_data/" + i[:-4] + ".csv")
    for i in dir:
        df = pd.read_csv("/Users/mcgrozov/Desktop/Sber/Проект/archive/" + i)
        df.to_sql(table, conn, if_exists='replace', index=2)
    conn.commit()

def xlsx2sql(dir,table):
    for i in dir:
        df = pd.read_excel('/Users/mcgrozov/Desktop/Sber/Проект/' +i, index_col=1)
        df.to_sql(name=table, con=conn, if_exists='append')
    conn.commit()

def file2archive():
    all_files = []
    for files in os.walk("."):
        for filename in files:
            all_files.append(filename)
    for file in all_files:
        file = file + '.backup'
    for file in all_files:
        shutil.move('/Users/mcgrozov/Desktop/Sber/Проект/', '/Users/mcgrozov/Desktop/Sber/Проект/archive/')

def check_table(table):
    cursor.execute(f"select * from {table}")
    for row in cursor.fetchall():

        print(row)

def drop_all_tables():
    cursor.executescript("""
    DROP TABLE IF EXISTS STG_PASSPORT_BLACKLIST;
    DROP TABLE IF EXISTS STG_TERMINALS;
    DROP TABLE IF EXISTS STG_TRANSACTIONS;
    """)
    conn.commit()

def check_blacklist_passport():
    cursor.executescript("""
    create view scam_blacklist as select 
        trans_date,
        passport_num,
        last_name,
        first_name,
        phone
    from STG_TRANSACTIONS t inner join cards c on t.card_num = c.card_num
    inner join account a on c.account_num = a.account_num
    inner join clients cl on a.client = cl.client_id,
    STG_PASSPORT_BLACKLIST
    where cl.passport_num = STG_PASSPORT_BLACKLIST.passport_num
    """)

def check_not_valid_account():
    cursor.executescript("""
    create view not_valid_account as select 
        trans_date,
        passport_num,
        last_name,
        first_name,
        phone
    from STG_TRANSACTIONS t inner join cards c on t.card_num = c.card_num
    inner join account a on c.account_num = a.account_num
    inner join clients cl on a.client = cl.client_id
    where CAST(strftime('%s', trans_date)  AS  integer) > CAST(strftime('%s', accounts.valid_to)  AS  integer)
    """)
