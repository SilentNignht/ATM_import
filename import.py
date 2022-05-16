import configparser
import os
import csv
import pymysql

# array chunk, 把一個list每N個切成一個array
# ref https://stackoverflow.com/questions/312443/how-do-you-split-a-list-into-evenly-sized-chunks


def chunks(lst, n):
    """Yield successive n-sized chunks from lst."""
    for i in range(0, len(lst), n):
        yield lst[i:i + n]


# 讀取設定檔案
config = configparser.RawConfigParser()
cwd = os.getcwd()
config_path = os.path.join(cwd, 'config.ini')
config.read(config_path)

# 讀取ATM提款機資料
arr_bank = []
arr_city = []
arr_atm = []

# TODO: 應該透過網址下載, 現在暫時先用本地檔案
# data source: https://data.gov.tw/dataset/24333
csv_path = os.path.join(cwd, 'test1.csv')
with open(csv_path, newline='', encoding="utf-8") as csv_reader:
    # 跳過第一行
    next(csv_reader)
    rows = csv.reader(csv_reader)
    for row in rows:

        # row[0]=銀行代碼, row[1]=銀行名稱
        # row[2]=ATM位置, row[3]=ATM所在城市
        # row[4]=ATM所在地址, row[5]=lat
        # row[6]=lng

        bank = [row[0], row[1]]
        city = row[3]

        # 將不重複的城市放入arr_city array
        if city not in arr_city:
            arr_city.append(city)
        # 將不重複的銀行放入arr_bank array
        if bank not in arr_bank:
            arr_bank.append(bank)
        arr_atm.append(row)

# 寫入資料庫

# DB 設定

# 資料庫設定
db_settings = {
    "host": config.get('atm-import', 'mysql_hostname'),
    "port": 3306,
    "user": config.get('atm-import', 'mysql_username'),
    "password": config.get('atm-import', 'mysql_password'),
    "db": config.get('atm-import', 'mysql_database'),
    "charset": "utf8"
}


try:
    # 建立Connection物件
    conn = pymysql.connect(**db_settings)

    # 建立Cursor物件
    with conn.cursor() as cursor:
        # atm/bank/city清空資料SQL語法
        cmd_disable_fg = 'SET FOREIGN_KEY_CHECKS = 0;'
        cmd_truncate_atm = 'TRUNCATE TABLE atm;'
        cmd_truncate_city = 'TRUNCATE TABLE city;'
        cmd_truncate_bank = 'TRUNCATE TABLE bank;'
        cmd_enable_fg = 'SET FOREIGN_KEY_CHECKS = 1;'
        cursor.execute(cmd_disable_fg)
        cursor.execute(cmd_truncate_atm)
        cursor.execute(cmd_truncate_city)
        cursor.execute(cmd_truncate_bank)
        cursor.execute(cmd_enable_fg)
        conn.commit()
        print('TRUNCATE TABLES...')

        # bank/city新增資料SQL語法
        cmd_city = 'INSERT INTO city (city_name) VALUES '
        cmd_bank = 'INSERT INTO bank (bank_code, bank_name) VALUES '
        # 資料庫的VALUES後面可以一次放多筆資料，先將資料轉換成(XXX, XXX)供SQL使用
        sql_vals_city = []
        sql_vals_bank = []
        for city in arr_city:
            sql_vals_city.append('(\"'+city+'")')
        for bank in arr_bank:
            sql_vals_bank.append('('+bank[0].zfill(3)+', "'+bank[1]+'")')

        cursor.execute(cmd_city + (",".join(sql_vals_city)) + ";")
        cursor.execute(cmd_bank + (",".join(sql_vals_bank)) + ";")

        print('INSERT CITY and BANK...')
        conn.commit()

        cmd_query_city = 'SELECT * FROM city;'
        cmd_query_bank = 'SELECT * FROM bank;'

        cursor.execute(cmd_query_city)
        data_all_city = cursor.fetchall()
        print('FETCH ALL CITY...')
        conn.commit()

        cursor.execute(cmd_query_bank)
        data_all_bank = cursor.fetchall()
        print('FETCH ALL BANK...')
        conn.commit()

        # 把產生出來的資料轉成dict, 比較方便查詢

        dict_all_city = dict()
        dict_all_bank = dict()
        for city in data_all_city:
            dict_all_city[city[1]] = city
        for bank in data_all_bank:
            dict_all_bank[bank[2]] = bank

        # atm新增資料SQL語法
        # 資料庫的VALUES後面可以一次放多筆資料，先將資料轉換成(XXX, XXX)供SQL使用
        cmd_atm = 'INSERT INTO atm (bank_id, city_id, atm_location, atm_address, lat, lng) VALUES '
        sql_vals_atm = []

        for atm in arr_atm:
            city_id = str(dict_all_city[atm[3]][0])
            bank_id = str(dict_all_bank[atm[1]][0])
            sql_vals_atm.append('('+bank_id+', '+city_id+', "'+atm[2]+'", "'+atm[4]+'", '+atm[5]+', '+atm[6]+')')

        # 每chunk_size筆塞一次，避免SQL超過最長指令限制
        # https://stackoverflow.com/questions/3536103/mysql-how-many-rows-can-i-insert-in-one-single-insert-statement
        chunk_size = 1000
        for chunk_atm in list(chunks(sql_vals_atm, chunk_size)):
            cursor.execute(cmd_atm + (",".join(chunk_atm)) + ";")
            print(f"INSERT ATM... with chunk_size = {chunk_size}")
            conn.commit()

        conn.close()
except Exception as ex:
    print(ex)
