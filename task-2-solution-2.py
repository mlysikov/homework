# ----------------------------------------------------------------------------------------------------------------------
# 2. На любом языке программирования реализовать сервис по сбору агрегатов из таблицы БД с банковскими транзакциями.
# ----------------------------------------------------------------------------------------------------------------------

# На стороне базы данных создана view, откуда REST сервис забирает данные:
# CREATE OR REPLACE VIEW v_recon_tran_aggregates as
# SELECT t.client_id
#       ,TO_CHAR(t.transaction_date, 'dd') AS dtd
#       ,TO_CHAR(t.transaction_date, 'mm') AS dtm
#       ,SUM(t.amount) AS total_amount
# FROM   admin.src_data t
# GROUP  BY t.client_id
#          ,TO_CHAR(t.transaction_date, 'dd')
#          ,TO_CHAR(t.transaction_date, 'mm')
# ORDER BY t.client_id;

# Чтобы я усовершенствовал в текущем коде:
# * Вместо view сделать materialized views с заданным интервалом пересчета

import requests
import json
import time

def get_tran_aggragates():
    start = time.time()

    req = requests.get('https://fu5xv60hmsuglge-db201910301628.adb.eu-frankfurt-1.oraclecloudapps.com/ords/admin/recon_tran_aggregates/get_all_records')
    js = req.json()
    #print(json.dumps(js, indent=4))

    print('client_id', 'dtd', 'dtm', 'total_amount')
    print('---------', '---', '---', '------------')
    for item in js['items']:
        print("{:>9}".format(item['client_id']), "{:>3}".format(item['dtd']), "{:>3}".format(item['dtm']), "{:>12}".format(item['total_amount']))

    elapsed = (time.time() - start)
    print("Data has been fetched in", elapsed, "seconds")

get_tran_aggragates()