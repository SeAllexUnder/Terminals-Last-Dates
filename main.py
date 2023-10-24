import glob
import os
import bz2
import time
# import Google_Sheets
from datetime import datetime
# from Google_Sheets import Google_Sheets as gs
import gspread


def read_directory(dir_name):
    # print(dir_name)
    dates = {}
    file_list = os.listdir(dir_name)
    sub_directories = [file for file in file_list if '.bz2' not in file]
    archives = [file for file in file_list if '.bz2' in file]
    if len(archives) != 0:
        dates[dir_name] = {}
    for a in archives:
        with bz2.open(f'{dir_name}/{a}', 'r') as bz2_file:
            rows = bz2_file.readlines()
            for r in rows:
                row = r.decode('utf-8')
                terminal = row.split(';')[2]
                date = row.split(';')[1]
                unix_date = ''
                try:
                    unix_date = int(datetime.strptime(date, "%Y-%m-%d %H:%M:%S.%f").timestamp())
                except ValueError:
                    # unix_date = int(datetime.strptime(date, "%Y-%m-%d %H:%M:%S").timestamp())
                    continue
                if terminal not in dates[dir_name]:
                    dates[dir_name][terminal] = []
                dates[dir_name][terminal].append(unix_date)

    # print(sub_directories)
    for sub in sub_directories:
        sub_last_dates = read_directory(f'{dir_name}/{sub}')
        for sld in sub_last_dates.keys():
            dates[sld] = sub_last_dates[sld]
    return dates


def main():
    last_dates = read_directory('D:\Back up')
    # print('Архивы считаны!\n\n')
    rows = []
    gs = gspread.service_account('refuelings-0679028911f4.json')
    table = gs.open_by_key('1CTYD6IilWWe-3MMgALWBYwSpFg-3a0kbaAG9wB_bMRc')
    info_sheet = table.worksheet('Информация по ТС')
    info_values = info_sheet.get_values()
    info_vehicles = {}
    client_vehicles = {}
    for val in info_values:
        if val[1] == '' or val[1] == 'IMEI':
            continue
        info_vehicles[val[1]] = val[0]
        client_vehicles[val[1]] = val[3]
    for ls in last_dates:
        # print(ls)
        port = ls.split('/')[-1]
        for terminal in last_dates[ls]:
            # print(terminal + ': ' + len(last_dates[ls][terminal]))
            first_date = datetime.fromtimestamp(min(last_dates[ls][terminal])).strftime('%d.%m.%Y %H:%M:%S')
            last_date = datetime.fromtimestamp(max(last_dates[ls][terminal])).strftime('%d.%m.%Y %H:%M:%S')
            count = len(last_dates[ls][terminal])
            vehicle_name, client = 'Не определено', 'Не определен'
            try:
                vehicle_name = info_vehicles[terminal]
            except KeyError:
                pass
            try:
                client = client_vehicles[terminal]
            except KeyError:
                pass
            row = [
                str(client),
                str(port),
                terminal,
                vehicle_name,
                first_date,
                last_date,
                count
            ]
            rows.append(row)
    worksheet = table.worksheet('Снятие данных с терминалов')
    row_count = worksheet.row_count
    worksheet.batch_clear([f'A2:G{row_count}'])
    worksheet.update('A2', rows)


if __name__ == '__main__':
    main()
    # exit()
    while True:
        # print(datetime.now())
        if datetime.now().minute == 55:
            main()
        time.sleep(60)
    # print(int(datetime.now().minute))
    # main()
