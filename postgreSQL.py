import psycopg2
import json


class PG_SQL:
    dbname = ''
    user = ''
    password = ''
    host = ''
    connection = None
    columns = {'cardNum': 'bigint',
               'drivers': 'text',
               'dates': 'int',
               'amounts': 'real',
               'prices': 'real',
               'sums': 'real',
               'posBrands': 'text',
               'latitude': 'real',
               'longitude': 'real',
               'posAddress': 'text',
               'serviceName': 'text'
               }
    read = None
    write = None

    def __init__(self):
        with open('config_SQL.json', encoding='utf-8') as p:
            config_sql = json.load(p)
        self.dbname = config_sql["db_name"]
        self.user = config_sql["user"]
        self.password = config_sql["password"]
        self.host = config_sql["host"]
        self.read = read_SQL()
        self.write = write_SQL()

    def _connect(self):
        try:
            self.connection = psycopg2.connect(dbname=self.dbname,
                                               user=self.user,
                                               password=self.password,
                                               host=self.host)
        except Exception as _ex_connect:
            print('Подключение к БД - ', _ex_connect)
        return self.connection

    def _disconnect(self):
        self.connection.close()


class read_SQL(PG_SQL):

    def __init__(self):
        with open('config_SQL.json', encoding='utf-8') as p:
            config_sql = json.load(p)
        self.dbname = config_sql["db_name"]
        self.user = config_sql["user"]
        self.password = config_sql["password"]
        self.host = config_sql["host"]

    def read_max_val_in_column(self, table, column, schema='', filters: dict = None):
        """
        :param filters: фильтр AND по колонкам {'Название колонки': 'Искомое значение'}. Колонок для фильтрации может
        быть несколько.
        :param schema: схема для подключения к таблице
        :param table: наименование таблицы
        :param column: столбец, по которому произвести сортировку (от макс. к мин.)
        :return: максимальное значение в столбце
        """
        sc = ''
        f = ''
        if schema != '':
            sc = f'{schema}.'
        if filters is not None:
            f = ' WHERE ' + ' AND '.join([f'{f_r}={filters[f_r]}' for f_r in filters.keys()])
        command = f'SELECT {column} FROM {sc}{table}{f} ORDER BY {column} DESC'
        self._connect()
        with self.connection.cursor() as cursor:
            try:

                cursor.execute(command)
                row = cursor.fetchone()
            except Exception as _ex:
                print('Чтение строк - ', _ex)
        self._disconnect()
        try:
            return row[0]
        except TypeError:
            return 0
        except UnboundLocalError:
            return 0

    def read_rows(self, table, col_s=None, schema='', filters: dict = None, limit=None):
        all_rows = []
        param = '*'
        sc = ''
        f = ''
        l = ''
        if schema != '':
            sc = f'{schema}.'
        if filters is not None:
            f = ' WHERE ' + ' AND '.join([f'{f_r}{filters[f_r]}' for f_r in filters.keys()])
        if col_s is not None:
            if len(col_s) > 1:
                param = ', '.join(col_s)
            elif len(col_s) == 1:
                param = str(col_s[0])
        if limit is not None:
            l = f' LIMIT {limit}'
        command = f'SELECT {param} FROM {sc}{table}{f}{l}'
        self._connect()
        with self.connection.cursor() as cursor:
            try:
                cursor.execute(command)
                all_rows = cursor.fetchall()
            except Exception as _ex_read_rows:
                print('Чтение строк - ', _ex_read_rows)
        self._disconnect()
        return all_rows


class write_SQL(PG_SQL):

    def __init__(self):
        with open('config_SQL.json', encoding='utf-8') as p:
            config_sql = json.load(p)
        self.dbname = config_sql["db_name"]
        self.user = config_sql["user"]
        self.password = config_sql["password"]
        self.host = config_sql["host"]

    def append_rows(self, table, rows, columns=None, schema=''):
        sc = ''
        if schema != '':
            sc = f'{schema}.'
        if columns is not None:
            col_s = f"({', '.join(columns)})"
        else:
            col_s = f"({', '.join([key for key in rows.keys()])})"
        count = len(rows[[key for key in rows.keys()][0]])
        # data = [tuple([str(value[i]) for value in rows.values()]) for i in range(count)]
        # rows_records = ', '.join(["%s"] * len(data))
        for i in range(count):
            rows_records = tuple([str(value[i]) for value in rows.values()])
            command = f'INSERT INTO {sc}{table}{col_s} VALUES {rows_records}'
            self._connect()
            with self.connection.cursor() as cursor:
                try:
                    cursor.execute(command)
                    self.connection.commit()
                    print(f'Строка {i+1} из {count} занесена в {table}')
                except Exception as _ex_append_rows:
                    pass
            self._disconnect()


if __name__ == '__main__':
    sql = PG_SQL()
    print(sql.read.read_rows(table='refuelings', schema='refuelings'))
