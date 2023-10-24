import time

import httplib2
import apiclient.discovery
from oauth2client.service_account import ServiceAccountCredentials
import glob


class Google_Sheets:
    CREDENTIALS_FILE = None
    credentials = None
    httpAuth = None
    service = None
    spreadsheetId = None

    def __init__(self, tableID, file):
        self.CREDENTIALS_FILE = file
        self.credentials = ServiceAccountCredentials.from_json_keyfile_name(self.CREDENTIALS_FILE,
                                                                       ['https://www.googleapis.com/auth/spreadsheets',
                                                                        'https://www.googleapis.com/auth/drive'])
        self.httpAuth = self.credentials.authorize(httplib2.Http())
        self.service = apiclient.discovery.build('sheets', 'v4', http=self.httpAuth)
        self.spreadsheetId = tableID

    def get_results(self, ranges):
        return self.service.spreadsheets().get(spreadsheetId=self.spreadsheetId,
                                               ranges=ranges,
                                               includeGridData=True).execute()

    def read_cell(self, sheet, cell):
        ranges = f'{sheet}!{cell}'
        results = self.get_results(ranges)
        return results['sheets'][0]['data'][0]['rowData'][0]['values'][0]['formattedValue']

    def read_rows(self, sheet, range):
        ranges = f'{sheet}!{range}'
        results = self.service.spreadsheets().values().batchGet(spreadsheetId=self.spreadsheetId,
                                                                ranges=ranges,
                                                                valueRenderOption='FORMATTED_VALUE').execute()
        return results['valueRanges'][0]['values']

    def update_cell(self, sheet, cell, value):
        body = {"valueInputOption": "RAW",
                "data": [{"range": f"{sheet}!{cell}",
                          "majorDimension": "ROWS",
                          "values": [[value]]
                          }]
                }
        self.service.spreadsheets().values().batchUpdate(spreadsheetId=self.spreadsheetId,
                                                                   body=body).execute()

    def append_rows(self, sheet, cell, rows):
        body = {"valueInputOption": "RAW",
                "data": [{"range": f"{sheet}!{cell}",
                          "majorDimension": "ROWS",
                          "values": rows
                          }]
                }
        self.service.spreadsheets().values().batchUpdate(spreadsheetId=self.spreadsheetId,
                                                         body=body).execute()

    def append_col_s(self, sheet, cell, col_s):
        body = {"valueInputOption": "RAW",
                "data": [{"range": f"{sheet}!{cell}",
                          "majorDimension": "COLUMNS",
                          "values": col_s
                          }]
                }
        self.service.spreadsheets().values().batchUpdate(spreadsheetId=self.spreadsheetId,
                                                         body=body).execute()

    def clear_sheet(self, sheet, rowcount='', colcount=''):
        properties = self.get_results([sheet])['sheets'][0]['properties']
        if rowcount == '':
            properties['gridProperties']['rowCount'] = 1
        else:
            properties['gridProperties']['rowCount'] = rowcount
        if colcount == '':
            properties['gridProperties']['columnCount'] = 1
        else:
            properties['gridProperties']['columnCount'] = colcount
        body = {
            'requests': [
                {
                    'updateSheetProperties': {
                        'properties': properties,
                        'fields': 'gridProperties.rowCount, gridProperties.columnCount'
                    }
                }
            ]
        }
        self.update_cell(sheet, 'A1', '')
        self.service.spreadsheets().batchUpdate(spreadsheetId=self.spreadsheetId,
                                                         body=body).execute()

    def group_rows(self, sheet, col_n, levels):
        sheet_id = self.get_results([sheet])['sheets'][0]['properties']['sheetId']
        rows = [i[0] for i in self.read_rows(sheet, f'{col_n}:{col_n}')]
        for k in sorted(levels.keys()):
            indexes = [i + 1 for i in range(len(rows)) if rows[i] == levels[k]] + ['stop']
            if k != 1:
                indexes = [i + 1 for i in range(len(rows)) if rows[i] == levels[k] or rows[i] == levels[k - 1]] + ['stop']
            for i in enumerate(indexes):
                start_index = i[1]
                if start_index == 'stop':
                    break
                if indexes[i[0] + 1] == 'stop':
                    end_index = rows.index(rows[-1])+1
                else:
                    end_index = indexes[i[0] + 1] - 1
                dimensionRange = {
                    'sheetId': sheet_id,
                    'dimension': 'ROWS',
                    'startIndex': start_index,
                    'endIndex': end_index
                }
                body = {
                    'requests': [
                        {
                            'addDimensionGroup': {
                                'range': dimensionRange
                            }
                        }
                    ]
                }
                try:
                    self.service.spreadsheets().batchUpdate(spreadsheetId=self.spreadsheetId, body=body).execute()
                    time.sleep(1)
                except Exception as ex:
                    print(ex)
                    time.sleep(3)
                    self.group_rows(sheet, col_n, levels)

    def collapsed_rows(self, sheet, level):
        groups = self.get_results([sheet])['sheets'][0]['rowGroups']
        for group in groups:
            if group['depth'] != level:
                continue
            group['collapsed'] = True
            body = {
                'requests': [
                    {
                        'updateDimensionGroup': {
                            'dimensionGroup': group,
                            'fields': 'collapsed'
                        }
                    }
                ]
            }
            self.service.spreadsheets().batchUpdate(spreadsheetId=self.spreadsheetId,
                                                    body=body).execute()

    def autoChange_size_columns(self, sheet):
        sheet_id = self.get_results([sheet])['sheets'][0]['properties']['sheetId']
        columnCount = self.get_results([f'{sheet}'])['sheets'][0]['data'][0]['columnMetadata']
        dimensionRange = {
            'sheetId': sheet_id,
            'dimension': 'COLUMNS',
            'startIndex': 0
        }
        body = {
            'requests': [
                {
                    'autoResizeDimensions': {
                        'dimensions': dimensionRange,
                    }
                }
            ]
        }
        self.service.spreadsheets().batchUpdate(spreadsheetId=self.spreadsheetId,
                                                body=body).execute()
        for column in columnCount:
            pixelSize = column['pixelSize'] + 10
            range = {
                'sheetId': sheet_id,
                'dimension': 'COLUMNS',
                'startIndex': columnCount.index(column),
                'endIndex': columnCount.index(column) + 1
            }
            properties = {
                "pixelSize": pixelSize
            }
            fields = "pixelSize"
            request_2 = {
                'updateDimensionProperties': {
                    'range': range,
                    'properties': properties,
                    'fields': fields
                }
            }
            body = {
                'requests': [request_2]
            }
            self.service.spreadsheets().batchUpdate(spreadsheetId=self.spreadsheetId,
                                                    body=body).execute()

    def set_alignment_left(self, sheet):
        sheet_id = self.get_results([sheet])['sheets'][0]['properties']['sheetId']
        range = {
            'sheetId': sheet_id,
            'startColumnIndex': 1
        }
        cell = {
            'userEnteredFormat': {
                'horizontalAlignment': 'LEFT',
                'verticalAlignment': 'TOP'
            }
        }
        fields = 'userEnteredFormat.horizontalAlignment, userEnteredFormat.verticalAlignment'
        request_1 = {
            'repeatCell': {
                'range': range,
                'cell': cell,
                'fields': fields
            }
        }
        body = {
            'requests': [request_1]
        }
        self.service.spreadsheets().batchUpdate(spreadsheetId=self.spreadsheetId,
                                                body=body).execute()


if __name__ == '__main__':
    table = Google_Sheets('1NHD9lX8j5a5nXDHqvS8vpNb-gFyWv1h6dii3CPjcrh0', glob.glob('*.json')[0])
    # rows = [['№', 'Дата и время', 'ТС', 'Прицеп', 'Заправлено в бак, л', 'Заправлено в прицеп, л', 'Заправлено по карте, л', 'Тип топлива', 'Разница, л', 'Расхождение, %', 'Топливная карта', 'Тип карты', 'Стоимость', 'Сумма', 'Адрес'], ['Итого', '-', 'Р064АР198 КамАЗ NEO Реф Леонченко', 'УУ7026 77 РЕФ Прицеп', '1030.64', '0.00', '500.00', 'ДТ-Меж', '-530.64', '-106.13', '7826010118796834', 'Роснефть', '55.25', '27625.00', '-'], [2, '06.02.2023 20:21:09', 'Р064АР198 КамАЗ NEO Реф Леонченко', 'УУ7026 77 РЕФ Прицеп', '522.94', 0, 500.0, 'ДТ-Меж', '-22.94', '-4.59', '7826010118796834', 'Роснефть', '55.25', 27625.0, 'Россия, Краснодарский край, Северский район, А146, 30 км, справа, пгт. Афипский '], [3, '09.02.2023 19:05:05', 'Р064АР198 КамАЗ NEO Реф Леонченко', 'УУ7026 77 РЕФ Прицеп', '507.70', 0, 0, '-', '-507.70', '100.00', '-', '-', '-', 0, '-'], ['Итого', '-', 'Х507ОМ178 КамАЗ NEO', '-', '812.19', '0.00', '0.00', '-', '-812.19', '100.00', '-', '-', '-', '0.00', '-'], [2, '07.02.2023 10:13:32', 'Х507ОМ178 КамАЗ NEO', '-', '301.81', 0, 0, '-', '-301.81', '100.00', '-', '-', '-', 0, '-'], [3, '08.02.2023 05:23:28', 'Х507ОМ178 КамАЗ NEO', '-', '252.64', 0, 0, '-', '-252.64', '100.00', '-', '-', '-', 0, '-'], [4, '10.02.2023 16:34:20', 'Х507ОМ178 КамАЗ NEO', '-', '257.74', 0, 0, '-', '-257.74', '100.00', '-', '-', '-', 0, '-'], ['Итого', '-', 'О813ХК198 КамАЗ NEO', '-', '800.00', '0.00', '0.00', '-', '-800.00', '100.00', '-', '-', '-', '0.00', '-'], [2, '07.02.2023 14:19:51', 'О813ХК198 КамАЗ NEO', '-', '358.33', 0, 0, '-', '-358.33', '100.00', '-', '-', '-', 0, '-'], [3, '09.02.2023 09:51:02', 'О813ХК198 КамАЗ NEO', '-', '401.47', 0, 0, '-', '-401.47', '100.00', '-', '-', '-', 0, '-'], [4, '09.02.2023 18:58:19', 'О813ХК198 КамАЗ NEO', '-', '40.20', 0, 0, '-', '-40.20', '100.00', '-', '-', '-', 0, '-']]
    # table.group_rows('Заправки', 'B', ['Итого по ТС'])
    # print(table.get_results(['Заправки!A:A']))
    table.autoChange_size_columns('Заправки')
    levels = {1: 'Итого по ТС',
              2: 'Итого по датчику'}
    table.group_rows('Заправки', 'B', levels)
    table.collapsed_rows('Заправки', 2)
    # print(table.read_rows('Заправки', 'A:ZZ'))

    # table.clear_sheet('Заправки')
    # table.update_cell('Отчет', 'D2', 'python')
    # table.append_col_s('Список ТС и групп', 'A5', [['1', '2'],['3', '4']])
    # table.append_rows('Заправки', 'A0', rows)
    # table.append_rows('Отчет', 'A5', for_delete)