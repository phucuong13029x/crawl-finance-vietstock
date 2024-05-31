import requests
import pandas as pd
from datetime import datetime, timedelta
from bs4 import BeautifulSoup


bypass = [
    '2024-09-03 00:00:00.000',
    '2024-09-02 00:00:00.000',
    '2024-05-01 00:00:00.000',
    '2024-04-30 00:00:00.000',
    '2024-04-29 00:00:00.000',
    '2024-04-18 00:00:00.000',
    '2024-02-14 00:00:00.000',
    '2024-02-13 00:00:00.000',
    '2024-02-12 00:00:00.000',
    '2024-02-11 00:00:00.000',
    '2024-02-10 00:00:00.000',
    '2024-02-09 00:00:00.000',
    '2024-02-08 00:00:00.000',
    '2024-01-01 00:00:00.000',
    '2023-09-04 00:00:00.000',
    '2023-09-03 00:00:00.000',
    '2023-09-02 00:00:00.000',
    '2023-09-01 00:00:00.000',
    '2023-05-03 00:00:00.000',
    '2023-05-02 00:00:00.000',
    '2023-05-01 00:00:00.000',
    '2023-04-30 00:00:00.000',
    '2023-04-29 00:00:00.000',
    '2023-01-26 00:00:00.000',
    '2023-01-25 00:00:00.000',
    '2023-01-24 00:00:00.000',
    '2023-01-23 00:00:00.000',
    '2023-01-22 00:00:00.000',
    '2023-01-21 00:00:00.000',
    '2023-01-20 00:00:00.000',
    '2023-01-02 00:00:00.000'
]

class Vietstock:
    def __init__(self) -> None:
        self.url       = 'https://finance.vietstock.vn/data/'
        self.endpoint  = {
            'thong-ke-gia': 'KQGDThongKeGiaPaging',
            'thong-ke-lenh': 'KQGDThongKeDatLenhPaging',
            'gd-khop-lenh-nn': 'KQGDGiaoDichNDTNNPaging',
            'gd-thoa-thuan-nn': 'KQGDGiaoDichNDTNNPaging',
            'gd-td': 'KQGDGiaoDichTuDoanhPaging'
        }
        self.url_token = 'https://finance.vietstock.vn/ket-qua-giao-dich'
        self.catid     = {
            'HSX': '1',
            'HNX': '2',
            'UPX': '3',
            'VN30': '4',
            'HNX30': '5'
        }
        self.headers   = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:126.0) Gecko/20100101 Firefox/126.0',
            'Content-Type': 'application/x-www-form-urlencoded; charset=UTF-8'
        }
        self.cookies   = {
            '__RequestVerificationToken': ''
        }
        self.data      = {
            'page': '1', # max page = response item 3
            'pageSize': '10',
            '__RequestVerificationToken': ''
        }

    def get_token_cookie(self):
        response = requests.get(self.url_token, headers=self.headers)
        self.cookies['__RequestVerificationToken'] = response.cookies.get_dict()['__RequestVerificationToken']
        main = BeautifulSoup(response.text, 'html.parser')
        search_token = main.find('input', {'name': '__RequestVerificationToken'})
        self.data['__RequestVerificationToken'] = search_token.get('value')
    
    def get_data(self, meta:str, market:str, date:str = '', page:str = '1', pageSize:str = '10'):
        url = f'{self.url}{self.endpoint[meta]}'
        if date == '':
            date = datetime.now().strftime('%Y-%m-%d')
        self.data['catID'] = self.catid[market.upper()]
        self.data['date']  = date
        self.data['page']  = page
        self.data['pageSize']  = pageSize
        response = requests.post(url, cookies=self.cookies, headers=self.headers, data=self.data)
        if response.status_code == 200:
            try:
                return {'code': response.status_code, 'data': response.json()}
            except:
                return {'code': response.status_code, 'data': {}}
        else:
            return {'code': response.status_code, 'data': response.text} 

def main(start_date, stop_date, market):
    re_start_date = datetime.strptime(start_date, '%Y-%m-%d')
    conn = Vietstock()
    conn.get_token_cookie()
    content = {}
    if stop_date == "":
        end = datetime.now()
    else:
        end = datetime.strptime(stop_date, '%Y-%m-%d')
    content["DATE"] = {}
    for i in market:
        content[i]  = {}
    content["TOTAL"]  = {}
    index = 0
    re_bypass = []
    for b in bypass:
        re_bypass.append(b[:10])
    while re_start_date < end:
        if re_start_date.weekday() < 5:
            str_re_start_date = re_start_date.strftime('%Y-%m-%d')
            if str_re_start_date not in re_bypass:
                print(f'Start get data in: {str_re_start_date}')
                content["DATE"].update({str(index): str_re_start_date})
                total = 0
                for i in market:
                    data = conn.get_data(meta='thong-ke-gia', market=i, date=str_re_start_date)
                    # print(data)
                    if data['code'] == 200:
                        item = float(data['data'][1][0]['TotalVal']) * 1000000
                        content[i].update({str(index): int(item)})
                        total += int(item)
                    else:
                        content[i].update({str(index): None})
                content["TOTAL"].update({str(index): total})
                print(f'{content["DATE"][str(index)]}, {content["HSX"][str(index)]}, {content["HNX"][str(index)]}, {content["UPX"][str(index)]}, {content["TOTAL"][str(index)]}')
                index += 1
        re_start_date = re_start_date + timedelta(days=1)
    return content

if __name__ == '__main__':
    start_date = '2024-01-01'
    stop_date  = '2024-05-31'
    market = ['HSX', 'HNX', 'UPX']
    result = main(start_date, stop_date, market)
    df = pd.DataFrame(result)
    df.to_csv(f"list_data_vietstock_{int(datetime.now().timestamp())}.csv", index=False)



