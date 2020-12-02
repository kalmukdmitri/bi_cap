import telebot
import json
import requests
import datetime
import pandas
from pandas import DataFrame as df
from pd_gbq import *
import time 
from googleapiclient.discovery import build
from oauth2client.service_account import ServiceAccountCredentials
from doc_token import get_tokens


class callibri():
    base = 'https://api.callibri.ru/'
    def __init__(self, token, 
                user_email = 'user_email=kalmukdmitri@gmail.com',
                 site_id = 'site_id=37222'):
        self.token = token
        self.user_email = user_email
        self.site_id= site_id
    def get_stats(self, date1,date2):
        date1= date1.strftime("%d.%m.%Y")
        date2 = date2.strftime("%d.%m.%Y")
        request_url  = f"{callibri.base}site_get_statistics?{self.token}&{self.user_email}&{self.site_id}&date1={date1}&date2={date2}"
        print(request_url)
        answer = requests.get(request_url)
        results = json.loads(answer.text)
        return results
    


def get_old_token(docname):
    """Intup: None
    Output: Old token"""

    googe_request = service.documents().get(documentId = docname).execute()
    token_str=googe_request['body']['content'][1]['paragraph']['elements'][0]['textRun']['content']
    doc_lenth = len(token_str)
    token = json.loads(token_str.strip().replace("'", '"'))
    return token,doc_lenth

def write_new_token(docname, token, doc_lenth):
    requests = [
        {'deleteContentRange': {
            'range' : {
                "startIndex": 1,
    "endIndex": doc_lenth
            }
        }},
        {'insertText': {
                'location': {
                    'index': 1,
                },
                'text': str(token)
            }
        }
    ]
    result = service.documents().batchUpdate(
        documentId=docname, body={'requests': requests}).execute()


def get_new_token(docname):
    """Intup: None
    Process: write new token instead of old one
    Output: New token """

    old_token,doc_lenth = get_old_token(docname)
    url = 'https://officeicapru.amocrm.ru/oauth2/access_token'
    data = json.dumps({
    "client_id": "e8b09b6a-3f20-43fa-9e63-7e045cb5dbeb",
    "client_secret": "IO61BEABH48e5VcQzL7ivtcCGSKg6NXnv8zRVumsiC5EbGQegV9ox0e5CJPVjMop",
    "grant_type": "refresh_token",
    'redirect_uri':"https://officeicapru.amocrm.ru/",
    "refresh_token": old_token['refresh_token']
                    })


    token = json.loads(requests.post(url, headers = {"Content-Type":"application/json"},data=data).text)
    write_new_token(docname,token,doc_lenth)

    return token


class get_AMO:
    m_url = "https://officeicapru.amocrm.ru/api/v2/"
    def __init__(self, token):
        self.headers = {
        'Authorization': f"Bearer {token}",
        "Content-Type":"application/json"
        }
    def get_data(self, prm):
        url = f"{get_AMO.m_url}{prm}"
        print(url)
        reqv = requests.get(url, headers = self.headers)       
        return json.loads(reqv.text)

    def get_big_amo(self,params):
        i = True
        c = -1
        res = []
        while i:
            c+=1
            offset = c * 500
            params_url = f'{params}?limit_rows=500&limit_offset={offset}'
            result = self.get_data(params_url)
            if '_embedded' not in result:
                return res
            else:
                result = result['_embedded']['items']
            res.extend(result)
            len_res= len(result)
            if c == 100 or len_res < 500: 
                i = False
        return res
def call_hole():
    passwords = get_tokens()
    callibri_connect = callibri(token= passwords['callibri'])
    date2 = datetime.datetime.today().date()  - datetime.timedelta(days=1)
    date1  = date2 - datetime.timedelta(days=5)
    callibri_data = callibri_connect.get_stats(date1, date2)
    cal_ph = {i['phone']:(i['date'][:10]+' '+i['date'][11:19]) for i in callibri_data['channels_statistics'][0]['calls']}

    SCOPES = ['https://www.googleapis.com/auth/drive',
            'https://www.googleapis.com/auth/documents']

    credentials = ServiceAccountCredentials.from_json_keyfile_name('kalmuktech-5b35a5c2c8ec.json', SCOPES)
    service = build('docs', 'v1', credentials=credentials)

    current_token = get_new_token("1V1gX11RDYJf4ZVFCEOqp-kY5j6weApl_oEFkv2oZzW4")
    amo_connect = get_AMO(current_token['access_token'])
    r_dt = datetime.datetime.today()
    t_dt = datetime.datetime(r_dt.year,r_dt.month,r_dt.day)
    date1 = t_dt - datetime.datetime(1970, 1, 1) - datetime.timedelta(days=6)
    date2 = t_dt - datetime.datetime(1970, 1, 1) - datetime.timedelta(days=1)
    date1_s = str(int(date1.total_seconds()))
    date2_s = str(int(date2.total_seconds()))
    fresj_cnts = amo_connect.get_big_amo('contacts')

    import string
    def get_custom_phone(cstms , fld = 'Телефон'):
        for i in cstms:
            if 'name' in i and i['name'] == fld:
                phn  = ''
                for j in i['values'][0]['value']:
                    if j in string.digits:
                        phn += j
                return phn
    cnt_map = {get_custom_phone(i['custom_fields']) : i['id'] for i in fresj_cnts}
    matches = []
    for i in cal_ph:
        if i in cnt_map:
            pass
        else:
            matches.append(i)

    matches = []

    for i in cal_ph:
        if i in cnt_map:
            pass
        else:
            matches.append(i)

    losts = {}
    for i in matches:
        losts[i] = cal_ph[i]
    if len(matches) > 0:
        message = 'Контакты не попавшие в амо:\n'
        for i,e in losts.items():
            message += f'{i} созданый {e}\n'
    else:
        message = 'Нет пропавших контактов'
    chats = [247391252,482876050]
    bot = telebot.TeleBot("1461276547:AAECMSMOMW1Zah3IEXeAyGAsBVJD0ktM86E")
    for i in chats:
        print(i)
        bot.send_message(i, message)