import requests

def call_hole():
    chats = [247391252,482876050]
    token = "1461276547:AAECMSMOMW1Zah3IEXeAyGAsBVJD0ktM86E"
    method = "sendMessage"
    
    url = "https://api.telegram.org/bot{token}/{method}".format(token=token, method=method)
    for i in chats:
        data = {"chat_id": i, "text": 'Нет пропавших контактов'}
        requests.post(url, data=data)
if __name__ == "__main__":
    call_hole()