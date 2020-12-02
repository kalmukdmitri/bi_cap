import datetime
import mysql.connector as mysql
import pandas as pd
import telebot
def query_df(qry):
    devDB  = {
        'host' :"185.180.163.10",
        'user' : "dima_statist",
        'passwd' : "YandexGoogle",
        'database' : "workface.ru"
    }

    cnx = mysql.connect(**devDB)
    cursor = cnx.cursor()
    cursor.execute(qry)
    resula = [i for i in cursor]
    field_names = [i[0] for i in cursor.description]
    cursor.close()
    cnx.close()
    db_data_df = pd.DataFrame(resula,
                           columns = field_names)
    return db_data_df
def wf_reg_bot():
    r_dt = datetime.datetime.today()
    t_dt = datetime.datetime(r_dt.year,r_dt.month,r_dt.day)
    date1 = t_dt - datetime.datetime(1970, 1, 1) - datetime.timedelta(days=1)
    date2 = t_dt - datetime.datetime(1970, 1, 1)
    date1_s = str(int(date1.total_seconds()))
    date2_s = str(int(date2.total_seconds()))

    query_compaines = f'''
    SELECT 
        phone,
        phone_status,
        create_date 
    FROM `users`
    WHERE create_date > {date1_s}
    and create_date < {date2_s}'''
    new_regs  = query_df(query_compaines)
    nul_phones = []
    good_phones  =[]
    for i in new_regs.itertuples():
        if i.phone_status == 1:
            good_phones.append(i.phone)
        else:
            nul_phones.append(i.phone)

    mess = f'Резулататы по регистрациям за {str(t_dt.date())}\n'
    if len(new_regs) == 0:
        mess+= 'Нет новых регистраций'
    else:
        mess+= 'Новые уcпешные регистрации:\n'
        for i in good_phones:
            mess+= i+'\n'
        mess+= 'Новые неподверждённые регистрации:\n'
        for i in nul_phones:
            mess+= i+'\n'
    chats = [247391252, 482876050]
    bot = telebot.TeleBot("1416074989:AAECtHYON681siUb5S1bzuMHKnLUI-qnb9M")
    for i in chats:
        print(i)
        bot.send_message(i, mess)