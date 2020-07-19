import pandas as pd
from google.oauth2 import service_account

# Прописываем адрес к файлу с данными по сервисному аккаунту и получаем credentials для доступа к данным
credentials = service_account.Credentials.from_service_account_file(
    'qstn-xxxxx-xxxxxxxxxxxxxx.json')

# Формируем запрос и получаем количество вопросов с тегом "pandas", сгруппированные по дате создания
query = '''
SELECT event_name, event_params
FROM `qstn-800bc.analytics_231248605.0712`
WHERE event_name="select_item" LIMIT 9000;
'''

# Тут немного подсказок насчет пандас: https://khashtamov.com/ru/pandas-introduction/

# Указываем идентификатор проекта
project_id = 'qstn-800bc'

# Выполняем запрос с помощью функции ((https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.read_gbq.html read_gbq)) в pandas, записывая результат в dataframe
df = pd.read_gbq(query, project_id=project_id, credentials=credentials)

# display(df.head(5))
print(df)
# print(df['select_item'])
print(df.columns)
print(df.event_params)

data_set = pd.DataFrame(df.event_params)

print(data_set.columns)
print(data_set)
# print(df.head())       # первые строки
# print(df.tail())       # последние строки
# print(df.sample(5))    # случайно выбранное кол-ва строк, полезно использовать для уменьшения матрицы для прогонки тестов
# print(df.shape)        # по аналогии с numpy - размерность матрицы
# print(df.describe())   # математические данные
# print(df.info())       # использование памяти

x = df.event_params


print(x[11])
print('----------------')
print(x[11][10])
print(x[11][11])
print('----------------')
print(x[11][10]['value'])
print(x[11][11]['value'])
print('----------------')
qstn_id = x[11][10]['value']
user_answer = x[11][11]['value']

print(qstn_id["string_value"], user_answer["string_value"])


# 1296 строк

i = 0
answers_array = []



while i < 545:
    qstn_id = x[i][10]['value']
    user_answer = x[i][11]['value']
    print(qstn_id["string_value"], user_answer["string_value"])
    f = open("qstnid12072020.txt", "a")
    f.write(qstn_id["string_value"] + '\n')
    f.close()
    f = open("answers12072020.txt", "a")
    f.write(user_answer["string_value"] + '\n')
    f.close()
    i = i + 1

