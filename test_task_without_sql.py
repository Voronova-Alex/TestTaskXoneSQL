import pandas as pd
import csv
import os
import re


def take_data():
    bid = ['b_id', 'client_number', 'play_id', 'amount', 'coefficient']
    event_entity = ['play_id', 'sport_name', 'home_team', 'away_team']
    vent_value = ['play_id', 'attribute', 'value', 'outcome']
    flag = 0
    with open('test_task_sql.sql', "r") as sql_f:
        for line in sql_f:
            if f'INSERT INTO `bid` VALUES' in line:
                info_list = []
                info = line[:-3].split(' ')[-1].split('),(')
                info[0] = info[0][1:]
                info[-1] = info[-1][:-1]
                for i in info:
                    info_list.append(i.split(','))
                with open('bid.csv', 'w', newline='') as csv_f:
                    w = csv.writer(csv_f)
                    w.writerow(bid)
                    w.writerows(info_list)
            elif f'INSERT INTO `event_entity` VALUES' in line:
                if flag == 0:
                    info_list = []
                    info = line[33:-2].split('),(')
                    info[0] = info[0][2:]
                    info[-1] = info[-1][:-1]
                    for i in info:
                        info_list.append(re.split(",(?=')", i))
                    with open('event_entity.csv', 'w', newline='') as csv_f:
                        w = csv.writer(csv_f)
                        w.writerow(event_entity)
                        w.writerows(info_list)
                    flag = 1
                else:
                    info_list = []
                    info = line[33:-2].split('),(')
                    info[0] = info[0][1:]
                    info[-1] = info[-1][:-1]
                    for i in info:
                        info_list.append(re.split(",(?=')", i))
                    with open('event_entity.csv', 'a', newline='') as csv_f:
                        w = csv.writer(csv_f)
                        w.writerows(info_list)
            elif f'INSERT INTO `event_value` VALUES' in line:
                info_list = []
                info = line[:-3].split(' ')[-1].split('),(')
                info[0] = info[0][1:]
                info[-1] = info[-1][:-1]
                for i in info:
                    info_list.append(i.split(','))
                with open('event_value.csv', 'a', newline='') as csv_f:
                    w = csv.writer(csv_f)
                    w.writerow(vent_value)
                    w.writerows(info_list)


def result_sql_1():
    df_event_value = pd.read_csv('event_value.csv')
    df_bid = pd.read_csv('bid.csv')
    df_event_value['play_id'] = df_event_value['play_id'].astype(str)
    df_bid['play_id'] = df_bid['play_id'].astype(str)
    result_1 = pd.DataFrame(
        df_bid.merge(df_event_value, left_on='play_id', right_on='play_id').groupby(['client_number'])['outcome'].agg(
            count='value_counts')).reset_index()
    print(result_1.to_string())


def result_sql_2():
    df_event_entity = pd.read_csv('event_entity.csv')
    new_2 = (df_event_entity['home_team'] + df_event_entity['away_team']).value_counts().to_dict()
    new_3 = (df_event_entity['away_team'] + df_event_entity['home_team']).value_counts().to_dict()
    for key in new_3.keys():
        if key in new_2:
            new_2[key] += new_3[key]
    print(pd.DataFrame(list(new_2.items()), columns=['game', 'game_count']).to_string())


take_data()
print('Необходимо написать запрос, который находит  сколько ставок сыграло и не сыграло у каждого пользователя')
result_sql_1()
print('Необходимо написать запрос, который находит сколько раз между собой играли команды')
result_sql_2()
os.remove('bid.csv')
os.remove('event_value.csv')
os.remove('event_entity.csv')
