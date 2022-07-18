import mysql.connector as m
from mysql.connector import Error
import termtables as tt


def create_connection(host_name, user_name, user_password, db_name):
    con = None
    try:
        con = m.connect(
            host=host_name,
            user=user_name,
            passwd=user_password,
            database=db_name
        )
        print("Connection to MySQL DB successful")
    except Error as e:
        print(f"The error '{e}' occurred")
    return con


def execute_read_query(con, query):
    cursor = con.cursor()
    result = None
    try:
        cursor.execute(query)
        result = cursor.fetchall()
        return result
    except Error as e:
        print(f"The error '{e}' occurred")


connection = create_connection("127.0.0.1", "root", "root", "my_db")

select_1 = """
SELECT bid.client_number, SUM(play.win) AS win, SUM(play.lose) AS lose
FROM bid,
    (
    SELECT event_value.play_id, 
    SUM(event_value.outcome = 'win') AS win,
    SUM(event_value.outcome = 'lose') AS lose
    FROM event_value
    GROUP BY event_value.play_id
    ) AS play
WHERE bid.play_id = play.play_id
GROUP BY bid.client_number
"""
select_2 = """
SELECT * 
FROM (
    SELECT CONCAT_WS("-", home_team, away_team) AS game, COUNT(*) AS game_count 
    FROM event_entity
    GROUP BY home_team, away_team
    ) as b
WHERE b.game not in (SELECT a.game FROM
                                (
                                SELECT x.game AS game, x.game_count+y.game_count AS game_count
                                FROM (
                                    SELECT CONCAT_WS("-", away_team, home_team) AS game, COUNT(*) AS game_count 
                                    FROM event_entity
                                    GROUP BY away_team, home_team
                                    ) AS x,
                                    (
                                    SELECT CONCAT_WS("-", home_team, away_team) AS game, COUNT(*) AS game_count 
                                    FROM event_entity
                                    GROUP BY home_team, away_team
                                    ) AS y
                                WHERE x.game = y.game
                                ) as a
                                )
UNION
SELECT x.game AS game, x.game_count+y.game_count AS game_count
FROM (
    SELECT CONCAT_WS("-", away_team, home_team) AS game, COUNT(*) AS game_count 
    FROM event_entity
    GROUP BY away_team, home_team
    ) AS x,
    (
    SELECT CONCAT_WS("-", home_team, away_team) AS game, COUNT(*) AS game_count 
    FROM event_entity
    GROUP BY home_team, away_team
    ) AS y
WHERE x.game = y.game
"""
result_1 = execute_read_query(connection, select_1)
print('Необходимо написать запрос, который находит  сколько ставок сыграло и не сыграло у каждого пользователя')
tt.print(result_1, header=['client_number', 'Побед', 'Поражений'])

result_2 = execute_read_query(connection, select_2)
print('Необходимо написать запрос, который находит сколько раз между собой играли команды')
tt.print(result_2, header=['game', 'game_count'])
