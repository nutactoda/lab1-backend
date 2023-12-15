import sqlite3
import pandas as pd

# создаем базу данных и устанавливаем соединение с ней
con = sqlite3.connect("store.sqlite")

# открываем файл с дампом базой двнных
f_damp = open('store.db', 'r', encoding='utf-8-sig')

# читаем данные из файла
damp = f_damp.read()

# закрываем файл с дампом
f_damp.close()

# запускаем запросы
con.executescript(damp)

# сохраняем информацию в базе данных
con.commit()

# задание 1
df = pd.read_sql('''
    SELECT
        title AS Название,
        name_author AS Автор,
        name_genre AS Жанр
    FROM book
        JOIN author USING (author_id)
        JOIN genre USING (genre_id)
    WHERE (price < :min_price OR price > :max_price) AND amount > :num
    ORDER BY title ASC, name_author ASC
''', con, params={"min_price": 200, "max_price": 400, "num": 4})
print('задание 1')
print(df, '\n')

# задание 2
df = pd.read_sql('''
    SELECT
        name_city AS Город,
        CASE WHEN SUM(amount) > 0
            THEN SUM(amount) else '0'
        END AS Количество
    FROM buy_book
        JOIN buy USING (buy_id)
        JOIN client USING (client_id)
        RIGHT JOIN city USING (city_id)
    GROUP BY name_city
    ORDER BY SUM(amount) DESC, name_city ASC
''', con)
print('задание 2')
print(df, '\n')

# задание 3
df = pd.read_sql('''
    WITH get_orders(c_id, sum_order)
    AS(
        SELECT client_id, price * buy_book.amount
        FROM buy_book
            JOIN book USING (book_id)
            JOIN buy USING (buy_id)
    ),
    get_max(max_count)
    AS (
       SELECT MAX(sum_order)
       FROM get_orders
    ),
    get_id(cl_id)
    AS (
        SELECT c_id
        FROM 
            get_orders
            JOIN get_max ON max_count = sum_order
    )
    SELECT
        title AS Название,
        name_author AS Автор
    FROM buy_book
        JOIN buy USING (buy_id)
        JOIN book USING (book_id)
        JOIN author USING (author_id)
        JOIN get_id ON client_id = cl_id
''', con)
print('задание 3')
print(df, '\n')

# задание 4
con.execute('''
    UPDATE book
    SET price = CASE
        WHEN (SELECT SUM(buy_book.amount) FROM buy_book WHERE book_id = book.book_id) > (SELECT AVG(buy_book.amount) FROM buy_book)
            THEN price * 1.1 ELSE price * 0.95
        END;
''')
df = pd.read_sql(' select * from book ', con)
print('задание 4')
print(df, '\n')

# задание 5
df = pd.read_sql('''
    SELECT
        ROW_NUMBER() OVER win_report AS "№пп",
        name_author AS Автор,
        CASE
            WHEN LENGTH(title) > 15 THEN SUBSTR(title, 1, 12) || '...'
            ELSE title
        END AS Книга,
        amount AS "Кол-во",
        RANK() OVER win_report AS Ранг,
        ROUND((CUME_DIST() OVER win_report), 2) AS Распределение,
        ROUND((PERCENT_RANK() OVER win_report) * 100, 2) AS "Ранг,%"
    FROM book
        JOIN author USING (author_id)
    WINDOW win_report
    AS(
        ORDER BY amount
    );
''', con)

print('задание 5')
print(df, '\n')

# закрываем соединение с базой
con.close()

