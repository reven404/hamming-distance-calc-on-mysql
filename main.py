import mysql.connector
from contextlib import closing
import random
import os
import time
from statistics import mean, median

def getDBConnection() -> mysql.connector.connection.MySQLConnection:
    conn = mysql.connector.connect(
        host = os.getenv("MYSQL_HOST"),
        port = os.getenv("MYSQL_PORT"),
        user = os.getenv("MYSQL_USER"),
        password = os.getenv("MYSQL_PASSWORD"),
        database = os.getenv("MYSQL_DATABASE"),
        buffered = True
    )

    return conn

def randomGenerator(n_max:int, n_chunk:int):
    j = 0
    l = []
    while True:
        for i in range(n_chunk):
            j += 1
            if j > n_max:
                break
            v = random.getrandbits(64)
            l.append((v,))
        if len(l) > 0:
            yield l
        if j > n_max:
            break
        l = []

def init(conn):
    print('begin initialization')
    print('begin creating table')
    with closing(conn.cursor()) as cur:
        cur.execute('DROP TABLE IF EXISTS `phash`')
        conn.commit()
        cur.execute('''
CREATE TABLE `phash` (
`hash` BIT(64) NOT NULL,
`creation_time` datetime DEFAULT CURRENT_TIMESTAMP,
PRIMARY KEY (`hash`)
) ENGINE=InnoDB
        ''')
        conn.commit()
        cur.execute('DELETE FROM `phash`')
        conn.commit()
    print('end creating table')
    print('begin insertion')
    with closing(conn.cursor()) as cur:
        for val in randomGenerator(int(1_000_000), int(10_000)):
            cur.executemany('INSERT INTO `phash` (hash) VALUES (%s)', val)
            conn.commit()
    print('end insertion')
    print('end initialization')

def measure(conn):
    l = []
    with closing(conn.cursor()) as cur:
        for i in range(10):
            start = time.time()
            cur.execute('SELECT SQL_NO_CACHE diff FROM (SELECT BIT_COUNT(9709740330175651432 ^ hash) AS diff FROM phash) naruhodo ORDER BY diff DESC LIMIT 10;')
            elapsed_time = time.time() - start
            l.append(elapsed_time)
            print(f"{elapsed_time}")
            records = cur.fetchall()
    return l

if __name__ == '__main__':
    with closing(getDBConnection()) as conn:
        init(conn)
        print('begin measuring(sec.)')
        elapsed_times = measure(conn)
        print('end measuring')
        print(f'mean: {mean(elapsed_times)} [sec]')
        print(f'med : {median(elapsed_times)} [sec]')
