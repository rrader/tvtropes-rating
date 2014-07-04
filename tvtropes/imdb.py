import concurrent.futures
import csv
import html
import logging
from random import random
import traceback
import sys
import time
import os
import threading
from urllib.parse import urlencode
import sqlite3
import re
from imdbpie import Imdb
import sqlitebck


class MyImdb(Imdb):
    def find_by_title(self, title):
        default_find_by_title_params = {'json': '1',
                                        'nr': 1,
                                        'tt': 'on',
                                        'q': title}
        query_params = urlencode(default_find_by_title_params)
        results = self.get(('http://www.imdb.com/'
                            'xml/find?{0}').format(query_params))

        keys = ['title_exact',
                'title_substring',
                'title_approx',
                'title_popular',
                ]
        title_results = []

        html_unescape = html.unescape

        for key in keys:
            if key in results:
                for r in results[key]:
                    year = None
                    year_match = re.search(r'(\d{4})', r['title_description'])
                    if year_match:
                        year = year_match.group(0)

                    title_match = {
                        'title': html_unescape(r['title']),
                        'year': year,
                        'imdb_id': r['id']
                    }
                    title_results.append(title_match)

        return title_results


imdb_db_connections = {}


def load_to_memory(conn):
    memorydb = sqlite3.connect(":memory:", timeout=60, check_same_thread=False)
    sqlitebck.copy(conn, memorydb)
    return memorydb


def get_imdb_connection(in_memory=True):
    """in_memory is faster but requires a lot of memory"""
    ident = threading.get_ident()
    if ident not in imdb_db_connections:
        logging.debug("Creating connection to IMDB for thread #{} [{}]".format(ident, threading.get_ident()))
        conn = sqlite3.connect('./3rd/sqlitedb/moviedb.sqlite')
        if in_memory:
            imdb_db_connections[ident] = load_to_memory(conn)
            conn.close()
        else:
            imdb_db_connections[ident] = conn

    return imdb_db_connections[ident]

rating_db_connections = {}
RATING_DB_PATH = './gen/rating.sqlite'
RATING_DB_SCHEMA_PATH = './tvtropesdb/rating.sql'
year_regex   = re.compile(".*(\d\d\d\d).*", re.VERBOSE)


def get_rating_connection():
    ident = threading.get_ident()
    if ident not in rating_db_connections:
        rating_db_connections[ident] = sqlite3.connect(RATING_DB_PATH, timeout=10)  # RATING_DB_PATH
        logging.debug('CONNECTION OPENED')
    return rating_db_connections[ident]


tvt_db_connections = {}


def get_tropes_connection():
    ident = threading.get_ident()
    if ident not in tvt_db_connections:
        tvt_db_connections[ident] = sqlite3.connect('./gen/tropes.sqlite', timeout=10)
    return tvt_db_connections[ident]


def get_offline_rating(name, years, aka=True):
    # TODO: rewrite this spaghetti
    c = get_imdb_connection().cursor()
    rating = []
    sql_s = "SELECT * FROM productions WHERE %s AND rating IS NOT NULL"
    like_clause = "title LIKE ?"
    sql_no_year = sql_s % like_clause
    year_clause_generic = "(%%s IN (%s) OR %%s IS NULL)" % (','.join(years))
    year_clause = " AND " + year_clause_generic % ("year", "year")
    sql_with_year = sql_no_year + year_clause

    aka_like_clause = 'aka_title LIKE ?'
    sql_aka_generic = "SELECT * FROM aka_titles WHERE %s"
    sql_aka = sql_aka_generic % aka_like_clause
    sql_aka_g = None
    original_name = None
    if aka:
        aka_year_clause = " AND (" + year_clause_generic % ("year", "year") + " OR " + year_clause_generic % ("aka_year", "aka_year") + ")"
        if years:
            sql_aka_g = sql_aka + aka_year_clause
            c.execute(sql_aka_g, [name])
            try:
                original_name = c.fetchone()[2]  # get original title
            except TypeError:
                pass
        if not original_name:
            sql_aka_g = sql_aka
            c.execute(sql_aka_g, [name])
            try:
                original_name = c.fetchone()[2]  # get original title
            except TypeError:
                pass
        if not original_name:
            sql_aka_g = sql_aka + aka_year_clause
            c.execute(sql_aka_g, ['%' + name + '%'])
            try:
                original_name = c.fetchone()[2]  # get original title
            except TypeError:
                pass
        if not original_name:
            sql_aka_g = sql_aka
            c.execute(sql_aka_g, ['%' + name + '%'])
            try:
                original_name = c.fetchone()[2]  # get original title
            except TypeError:
                pass
        if not original_name:
            parts = [('%%%s%%' % part) for part in re.split(r' |:|,|-', name) if len(part) > 0]
            if len(parts):
                where = ' AND '.join([aka_like_clause for _ in parts])
                sql_aka_g = sql_aka_generic % where
                c.execute(sql_aka_g, parts)
                try:
                    original_name = c.fetchone()[2]  # get original title
                except TypeError:
                    pass
        if not original_name:
            parts = [('%%%s%%' % part) for part in re.split(r' |:|,|-', name) if len(part) > 0]
            if len(parts):
                where = ' AND '.join([aka_like_clause for _ in parts])
                sql_aka_g = sql_aka_generic % where + aka_year_clause
                c.execute(sql_aka_g, parts)
                try:
                    original_name = c.fetchone()[2]  # get original title
                except TypeError:
                    pass

        if original_name:
            logging.debug("{}, [{}]".format(sql_aka_g, [name]))
            original_rating, found_name, found_years = get_offline_rating(original_name, years, aka=False)
            if original_rating:
                return original_rating, found_name, found_years

    records = []
    sql = None
    if years:
        sql = sql_with_year
        parts = [name]
        c.execute(sql, parts)
        records = c.fetchall()
    if not records:
        sql = sql_no_year
        parts = [name]
        c.execute(sql, parts)
        records = c.fetchall()
    if not records:
        sql = sql_with_year
        parts = ['%' + name + '%']
        c.execute(sql, parts)
        records = c.fetchall()
    if not records:
        sql = sql_no_year
        parts = ['%' + name + '%']
        c.execute(sql, parts)
        records = c.fetchall()
    if not records:
        parts = [('%%%s%%' % part) for part in re.split(r' |:|,|-', name) if len(part) > 0]
        if len(parts):
            where = ' AND '.join([like_clause for _ in parts])
            sql = (sql_s % where) + year_clause
            c.execute(sql, parts)
            records = c.fetchall()
    if not records:
        parts = [('%%%s%%' % part) for part in name.split(' ') if len(part) > 0]
        if len(parts):
            where = ' AND '.join([like_clause for _ in parts])
            sql = (sql_s % where) #  + year_clause
            print(sql, parts)
            c.execute(sql, parts)
            records = c.fetchall()
    if not records:
        parts = [('%%%s%%' % part) for part in name.split(' ') if len(part) > 3]
        if len(parts):
            where = ' AND '.join([like_clause for _ in parts])
            sql = (sql_s % where) + year_clause
            c.execute(sql, parts)
            records = c.fetchall()
    if not records:
        parts = [('%%%s%%' % part) for part in name.split(' ') if len(part) > 3]
        if len(parts):
            where = ' AND '.join([like_clause for _ in parts])
            sql = (sql_s % where) #  + year_clause
            c.execute(sql, parts)
            records = c.fetchall()
    # try without year
    if not records:
        years_in_title = year_regex.search(name)
        if years_in_title:
            years = list(set(years).union(set(years_in_title.groups())))
            name_n = year_regex.sub('', name).strip()
            original_rating, found_name, found_years = get_offline_rating(name_n, years, aka=True)
            if original_rating:
                return original_rating, found_name, found_years
#
    if not records and len(name.split(':')) > 1:
        parts = [('%%%s%%' % part) for part in name.split(':')[1].split(' ') if len(part) > 3]
        if len(parts):
            where = ' AND '.join([like_clause for _ in parts])
            sql = (sql_s % where) + year_clause
            c.execute(sql, parts)
            records = c.fetchall()
    if not records and len(name.split("'s")) > 1:
        parts = [('%%%s%%' % part) for part in name.split("'s")[1].strip().split(' ') if len(part) > 3]
        if len(parts):
            where = ' AND '.join([like_clause for _ in parts])
            sql = (sql_s % where) + year_clause
            c.execute(sql, parts)
            records = c.fetchall()
    if not records and len(name.split("'s")) > 1:
        parts = [('%%%s%%' % part) for part in name.split("'s")[1].strip().split(' ') if len(part) > 3]
        if len(parts):
            where = ' AND '.join([like_clause for _ in parts])
            sql = (sql_s % where)
            c.execute(sql, parts)
            records = c.fetchall()

    logging.debug("{}, [{}]".format(sql, parts))

    found_years = []
    for record in records:
        if record is not None and record[13] is not None:
            rating.append(record[13])
            found_years.append(str(record[6]))
    if not rating:
        return None, None, None
    # TODO: take into account count of votes (weighted mean)
    return sum(rating) / len(rating), name, ','.join(found_years)


def init_db():
    try:
        os.remove(RATING_DB_PATH)
    except:
        pass
    db = get_rating_connection()
    cursor = db.cursor()
    cursor.executescript(open(RATING_DB_SCHEMA_PATH).read())


class IMDBSpider(object):
    def prepare(self):
        # self.csvfile_dst = open("gen/films_imdb.csv", "w", newline='')
        self.csvfile_dst_notfound = open("gen/films_no_imdb.csv", "w", newline='')

        # self.writer = csv.writer(self.csvfile_dst)
        self.writer_no_imdb = csv.writer(self.csvfile_dst_notfound)
        self.imdb = None
        self.processed = 0

        get_imdb_connection()
        get_tropes_connection()
        init_db()

    def shutdown(self):
        # self.csvfile_dst.close()
        get_rating_connection().commit()
        get_rating_connection().close()
        self.csvfile_dst_notfound.close()

    def task_generator(self):
        c = get_tropes_connection().cursor()
        c.execute('SELECT * FROM films')

        for row in c:
            yield {'name': 'film', 'title': row[1], 'years': row[3].split(',')}

    def task_film(self, task, tries=1):
        logging.debug("Processing {}...".format(task['title']))
        if self.imdb == None:
            self.imdb = MyImdb()

        try:
            # film = self.imdb.find_by_title(task['title'])[0]
            # real_title = film["title"]
            real_title = task['title']
            movie_rating, found_name, found_years = get_offline_rating(real_title, task['years'])
            if movie_rating:
                self.save_film(title=task['title'], imdb_title=found_name,
                               rating=movie_rating, years=found_years)
                logging.info("{}: [{}] {}".format(task['title'], movie_rating, real_title))
            else:
                self.writer_no_imdb.writerow([
                    task['title']
                ])
                logging.debug("No {} in IMDB".format(task['title']))
        except IndexError:
            self.writer_no_imdb.writerow([
                task['title']
            ])
            logging.debug("No {} in IMDB".format(task['title']))
        except Exception as ex:
            logging.error("Some error: {}".format(ex))
            traceback.print_exc(file=sys.stdout)
            raise ex
        # self.csvfile_dst.flush()
        self.processed += 1
        get_rating_connection().commit()
        self.csvfile_dst_notfound.flush()

    def save_film(self, title, imdb_title, rating, years):
        retry = False
        while True:
            try:
                c = get_rating_connection().cursor()
                c.execute("""INSERT INTO films(
                            title,
                            imdb_title,
                            rating,
                            years)
                          VALUES (?, ?, ?, ?)
                          """, [title, imdb_title, rating, years])
                c.close()
            except sqlite3.OperationalError as ex:
                logging.debug('retry... {}'.format(ex))
                retry = True
            else:
                if retry:
                    logging.debug('DB unlocked...')
                    time.sleep(random())
                break


    def run(self, parallel=True):
        self.prepare()

        gen = self.task_generator()

        if parallel:
            with concurrent.futures.ThreadPoolExecutor(max_workers=8) as executor:
                for row in gen:
                    method = getattr(self, 'task_{}'.format(row['name']))
                    executor.submit(method, row)
                executor.shutdown(wait=True)
        else:
            for row in gen:
                method = getattr(self, 'task_{}'.format(row['name']))
                method(row)

        self.shutdown()


if __name__ == "__main__":
    logging.basicConfig(level=logging.DEBUG)
    bot = IMDBSpider()
    bot.run(parallel=True)
