import concurrent.futures
import csv
import html
import logging
import threading
from urllib.parse import urlencode
import re
import sqlite3
import re
from imdbpie import Imdb


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


sqlite3_connections = {}


def get_connection():
    ident = threading.get_ident()
    if ident not in sqlite3_connections:
        sqlite3_connections[ident] = sqlite3.connect('./3rd/sqlitedb/moviedb.sqlite')
    return sqlite3_connections[ident]


def get_offline_rating(name, years, aka=False):
    c = get_connection().cursor()
    rating = []
    sql_s = "SELECT * FROM productions WHERE %s AND rating IS NOT NULL"
    like_clause = "title LIKE ?"
    sql_no_year = sql_s % like_clause
    year_clause_generic = "(%%s IN (%s) OR %%s IS NULL)" % (','.join(years))
    year_clause = " AND " + year_clause_generic % ("year", "year")
    sql_with_year = sql_no_year + year_clause

    sql_aka = "SELECT * FROM aka_titles WHERE aka_title LIKE ?"
    if years:
        sql_aka_year = sql_aka + " AND (" + year_clause_generic % ("year", "year") + " OR " + year_clause_generic % ("aka_year", "aka_year") + ")"
    else:
        sql_aka_year = sql_aka

    logging.debug("{}, [{}]".format(sql_aka_year, [name]))
    c.execute(sql_aka_year, [name])
    try:
        name = c.fetchone()[2]  # get original title
    except TypeError:
        pass

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
        where = ' AND '.join([like_clause for _ in parts])
        sql = (sql_s % where) + year_clause
        c.execute(sql, parts)
        records = c.fetchall()
    if not records:
        parts = [('%%%s%%' % part) for part in name.split(' ') if len(part) > 0]
        where = ' AND '.join([like_clause for _ in parts])
        sql = (sql_s % where) #  + year_clause
        c.execute(sql, parts)
        records = c.fetchall()
    if not records:
        parts = [('%%%s%%' % part) for part in name.split(' ') if len(part) > 3]
        where = ' AND '.join([like_clause for _ in parts])
        sql = (sql_s % where) + year_clause
        c.execute(sql, parts)
        records = c.fetchall()
    if not records:
        parts = [('%%%s%%' % part) for part in name.split(' ') if len(part) > 3]
        where = ' AND '.join([like_clause for _ in parts])
        sql = (sql_s % where) #  + year_clause
        c.execute(sql, parts)
        records = c.fetchall()
#
    if not records and len(name.split(':')) > 1:
        parts = [('%%%s%%' % part) for part in name.split(':')[1].split(' ') if len(part) > 3]
        where = ' AND '.join([like_clause for _ in parts])
        sql = (sql_s % where) + year_clause
        c.execute(sql, parts)
        records = c.fetchall()
    if not records and len(name.split("'s")) > 1:
        parts = [('%%%s%%' % part) for part in name.split("'s")[1].strip().split(' ') if len(part) > 3]
        where = ' AND '.join([like_clause for _ in parts])
        sql = (sql_s % where) + year_clause
        c.execute(sql, parts)
        records = c.fetchall()
    if not records and len(name.split("'s")) > 1:
        parts = [('%%%s%%' % part) for part in name.split("'s")[1].strip().split(' ') if len(part) > 3]
        where = ' AND '.join([like_clause for _ in parts])
        sql = (sql_s % where)
        c.execute(sql, parts)
        records = c.fetchall()

    logging.debug("{}, [{}]".format(sql, parts))

    for record in records:
        if record is not None and record[13] is not None:
            rating.append(record[13])
    if not rating:
        return
    return sum(rating) / len(rating)


class IMDBSpider(object):
    def prepare(self):
        self.csvfile_dst = open("gen/films_imdb.csv", "w", newline='')
        self.csvfile_dst_notfound = open("gen/films_no_imdb.csv", "w", newline='')

        self.writer = csv.writer(self.csvfile_dst)
        self.writer_no_imdb = csv.writer(self.csvfile_dst_notfound)
        self.imdb = None

    def shutdown(self):
        self.csvfile_dst.close()
        self.csvfile_dst_notfound.close()

    def task_generator(self):
        csvfile_src = open("gen/films.csv", newline='')
        reader = csv.reader(csvfile_src)
        for row in reader:
            yield {'name': 'film', 'title': row[0], 'years': row[1].split(',')}

    def task_film(self, task, tries=1):
        logging.debug("Processing {}...".format(task['title']))
        if self.imdb == None:
            self.imdb = MyImdb()

        try:
            # film = self.imdb.find_by_title(task['title'])[0]
            # real_title = film["title"]
            real_title = task['title']
            movie_rating = get_offline_rating(real_title, task['years'])
            if movie_rating:
                self.writer.writerow([
                    task['title'],
                    movie_rating
                ])
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
        self.csvfile_dst.flush()
        self.csvfile_dst_notfound.flush()

    def run(self):
        self.prepare()

        gen = self.task_generator()

        with concurrent.futures.ThreadPoolExecutor(max_workers=5) as executor:
            for row in gen:
                method = getattr(self, 'task_{}'.format(row['name']))
                executor.submit(method, row)
            executor.shutdown(wait=True)

        self.shutdown()


if __name__ == "__main__":
    logging.basicConfig(level=logging.DEBUG)
    bot = IMDBSpider()
    bot.run()
