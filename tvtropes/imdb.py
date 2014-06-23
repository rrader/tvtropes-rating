import concurrent.futures
import csv
import html
import logging
from threading import Thread
import threading
from urllib.parse import urlencode
import time
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


def get_offline_rating(name, years):
    c = get_connection().cursor()
    rating = []
    sql_no_year = "SELECT * FROM productions WHERE title LIKE ? AND rating IS NOT NULL"
    sql_with_year = sql_no_year + " AND (year IN (%s) OR year IS NULL)" % (','.join(years))
    records = []
    if years:
        c.execute(sql_with_year, [name])
        records = c.fetchall()
    if not records:
        c.execute(sql_no_year, [name])
        records = c.fetchall()

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
            self.writer.writerow([
                task['title'],
                movie_rating
            ])
            logging.info("{}: [{}] {}".format(task['title'], movie_rating, real_title))
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
