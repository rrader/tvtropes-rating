import threading
import time
import os
import sqlite3
import sqlitebck
import re
from grab.spider import Spider, Task
import redis
import logging

year_regex = re.compile(".*\((\d\d\d\d)\).*", re.VERBOSE)
imdb_db_connections = {}
TVTROPES_DB_PATH = './gen/tropes.sqlite'
TVTROPES_DB_SCHEMA_PATH = './tvtropesdb/tropes.sql'


def get_db_connection():
    # ident = threading.get_ident()
    ident = 0  #
    if ident not in imdb_db_connections:
        logging.debug("Creating connection for thread #{} [{}]".format(ident, threading.get_ident()))
        imdb_db_connections[ident] = sqlite3.connect(":memory:", timeout=60, check_same_thread=False)  # TVTROPES_DB_PATH
    return imdb_db_connections[ident]


def init_db():
    try:
        os.remove(TVTROPES_DB_PATH)
    except:
        pass
    db = get_db_connection()
    cursor = db.cursor()
    cursor.executescript(open(TVTROPES_DB_SCHEMA_PATH).read())


def save_db():
    logging.debug("Saving memory db to disk... thread #{}".format(threading.get_ident()))
    disk_db = sqlite3.connect(TVTROPES_DB_PATH, timeout=60, check_same_thread=False)
    sqlitebck.copy(get_db_connection(), disk_db)
    disk_db.close()
    logging.debug("DB Saved!")


class TVTropesSpider(Spider):
    ns = 'Film'
    initial_urls = ['http://tvtropes.org/pmwiki/pmwiki.php/' + ns]

    def prepare(self):
        super().prepare()
        init_db()
        self.redis = redis.Redis(db=1)

    def shutdown(self):
        super().shutdown()
        get_db_connection().commit()
        save_db()
        get_db_connection().close()

    def task_initial(self, grab, task):
        sections = grab.doc.select('//div[@id="wikitext"]/ul[1]/li/a[1]')
        for section in sections:
            yield Task("section", s_name=section.text(), url=section.attr('href'), level=1)
        # yield Task("film", f_name="Return of the Jedi", url="http://tvtropes.org/pmwiki/pmwiki.php/Film/ReturnOfTheJedi")

    def task_section(self, grab, task):
        if self.redis.getset(task.url, "1") is not None:
            return
        logging.debug("Section " + task.url)
        films = grab.doc.select('//div[@id="wikitext"]//li//a[1]')
        for film in films:
            if film.attr('href').split('/')[-2] == self.ns:  # Film
                yield Task("film", f_name=film.text(), url=film.attr('href'))
            elif film.attr('href').split('/')[-2] == "Main":  # subsection
                if task.level < 2:
                    logging.debug("subsection " + film.attr('href') + ' ' + task.url)
                    yield Task("section", s_name=film.text(), url=film.attr('href'), level=task.level+1)

    def task_film(self, grab, task):
        if self.redis.getset(task.url, "1") is not None:
            return
        logging.info("Film " + task.url)
        years_sel = grab.doc.select('//div[@id="wikitext"]').rex('(\d\d\d\d)')
        years = set(y.group() for y in years_sel.items)
        years_in_title = year_regex.search(task.f_name)
        if years_in_title:
            years = years.union(set(years_in_title.groups()))

        title = grab.doc.select('//div[@class="pagetitle"]/span').text().strip()
        title = year_regex.sub('', title).strip()

        tropes = grab.doc.select('//div[@id="wikitext"]//li//a[1]')
        found_tropes = []
        for trope in tropes:
            if trope.attr('href').split('/')[-2] == "Main":
                found_tropes.append( (trope.attr('href').split('/')[-1], trope.attr('href')) )
        self.save_tvtrope(title,
                          task.url,
                          ','.join(years),
                          found_tropes)

    @staticmethod
    def save_tvtrope(title, url, years, tropes):
        c = get_db_connection().cursor()
        c.execute("""INSERT OR IGNORE INTO films(title, url, years) VALUES (?, ?, ?)""",
                  [title, url, years])
        film_id = c.lastrowid
        c.executemany("""INSERT OR IGNORE INTO tropes(trope, url) VALUES (?, ?)""",
                      tropes)
        c.executemany("""INSERT OR IGNORE INTO film_tropes(film, trope)
                         VALUES
                         ( ?,
                           (SELECT id FROM tropes WHERE trope = ?)
                         )
                      """,
                      [(film_id, trope[0]) for trope in tropes])
        c.close()
        get_db_connection().commit()


if __name__ == "__main__":
    logging.basicConfig(level=logging.DEBUG)
    bot = TVTropesSpider(thread_number=15)
    bot.setup_queue(backend="mongo", database="tvtropes-grab")
    bot.run()
