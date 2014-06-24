from __future__ import with_statement
from sqlite3 import dbapi2 as sqlite3
import regexes
import os
import re
import shutil
import urllib2
import gzip
import timer


def connect_db():
    """Returns a new connection to the database."""
    return sqlite3.connect('sqlitedb/moviedb.sqlite')


def get_title_identifier(title, year):
    return re.sub(r"[^a-zA-Z\d]+", '-', title + ' ' + year).lower().strip('-')


def download_files():
    dir = 'dumps'
    urls = (
        'ftp://ftp.fu-berlin.de/pub/misc/movies/database/movies.list.gz',
        'ftp://ftp.fu-berlin.de/pub/misc/movies/database/directors.list.gz',
        'ftp://ftp.fu-berlin.de/pub/misc/movies/database/ratings.list.gz',
        )
    for url in urls:
        zipfilename = dir + '/' + url.split('/')[-1]
        diskfile = open(zipfilename, 'w')
        webfile = urllib2.urlopen(url)
        shutil.copyfileobj(webfile, diskfile)
        gzip.open(zipfilename, 'r')


def do_import():
    t = timer.Timer()

    # wipe out existing cb
    try:
        os.remove('sqlitedb/moviedb.sqlite')
    except:
        pass

    # create db schema
    f = open('sqlitedb/schema.sql')
    db = connect_db()
    cursor = db.cursor()
    cursor.executescript(f.read())

    # debugging - write unusable lines to a file
    badlines_movies    = open('sqlitedb/badlines_movies.txt',    'w')
    badlines_ratings   = open('sqlitedb/badlines_ratings.txt',   'w')
    badlines_directors = open('sqlitedb/badlines_directors.txt', 'w')
    badlines_aka_titles = open('sqlitedb/badlines_aka_titles.txt', 'w')

    limit = 10000000
    commit_every = 10000
    # cache we'll be using for every file import; title -> database id
    production_ids = {}

    print "Importing movies..."
    # import movies
    i = 0
    for line in open('dumps/movies.list', 'r'):
        i += 1

        if 0 == i % 100000:
            print i, 'Movies'

        if i >= limit:
            break

        line = line
        regex = regexes.productions_regex
        m = re.match(regex, line.decode('iso8859', 'replace'))
        if (not m):
            badlines_movies.write(line)
            continue

        unique_title     = m.group('unique_title')
        production_title = m.group('production_title')
        tv_show_title    = m.group('tv_show_title')
        episode_title    = m.group('episode_title')
        season_number    = m.group('season_number')
        episode_number   = m.group('episode_number')
        year             = m.group('year')
        year_number      = m.group('year_number')
        media_type       = m.group('media_type')
        is_tv_show_episode = True if m.group('season_number') and m.group('episode_number') else False
        is_tv_show_primary = True if not is_tv_show_episode and unique_title[0] == '"' else False
        actual_title = episode_title if is_tv_show_episode else production_title
        try:
            slug_title = production_title + '-' + episode_title if is_tv_show_episode else production_title
        except TypeError:
            pass
        slug = get_title_identifier(slug_title, year)

        # if this is a tv episode, look up the id of that show
        id_parent = None
        if (is_tv_show_episode and tv_show_title in production_ids):
            id_parent = production_ids[tv_show_title]

        try:
            cursor.execute("""INSERT INTO productions(
                title,
                year,
                year_number,
                media_type,
                is_tv_show_primary,
                is_tv_show_episode,
                slug,
                season,
                number,
                id_parent
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
            [
                actual_title,
                year,
                year_number,
                media_type,
                is_tv_show_primary,
                is_tv_show_episode,
                slug,
                season_number,
                episode_number,
                id_parent
            ])
        except:
            continue

        production_ids[unique_title] = cursor.lastrowid

        if i % commit_every == 0:
            db.commit()

    db.commit()
    print t.elapsed_seconds()

    print "Importing ratings..."

    # import ratings
    i = 0
    for line in open('dumps/ratings.list', 'r'):
        i += 1

        if 0 == i % 100000:
            print i, 'Ratings'

        if i >= limit:
            break

        line = line
        regex = regexes.rating_regex
        m = re.match(regex, line.decode('iso8859', 'replace'))
        if (not m):
            badlines_ratings.write(line)
            continue
        rating_distribution = m.group('rating_distribution')
        num_ratings  = m.group('num_ratings')
        rating       = m.group('rating')
        unique_title = m.group('unique_title')
        if (unique_title in production_ids):
            id_productions = production_ids[unique_title]
        try:
            cursor.execute("""UPDATE productions SET
                    rating = ?,
                    num_ratings = ?,
                    rating_distribution = ?
                WHERE id = ?""", [rating, num_ratings, rating_distribution, id_productions])
        except:
            continue

        if i % commit_every == 0:
            db.commit()

    db.commit()
    print t.elapsed_seconds()

    print "Importing directors..."

    # import directors
    i = 0
    people_ids = {}
    for line in open('dumps/directors.list', 'r'):
        i += 1

        if 0 == i % 100000:
            print i, 'Directors'

        if i >= limit:
            break

        # blank lines separate directors
        if line == "\n":
            current_id_people = None
            continue

        regex = regexes.director_regex
        m = re.match(regex, line.decode('iso8859', 'replace'))

        if not m:
            badlines_directors.write(line)
            continue

        unique_name  = m.group('unique_name')
        first_name   = m.group('first_name')
        last_name    = m.group('last_name')
        number       = m.group('number')
        unique_title = m.group('unique_title')
        description  = m.group('description')

        if unique_title in production_ids:
            id_productions = production_ids[unique_title]
        else:
            continue

        if (unique_name and (unique_name in people_ids)):
            current_id_people = people_ids[unique_name]

        elif (unique_name):

            try:
                cursor.execute("""INSERT INTO people (first_name, last_name, number) VALUES (?, ?, ?)""",
                    [first_name, last_name, number])
            except:
                continue

            current_id_people = cursor.lastrowid
            people_ids[unique_name] = current_id_people

        try:
            cursor.execute("""INSERT INTO roles (id_people, id_productions, id_role_types, description) VALUES (?,?,?,?)""",
                [current_id_people, id_productions, 'director', description])
        except:
            continue

        if i % commit_every == 0:
            db.commit()

    db.commit()
    print t.elapsed_seconds()

    print "Importing titles..."

    # import titles
    i = 0
    current_title = None
    current_year = None
    for line in open('dumps/aka-titles.list', 'r'):
        i += 1

        if 0 == i % 100000:
            print i, 'Titles'

        if i >= limit:
            break

        # blank lines separate productions
        if line == "\n":
            current_title = None
            current_year = None
            continue

        regex = regexes.aka_titles_regex
        m = re.match(regex, line.decode('iso8859', 'replace'))

        if not m:
            badlines_aka_titles.write(line)
            continue

        # print m.groups()

        if not current_title:
            current_title = (m.group('name') or m.group('qname')).strip()
            current_year = (m.group('year') or m.group('noyear')).strip()
            assert current_title is not None
        else:
            aka_title = (m.group('aka_name') or m.group('aka_qname')).strip()
            aka_year = (m.group('aka_year') or m.group('aka_noyear')).strip()

            try:
                cursor.execute("""INSERT INTO aka_titles (year, title, aka_year, aka_title) VALUES (?,?,?,?)""",
                    [current_year, current_title, aka_year, aka_title])
            except:
                continue

        if i % commit_every == 0:
            db.commit()

    db.commit()
    print t.elapsed_seconds()

    print "Importing genres..."

    # import genres
    i = 0
    genre_ids = {}
    for line in open('dumps/genres.list', 'r'):
        i += 1
        if 0 == i % 100000:
            print i, 'Genre lines'

        if i >= limit:
            break
        regex = regexes.genre_regex
        m = re.match(regex, line.decode('iso8859', 'replace'))

        if not m:
            continue

        unique_title = m.group('unique_title')
        genre = m.group('genre').strip()

        if unique_title in production_ids:
            id_productions = production_ids[unique_title]
        else:
            continue

        if genre in genre_ids:
            id_genres = genre_ids[genre]
        else:
            cursor.execute("""INSERT INTO genres (title) VALUES (?)""", [genre])
            id_genres = cursor.lastrowid
            genre_ids[genre] = id_genres

        try:
            cursor.execute("""INSERT INTO genres_productions (id_genres, id_productions) VALUES (?,?)""", [id_genres, id_productions])
        except:
            continue

        if (i % commit_every == 0):
            db.commit()

    db.commit()
    db.close()
    print t.elapsed_seconds()

    print "Finished importing"


def add_stats_and_indexes():
    print "Creating indexes..."
    t = timer.Timer()

    db = connect_db()
    cursor = db.cursor()

    sql_statements = (
        """CREATE INDEX "main"."roles_id_people" ON "roles" ("id_people" ASC)""",
        """CREATE INDEX productions_id_parent ON productions(id_parent)""",
        """CREATE INDEX productions_is_tv_show_primary ON productions(is_tv_show_primary)""",
        """CREATE INDEX productions_num_ratings ON productions(num_ratings)""",
        """CREATE INDEX productions_year ON productions(year)""",
        """CREATE INDEX "genres_productions_id_genres" ON "genres_productions" ("id_genres" ASC)""",
        """CREATE INDEX "genres_productions_id_productions" ON "genres_productions" ("id_productions" ASC)""",
        """UPDATE people
                SET total_popularity = (
                    SELECT SUM(num_ratings)
                    FROM productions
                    WHERE id IN (
                        SELECT id_productions
                        FROM roles
                        WHERE id_people = people.id
                    )
            )""",
        """UPDATE people
                SET average_rating = (
                    SELECT AVG(rating)
                    FROM productions
                    WHERE num_ratings > 1000 AND id IN (
                        SELECT id_productions
                        FROM roles
                        WHERE id_people = people.id
                    )
            )""",
        """UPDATE productions
                SET total_tv_show_popularity = (
                    SELECT SUM(p.num_ratings)
                    FROM productions p
                    WHERE p.id_parent = productions.id
                )
            WHERE is_tv_show_primary;""",
        """UPDATE productions
                SET average_tv_show_rating = (
                    SELECT AVG(p.rating)
                    FROM productions p
                    WHERE p.id_parent = productions.id
            )
            WHERE is_tv_show_primary""",
        """CREATE INDEX productions_total_tv_show_popularity ON productions(total_tv_show_popularity)""",
        """CREATE INDEX people_total_popularity ON people(total_popularity)""",
    )
    for sql_statement in sql_statements:
        try:
            print "Executing statement: " + sql_statement
            cursor.execute(sql_statement)
            print t.elapsed_seconds()
        except:
            pass

    db.commit() # not sure if this is necessary?
    db.close()

if __name__ == '__main__':
    do_import()
    add_stats_and_indexes()
