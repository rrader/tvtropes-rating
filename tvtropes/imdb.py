import csv
import html
from urllib.parse import urlencode
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


if __name__ == "__main__":
    imdb = MyImdb()
    with open("gen/films.csv", newline='') as csvfile_src:
        with open("gen/films_imdb.csv", "w", newline='') as csvfile_dst:
            with open("gen/films_no_imdb.csv", "w", newline='') as csvfile_dst2:
                reader = csv.reader(csvfile_src)
                writer = csv.writer(csvfile_dst)
                writer_no_imdb = csv.writer(csvfile_dst2)
                for row in reader:
                    print(row)
                    try:
                        film = imdb.find_by_title(row[0])[0]
                        i_id = film["imdb_id"]
                        movie = imdb.find_movie_by_id(i_id)
                        writer.writerow([
                            row[0],
                            movie.imdb_id,
                            movie.title,
                            movie.rating,
                            movie.year,
                            movie.tagline,
                            ','.join(movie.genres)
                        ])
                    except IndexError:
                        writer_no_imdb.writerow([
                            row[0]
                        ])
                    csvfile_dst.flush()
                    csvfile_dst2.flush()
