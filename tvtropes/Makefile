clean:
	rm -f gen/rating.sqlite gen/tropes.sqlite gen/films_no_imdb.csv
	mongo tvtropes-grab --eval "db.dropDatabase()"
	redis-cli -n 1 flushdb

cleanall: clean
	rm -f 3rd/sqlitedb/moviedb.sqlite
