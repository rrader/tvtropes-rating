clean:
	rm -f 3rd/sqlitedb/moviedb.sqlite gen/rating.sqlite gen/tropes.sqlite
	mongo tvtropes-grab --eval "db.dropDatabase()"
	redis-cli -n 1 flushdb
