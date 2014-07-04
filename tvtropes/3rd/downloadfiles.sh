[ -f dumps/movies.list ] || curl ftp://ftp.fu-berlin.de/pub/misc/movies/database/movies.list.gz | gunzip > dumps/movies.list
[ -f dumps/aka-titles.list ] || curl ftp://ftp.fu-berlin.de/pub/misc/movies/database/aka-titles.list.gz | gunzip > dumps/aka-titles.list
[ -f dumps/directors.list ] || curl ftp://ftp.fu-berlin.de/pub/misc/movies/database/directors.list.gz | gunzip > dumps/directors.list
[ -f dumps/ratings.list ] || curl ftp://ftp.fu-berlin.de/pub/misc/movies/database/ratings.list.gz | gunzip > dumps/ratings.list
[ -f dumps/genres.list ] || curl ftp://ftp.fu-berlin.de/pub/misc/movies/database/genres.list.gz | gunzip > dumps/genres.list
