source ../venv/bin/activate  # should be virtualenv for python 3.x

make clean

rm -rf dl
mkdir -p dl

echo "==== Pulling data from TVTropes ==="
if [ "z$1" != 'zforce' ] && curl --user $(cat yandex.secret) https://webdav.yandex.ru/tropes.sqlite --fail > dl/tropes.sqlite; then
  echo
  echo "TVTropes DB Found on Yandex Disk"
  mv dl/tropes.sqlite gen/tropes.sqlite
else
  echo "Creating TVTropes DB"
  python crawler.py
  echo "Uploading TVTropes DB"
  curl --user $(cat yandex.secret) -T gen/tropes.sqlite https://webdav.yandex.ru
fi
echo

echo "==== Filling up local IMDB database ==="
cd 3rd
mkdir -p dumps
bash downloadfiles.sh
[ -f sqlitedb/moviedb.sqlite ] || python2 importdb_sqlite.py
cd ..

echo "==== Calculating rating for films ==="
if [ "z$1" != 'zforce' ] && curl --user $(cat yandex.secret) https://webdav.yandex.ru/rating.sqlite --fail > dl/rating.sqlite; then
  echo "Ratings DB Found on Yandex Disk"
  mv dl/rating.sqlite gen/rating.sqlite
else
  echo "Creating ratings DB"
  python imdb.py
  echo "Uploading ratings DB"
  curl --user $(cat yandex.secret) -T gen/rating.sqlite https://webdav.yandex.ru
fi

