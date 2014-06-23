#!/bin/bash

mkdir -p dumps
curl ftp://ftp.fu-berlin.de/pub/misc/movies/database/ratings.list.gz | gunzip > dumps/ratings.list
