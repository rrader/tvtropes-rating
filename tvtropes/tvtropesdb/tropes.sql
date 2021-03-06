BEGIN TRANSACTION;

CREATE TABLE film_tropes (
    id INTEGER PRIMARY KEY AUTOINCREMENT NOT NULL,
    film INTEGER,
    trope INTEGER,
    FOREIGN KEY(film) REFERENCES films(id),
    FOREIGN KEY(trope) REFERENCES tropes(id),
    UNIQUE(film, trope)
);

CREATE TABLE films (
    id INTEGER PRIMARY KEY AUTOINCREMENT NOT NULL,
    title VARCHAR UNIQUE,
    url VARCHAR UNIQUE,
    years VARCHAR
);

CREATE TABLE tropes (
    id INTEGER PRIMARY KEY AUTOINCREMENT NOT NULL,
    trope VARCHAR UNIQUE,
    url VARCHAR UNIQUE
);

COMMIT;

