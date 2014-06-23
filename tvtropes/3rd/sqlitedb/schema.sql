BEGIN TRANSACTION;

CREATE TABLE productions (
	id INTEGER PRIMARY KEY AUTOINCREMENT NOT NULL,
	id_parent INTEGER DEFAULT NULL REFERENCES productions(id) ON DELETE CASCADE ON UPDATE CASCADE,
	is_tv_show_primary BOOL DEFAULT false,
	is_tv_show_episode BOOL DEFAULT false,
	total_tv_show_popularity INTEGER,
	average_tv_show_rating FLOAT,
	year INTEGER,
        year_number VARCHAR,
	slug VARCHAR,
	title VARCHAR,
	season INTEGER,
	number INTEGER,
	media_type VARCHAR,
	rating FLOAT,
	num_ratings INTEGER,
	rating_distribution VARCHAR
);
--CREATE UNIQUE INDEX UK_productions_title_year ON productions(title ASC, year ASC);
--CREATE UNIQUE INDEX UK_productions_slug ON productions(slug ASC);

CREATE TABLE people (
	id INTEGER PRIMARY KEY AUTOINCREMENT NOT NULL,
	last_name VARCHAR,
	first_name VARCHAR,
	number VARCHAR DEFAULT NULL,
	total_popularity INTEGER,
	average_rating FLOAT
);
--CREATE UNIQUE INDEX UK_people_last_name_first_name_number ON people(last_name, first_name, number);

CREATE TABLE roles (
	id INTEGER PRIMARY KEY AUTOINCREMENT NOT NULL,
	id_people INTEGER REFERENCES people(id) ON DELETE CASCADE ON UPDATE CASCADE,
	id_role_types INTEGER, -- REFERENCES role_types(id),
	id_productions INTEGER REFERENCES productions(id) ON DELETE CASCADE ON UPDATE CASCADE,
	description VARCHAR
);
--CREATE UNIQUE INDEX UK_roles_id_people_id_role_types_id_productions ON roles(id_people, id_role_types, id_productions);

CREATE TABLE role_types (
	textid VARCHAR PRIMARY KEY NOT NULL,
	title VARCHAR
);
--CREATE UNIQUE INDEX UK_role_types_title ON role_types(title);

CREATE TABLE genres (
	id INTEGER PRIMARY KEY AUTOINCREMENT NOT NULL,
	title VARCHAR
);

CREATE TABLE genres_productions (
	id INTEGER PRIMARY KEY AUTOINCREMENT NOT NULL,
	id_productions INTEGER REFERENCES productions(id) ON DELETE CASCADE ON UPDATE CASCADE,
	id_genres INTEGER REFERENCES genres(id) ON DELETE CASCADE ON UPDATE CASCADE
);

COMMIT;

