CREATE TABLE processed_archive_page (
	publisher VARCHAR,
	url VARCHAR PRIMARY KEY
)

CREATE TABLE article (
	url VARCHAR PRIMARY KEY,
	publisher VARCHAR,
	title VARCHAR,
	author VARCHAR,
	published TIMESTAMP WITH TIME ZONE,
	category VARCHAR,
	content VARCHAR,
	tags VARCHAR[]
)