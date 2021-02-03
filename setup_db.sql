CREATE TABLE processed_archive_page (
	publisher VARCHAR,
	url VARCHAR PRIMARY KEY
)

CREATE TABLE article (
	url VARCHAR PRIMARY KEY,
	publisher VARCHAR,
	title VARCHAR,
	published TIMESTAMP WITH TIME ZONE,
	category VARCHAR,  -- publisher specific
	content VARCHAR,
	tags VARCHAR[],  -- publisher specific
)

CREATE TABLE temp (
    publisher VARCHAR,
    key VARCHAR,
    val VARCHAR
)
