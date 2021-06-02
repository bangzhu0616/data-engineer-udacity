# DROP TABLES

pestcide_usage_table_drop = 'drop table if exists pestcide_use'
compound_table_drop = 'drop table if exists compounds'
county_table_drop = 'drop table if exists counties'
state_table_drop = 'drop table if exists states'


# CREATE TABLES

pestcide_usage_table_create = ("""
CREATE TABLE pestcide_use (
    pestcide_use_id serial primary key,
    compound_id int NOT NULL REFERENCES compounds (compound_id),
    year int,
    county_id int NOT NULL REFERENCES counties (county_id),
    low_est decimal,
    high_est decimal
);
""")

compound_table_create = ("""
CREATE TABLE compounds (
    compound_id serial primary key,
    compound_name varchar
);
""")

county_table_create = ("""
CREATE TABLE counties (
    county_id serial primary key,
    state_code int NOT NULL REFERENCES states (state_code),
    county_code int,
    county_name varchar
);
""")

state_table_create = ("""
CREATE TABLE states (
    state_code int primary key,
    state_name varchar
);
""")


# INSERT RECORDS
pestcide_usage_table_insert = ("""
INSERT INTO pestcide_use (compound_id, year, county_id, low_est, high_est)
VALUES (%s, %s, %s, %s, %s);
""")

compound_table_insert = ("""
INSERT INTO compounds (compound_name)
SELECT %s
WHERE
    NOT EXISTS(SELECT compound_name FROM compounds WHERE compound_name=%s);
""")

county_table_insert = ("""
INSERT INTO counties (state_code, county_code, county_name)
SELECT %s, %s, %s
WHERE
    NOT EXISTS(SELECT county_name FROM counties WHERE state_code=%s AND county_code=%s);
""")

state_table_insert = ("""
INSERT INTO states (state_code, state_name)
VALUES (%s, %s)
ON conflict(state_code) do nothing;
""")

create_table_queries = [state_table_create, county_table_create, compound_table_create, pestcide_usage_table_create]
drop_table_queries = [pestcide_usage_table_drop, compound_table_drop, county_table_drop, state_table_drop]