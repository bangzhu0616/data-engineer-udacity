CREATE TABLE IF NOT EXISTS public.pestcide_use (
    pestcide_use_id bigint identity(1, 1),
    compound_id int NOT NULL,
    year int4,
    county_id int NOT NULL,
    low_est decimal,
    high_est decimal,
    CONSTRAINT pestcide_use_pkey PRIMARY KEY(pestcide_use_id)
);

CREATE TABLE IF NOT EXISTS public.compounds (
    compound_id bigint identity(1, 1),
    compound_name varchar(256),
    CONSTRAINT compounds_pkey PRIMARY KEY(compound_id)
);

CREATE TABLE IF NOT EXISTS public.counties (
    county_id bigint identity(1, 1),
    state_code int NOT NULL,
    county_code int,
    county_name varchar(256),
    CONSTRAINT counties_pkey PRIMARY KEY(county_id)
);

CREATE TABLE IF NOT EXISTS public.states (
    state_code int NOT NULL,
    state_name varchar(256),
    CONSTRAINT states_pkey PRIMARY KEY(state_code)
);

CREATE TABLE IF NOT EXISTS public.staging_pestcide (
    compound_name varchar(256),
    year int4,
    state_code int,
    county_code int,
    low_est decimal,
    high_est decimal
);

CREATE TABLE IF NOT EXISTS public.staging_dictionary (
    state_code int,
    county_code int,
    county_name varchar(256),
    state_name varchar(256)
);