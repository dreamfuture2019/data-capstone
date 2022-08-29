CREATE TABLE IF NOT EXISTS public.immigrations (
	cicid           BIGINT NOT NULL PRIMARY KEY SORTKEY,
	i94yr           INT,
	i94mon          INT DISTKEY,
	i94res          INT,
	i94port         VARCHAR(5),
	arrdate         BIGINT,
	i94mode         INT,
	i94addr         VARCHAR(5),
	depdate         BIGINT,
	i94bir          INT,
	i94visa         INT,
	count           INT,
	dtadfile        VARCHAR(256),
	visapost        VARCHAR(256),
	occup           VARCHAR(256),
	entdepa         VARCHAR(256),
	entdepd         VARCHAR(256),
	entdepu         VARCHAR(256),
	matflag         VARCHAR(5),
	biryear         INT,
	dtaddto         VARCHAR(256),
	gender          VARCHAR(5),
	insnum          VARCHAR(256),
	airline         VARCHAR(5),
	admnum          BIGINT,
	fltno           VARCHAR(256),
	visatype        VARCHAR(5)
) diststyle key;

CREATE TABLE IF NOT EXISTS public.i94_models (
	mode_id          INT PRIMARY KEY SORTKEY, 
	name             VARCHAR(256)
)  diststyle all;

CREATE TABLE IF NOT EXISTS public.i94_countries (
	code             INT PRIMARY KEY SORTKEY,
	name             VARCHAR(256)
)  diststyle all;

CREATE TABLE IF NOT EXISTS public.i94_addresses (
	code             VARCHAR(5) PRIMARY KEY SORTKEY,
	address          VARCHAR(256)
)  diststyle all;

CREATE TABLE IF NOT EXISTS public.i94_ports (
	code             VARCHAR(5) PRIMARY KEY SORTKEY,
	name             VARCHAR(256)
) diststyle all;

CREATE TABLE IF NOT EXISTS public.i94_visas (
	id          INT PRIMARY KEY SORTKEY, 
	name             VARCHAR(256)
)  diststyle all;

CREATE TABLE IF NOT EXISTS public.temperatures (
	dt                     TIMESTAMP PRIMARY KEY SORTKEY,
	avg_temp               numeric(18,0),
    avg_temp_uncertainty   numeric(18,0),
    city                   VARCHAR(256),
    country                VARCHAR(256) DISTKEY,
    latitude               numeric(18,0),
    longtitude             numeric(18,0)
) diststyle key;

CREATE TABLE IF NOT EXISTS public.arrival_date (
	arrdate      BIGINT PRIMARY KEY SORTKEY,
    hour         SMALLINT,
    day          SMALLINT,
    week         SMALLINT,
    month        SMALLINT DISTKEY,
    year         SMALLINT,
    weekday      SMALLINT
)  diststyle key;

CREATE TABLE IF NOT EXISTS public.us_cities_demographics (
	id               INT IDENTITY(1,1) PRIMARY KEY,
    city             VARCHAR(256),
    state            VARCHAR(256),
    median_age       INT,
    male_population  INT,
    female_population  INT,
    total_population   INT,
    number_of_veterans INT,
    foreign_born            INT,
    average_household_size  INT,
    state_code              VARCHAR(5),
    race                    VARCHAR(256),
    count                   INT
)  diststyle all;

CREATE TABLE IF NOT EXISTS public.airport_codes (
	ident          VARCHAR(10) PRIMARY KEY SORTKEY,
    type           VARCHAR(256),
    name           VARCHAR(256),
    elevation_ft   INT,
    continent      VARCHAR(256),
    iso_country    VARCHAR(256),
    iso_region     VARCHAR(256),
    municipality   VARCHAR(256),
    gps_code       VARCHAR(10),
    iata_code      VARCHAR(10),
    local_code     VARCHAR(10) SORTKEY,
    coordinates    VARCHAR(256)
)  diststyle all;


