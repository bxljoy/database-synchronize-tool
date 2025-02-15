CREATE TABLE public.netflix_shows (
	show_id text NOT NULL,
	"type" text NULL,
	title text NULL,
	director text NULL,
	cast_members text NULL,
	country text NULL,
	date_added date NULL,
	release_year int4 NULL,
	rating text NULL,
	duration text NULL,
	listed_in text NULL,
	description text NULL,
	CONSTRAINT netflix_shows_pkey PRIMARY KEY (show_id)
);