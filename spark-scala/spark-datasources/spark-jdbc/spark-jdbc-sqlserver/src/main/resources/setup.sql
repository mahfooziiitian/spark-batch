create table films
(
    film_id              serial primary key,
    title                varchar,
    description          varchar,
    release_year         varchar,
    language_id          integer,
    original_language_id integer,
    rental_duration      integer,
    rental_rate          decimal(4, 2),
    length               integer,
    replacement_cost     decimal(5, 2),
    rating               varchar,
    special_features     varchar,
    last_update timestamp default current_timestamp
);