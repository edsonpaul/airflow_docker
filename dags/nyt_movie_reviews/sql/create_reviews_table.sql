CREATE TABLE IF NOT EXISTS reviews (
    id int GENERATED BY DEFAULT AS IDENTITY PRIMARY KEY,
    ingested_at timestamp DEFAULT CURRENT_TIMESTAMP,
    display_title varchar(50),
    mpaa_rating varchar(50),
    critics_pick int,
    byline varchar(50),
    headline text,
    summary_short text,
    publication_date date,
    opening_date date,
    date_updated date,
    link jsonb,
    multimedia jsonb
)