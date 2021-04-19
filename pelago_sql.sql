-- Create Reddit schema 

CREATE SCHEMA reddit ;

-- Create table to store the hot 100 reddit posts hourly

-- Table: reddit.tbl_reddit_hot_posts

-- DROP TABLE reddit.tbl_reddit_hot_posts;

CREATE TABLE reddit.tbl_reddit_hot_posts
(
    post_id varchar(100) NOT NULL,  --- reddit id
    title text ,    --- reddit title of the post
    url varchar,    --- url of the reddit post
    selftext text,  --- text in the reddit post
    author varchar(512),  --- author of the post
    upvote_ratio decimal(5,2),  --- upvote ratio
    over_18 boolean,  ---  boolean flag to check if age > 18
    score int,  --- score of the post
    num_comments int,  --- number of comments for the post
    author_premium boolean,  --- flag to check if the author is premium
    treatment_tags text,  --- list of tags
    created_datetime timestamp,  --- create timestamp of the post
    current_dt date NOT NULL,   --- current date i.e the date the post is retrieved 
    current_hr smallint NOT NULL,  --- current hour i.e the hour the post is retrieved
    last_updated_ts timestamp without time zone DEFAULT now(),  --- last updated timestamp for audit
    CONSTRAINT tbl_reddit_hot_posts_pkey PRIMARY KEY (post_id, current_dt, current_hr)  --- primary key
)
WITH (
    OIDS = FALSE
)
TABLESPACE pg_default;

ALTER TABLE reddit.tbl_reddit_hot_posts  --- change owner to the username of db
    OWNER to pelagodb;

-- FUNCTION: reddit.update_modified_column()

--- To update the last modified timestamp whenever there is an update to table columns.

-- DROP FUNCTION reddit.update_modified_column();

CREATE FUNCTION reddit.update_modified_column()
    RETURNS trigger
    LANGUAGE 'plpgsql'
    COST 100
    VOLATILE NOT LEAKPROOF
AS $BODY$
BEGIN
    NEW.last_updated_ts = now();
    RETURN NEW; 
END;
$BODY$;

ALTER FUNCTION reddit.update_modified_column()
    OWNER TO pelagodb;

-- Trigger: update_modtime

-- A trigger to modify the last_updates_ts column whenever there is an update to the table columns.

-- DROP TRIGGER update_modtime ON reddit.tbl_reddit_hot_posts;

CREATE TRIGGER update_modtime
    BEFORE UPDATE 
    ON reddit.tbl_reddit_hot_posts
    FOR EACH ROW
    EXECUTE PROCEDURE reddit.update_modified_column();
	
-- Role: "Reddit-Lambda-role"
-- Create lambda role 
-- DROP ROLE "Reddit-Lambda-role";

CREATE ROLE "Reddit-Lambda-role" WITH
  LOGIN
  NOSUPERUSER
  INHERIT
  CREATEDB
  NOCREATEROLE
  NOREPLICATION;

GRANT rds_iam TO "Reddit-Lambda-role";

GRANT ALL ON SCHEMA reddit TO "Reddit-Lambda-role";

GRANT ALL ON TABLE reddit.tbl_reddit_hot_posts TO pelagodb; -- grant all access on the table to the user

GRANT ALL ON TABLE reddit.tbl_reddit_hot_posts TO rds_iam;  --- grant all access to rds_iam if in future we connect to db using iam username for additional security

	