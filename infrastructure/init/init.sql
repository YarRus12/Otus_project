CREATE SCHEMA IF NOT EXISTS STAGE;

CREATE TABLE IF NOT EXISTS STAGE.FLATS_TABLE
    (city varchar(100),
    street varchar(100),
    floor int,
    rooms int,
    price bigint,
    created_at date
    );

CREATE TABLE IF NOT EXISTS STAGE.ANSWERS_TABLE
    (id varchar(100),
    prediction bigint,
    created_at date
    );


DO
$$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'docker_app') THEN
        CREATE ROLE docker_app WITH PASSWORD 'docker_app';
    END IF;
END;
$$;

