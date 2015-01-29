DROP TABLE IF EXISTS montreal_rules_translation;
CREATE TABLE montreal_rules_translation (
    id serial,
    code varchar,
    description varchar,
    season_start varchar,
    season_end varchar,
    time_max_parking float,
    time_start float,
    time_end float,
    time_duration float,
    lun smallint,
    mar smallint,
    mer smallint,
    jeu smallint,
    ven smallint,
    sam smallint,
    dim smallint,
    daily float,
    special_days varchar,
    restrict_typ varchar
);

copy montreal_rules_translation (code,description,season_start,season_end,
    time_max_parking,time_start,time_end,time_duration,lun,mar,mer,jeu,ven,
    sam,dim,daily,special_days,restrict_typ)
from '{}'
WITH CSV HEADER DELIMITER ',' ENCODING 'UTF-8';
