DROP TABLE IF EXISTS montreal_rules_translation;
CREATE TABLE montreal_rules_translation (
    id serial,
    code varchar,
    description varchar,
    season_start varchar DEFAULT '',
    season_end varchar DEFAULT '',
    time_max_parking float DEFAULT 0.0,
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
    special_days varchar DEFAULT '',
    metered smallint,
    restrict_typ varchar DEFAULT '',
    permit_no varchar DEFAULT ''
);

copy montreal_rules_translation (code,description,season_start,season_end,
    time_max_parking,time_start,time_end,time_duration,lun,mar,mer,jeu,ven,
    sam,dim,daily,special_days,metered,restrict_typ)
from '{}'
WITH CSV HEADER DELIMITER ',' ENCODING 'UTF-8';
