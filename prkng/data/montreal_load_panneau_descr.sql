-- "POTEAU_ID_POT"|"PANNEAU_ID_PAN"|"DESCRIPTION_RPA"|"CODE_RPA"|"FLECHE_PAN"|"POSITION_POP"
drop table if exists montreal_descr_panneau;
create table montreal_descr_panneau (
	 POTEAU_ID_POT int,
	 PANNEAU_ID_PAN int,
	 DESCRIPTION_RPA varchar,
	 CODE_RPA varchar,
	 FLECHE_PAN int,
	 POSITION_POP int
);

-- load from csv
copy montreal_descr_panneau from '{description_panneau}'
WITH CSV HEADER DELIMITER '|' ENCODING 'LATIN1';

-- indexes
create index on montreal_descr_panneau(PANNEAU_ID_PAN);
create index on montreal_descr_panneau(POTEAU_ID_POT);

-- create a joined table between sign and signpost
drop table if exists montreal_panneau;
create table montreal_panneau as
    select
        row_number() over () as id
        ,d.poteau_id_pot
        ,m.geom
        ,d.description_rpa
        ,d.code_rpa
        ,d.fleche_pan
        ,d.position_pop
    from montreal_descr_panneau d
    join montreal_poteaux m on m.poteau_id_pot = d.poteau_id_pot;

-- indexes
create index on montreal_panneau using gist(geom);
create index on montreal_panneau (poteau_id_pot);
create index on montreal_panneau (fleche_pan);
