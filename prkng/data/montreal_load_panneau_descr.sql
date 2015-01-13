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
create index on montreal_descr_panneau(CODE_RPA);


