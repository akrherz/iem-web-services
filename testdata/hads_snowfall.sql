-- 6 hour snowfall
insert into raw2023_11(station, valid, key, value) values
('DMX', '2023-11-10 12:00+00', 'SFQRZZZ', 10);

insert into stations(iemid, id, name, network, geom) values
(-1, 'DMX', 'Des Moines', 'IA_DCP', ST_Point(-93.648, 41.533, 4326));
