
insert into alldata(model, station, runtime, ftime)
values ('NBS', 'KDSM', (date_trunc('day', now()
at time zone 'UTC') || '+00')::timestamptz,
(date_trunc('day', now() at time zone 'UTC') || '+00')::timestamptz);
