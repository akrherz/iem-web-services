copy stations(id,synop,name,state,country,elevation,network,online,params,county,plot_name,climate_site,remote_id,nwn_id,spri,wfo,archive_begin,archive_end,modified,tzname,iemid,metasite,sigstage_low,sigstage_action,sigstage_bankfull,sigstage_flood,sigstage_moderate,sigstage_major,sigstage_record,ugc_county,ugc_zone,geom,ncdc81,temp24_hour,precip24_hour,ncei91) FROM STDIN (FORMAT CSV);
96404,,Tok 70 SE,AK,US,2000,USCRN,t,,Southeast Fairbanks,,AKTAGY,,,,AFG,,,2021-04-20 07:54:46.367603-05,America/Anchorage,254829,f,,,,,,,,AKC240,AKZ224,0101000020E61000006666666666A661C03D0AD7A3705D4F40,USC00507513,,,
DNKI4,,Dunkerton 1WNW - Crane Creek,IA,US,287.222,IA_DCP,t,,Black Hawk,Dunkerton 1WNW - Crane Creek,IA8706,,,,DMX,2018-06-08 09:30:00-05,,2019-08-13 12:41:01.897685-05,America/Chicago,255660,f,,,,,,,,IAC013,IAZ039,0101000020E6100000643BDF4F8D0B57C0992A1895D4494540,USW00094910,,,
\.

-- Some faked webcam data
COPY webcams(id, geom) FROM STDIN (FORMAT CSV);
IDOT-013-01,SRID=4326;POINT(-95 42)
IDOT-013-02,SRID=4326;POINT(-95 42)
\.

COPY camera_log (cam, valid, drct) FROM STDIN (FORMAT CSV);
IDOT-013-01,2021-01-01 12:00:00+00,0
IDOT-013-02,2021-01-01 12:04:00+00,0
\.
