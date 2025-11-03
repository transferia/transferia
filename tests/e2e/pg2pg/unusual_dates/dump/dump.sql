create table testtable (
    id integer primary key,
    val1 timestamp (6) without time zone,
    val2 timestamp (6) with time zone,
    val3 date
);
insert into testtable values (1, '2000-10-19 10:23:54.123', '2000-10-19 10:23:54.123+02', '2000-10-19');
-- insert into testtable values (2, '2000-10-19 10:23:54.123 BC', '2000-10-19 10:23:54.123+02 BC', '2000-10-19 BC');
insert into testtable values (3, '40000-10-19 10:23:54.123456', '40000-10-19 10:23:54.123456+02', '40000-10-19');
-- insert into testtable values (4, '4000-10-19 10:23:54.123456 BC', '4000-10-19 10:23:54.123456+02 BC', '4000-10-19 BC');

-- BC dates will be supported in https://st.yandex-team.ru/TM-5127
