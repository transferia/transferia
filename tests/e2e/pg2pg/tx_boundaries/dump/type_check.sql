create table trash (trash_id serial primary key, title text);

create table __test (id serial primary key, title text);

insert into __test select s, md5(random()::text) from generate_Series(1, 50000) as s;

create table pkey_only (key1 text, key2 text, PRIMARY KEY (key1, key2));
insert into pkey_only values ('foo', 'bar');
