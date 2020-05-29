create table users_seq(id int, next_id bigint, cache bigint, primary key(id)) comment 'vitess_sequence';
insert into users_seq(id, next_id, cache) values(0, 539590, 2);
create table friend_relationship_seq(id int, next_id bigint, cache bigint, primary key(id)) comment 'vitess_sequence';
insert into friend_relationship_seq(id, next_id, cache) values(0, 300, 2);
create table chat_history_seq(id int, next_id bigint, cache bigint, primary key(id)) comment 'vitess_sequence';
insert into chat_history_seq(id, next_id, cache) values(0, 300, 2);
