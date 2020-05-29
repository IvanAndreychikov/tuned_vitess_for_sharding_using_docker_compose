CREATE TABLE users_seq (id int, next_id bigint, cache bigint, primary key(id)) comment 'vitess_sequence';
INSERT INTO users_seq (id, next_id, cache) VALUES (0, 100, 2);
CREATE TABLE chat_history_seq (id int, next_id bigint, cache bigint, primary key(id)) comment 'vitess_sequence';
INSERT INTO chat_history_seq (id, next_id, cache) VALUES (0, 100, 2);
