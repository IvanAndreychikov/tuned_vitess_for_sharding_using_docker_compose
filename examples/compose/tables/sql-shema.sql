CREATE TABLE users (
  id bigint unsigned NOT NULL,
  name varchar(100) NOT NULL,
  surname varchar(200) NOT NULL,
  email varchar(160) NOT NULL,
  password varchar(600) NOT NULL,
  age int(10) unsigned NOT NULL DEFAULT '0',
  sex varchar(80) NOT NULL,
  interests mediumtext NOT NULL,
  city varchar(300) NOT NULL,
  PRIMARY KEY (id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;

CREATE TABLE chat_history (
  id bigint unsigned NOT NULL,
  user_id_from bigint unsigned NOT NULL,
  user_id_to bigint unsigned NOT NULL,
  time_send datetime NOT NULL,
  message text NOT NULL,
  PRIMARY KEY (id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;

CREATE TABLE friend_relationship (
  left_user bigint unsigned NOT NULL,
  right_user bigint unsigned NOT NULL,
  PRIMARY KEY (left_user,right_user)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;
