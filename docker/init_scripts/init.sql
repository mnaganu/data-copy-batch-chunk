CREATE DATABASE IF NOT EXISTS `test_db1`;
GRANT ALL PRIVILEGES ON `test_db1`.* TO `test`@`%`;
CREATE DATABASE IF NOT EXISTS `test_db2`;
GRANT ALL PRIVILEGES ON `test_db2`.* TO `test`@`%`;
CREATE DATABASE IF NOT EXISTS `test_db3`;
GRANT ALL PRIVILEGES ON `test_db3`.* TO `test`@`%`;

use test_db1;

CREATE TABLE `sample` (
`id` int UNIQUE NOT NULL,
`name` text
);

INSERT INTO sample VALUES(1, 'name1');
INSERT INTO sample VALUES(2, NULL);
INSERT INTO sample VALUES(3, 'name3');

use test_db2;

CREATE TABLE `sample` (
`id` int UNIQUE NOT NULL,
`name` text
);
