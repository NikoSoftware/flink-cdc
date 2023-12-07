

#### mysql ---> 为mysql的docker容器的name，你也可以使用mysql的docker容器的id

#### 开启log_bin的docker容器配置
docker exec mysql5.7 bash -c "echo '[mysqld]' >> /etc/mysql/mysql.conf.d/mysqld.cnf"
docker exec mysql5.7 bash -c "echo 'server-id=1' >> /etc/mysql/mysql.conf.d/mysqld.cnf"
docker exec mysql5.7 bash -c "echo 'sync_binlog=1' >> /etc/mysql/mysql.conf.d/mysqld.cnf"
docker exec mysql5.7 bash -c "echo 'log-bin=/var/lib/mysql/mysql-bin' >> /etc/mysql/mysql.conf.d/mysqld.cnf"
docker exec mysql5.7 bash -c "echo 'binlog_cache_size=256M' >> /etc/mysql/mysql.conf.d/mysqld.cnf"
docker exec mysql5.7 bash -c "echo 'binlog_format=mixed' >> /etc/mysql/mysql.conf.d/mysqld.cnf"
docker exec mysql5.7 bash -c "echo 'binlog-ignore-db=mysql,information_schema,performance_schema,sys' >> /etc/mysql/mysql.conf.d/mysqld.cnf"
docker exec mysql5.7 bash -c "echo 'lower_case_table_names=1' >> /etc/mysql/mysql.conf.d/mysqld.cnf"
docker exec mysql5.7 bash -c "echo 'character-set-server=utf8' >> /etc/mysql/mysql.conf.d/mysqld.cnf"
docker exec mysql5.7 bash -c "echo 'collation-server=utf8_general_ci' >> /etc/mysql/mysql.conf.d/mysqld.cnf"
docker exec mysql5.7 bash -c "echo 'sql_mode=ONLY_FULL_GROUP_BY,STRICT_TRANS_TABLES,NO_ZERO_IN_DATE,NO_ZERO_DATE,ERROR_FOR_DIVISION_BY_ZERO,NO_AUTO_CREATE_USER,NO_ENGINE_SUBSTITUTION' >> /etc/mysql/mysql.conf.d/mysqld.cnf"

#### 重启mysql容器
docker restart mysql5.7




