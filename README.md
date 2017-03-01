# styx-data-jdbc
JDBC backend for styx-data

## Running JUnit Tests

For MySQL and PostgreSQL, a running database server is required. 
The simplest way to install and start these servers is to use Docker:

    docker run -d --name mysql -p 3306:3306 -e MYSQL_ROOT_PASSWORD=sesam -e MYSQL_DATABASE=styx_test mysql:latest
    docker run -d --name mypostgres -p 5432:5432 -e POSTGRES_PASSWORD=sesam -e POSTGRES_DB=styx_test postgres:latest
