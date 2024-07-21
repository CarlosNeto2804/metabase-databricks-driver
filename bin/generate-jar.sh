docker build -t dbricks_driver .
docker create --name dbricks_container dbricks_driver
docker cp dbricks_container:/driver/target/databricks-sql.metabase-driver.jar ./databricks-sql.metabase-driver.jar
docker rm dbricks_container
docker rmi -f dbricks_driver
