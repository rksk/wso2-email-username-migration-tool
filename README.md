# wso2-email-username-migration-tool

### Configuration files

The file `config.properties` defines DB connection parameters for both UM DB and REG/IDN DBs.
```
CONNECTION_URL = <DB connection URL>
CONNECTION_USERNAME = <DB username>
CONNECTION_PASSWORD = <DB password>
CONNECTION_DRIVERCLASS = <JDBC driver class>
CONNECTION_JDBCDRIVER = <path of the JDBC drive>
```

### Steps to execute the tool

* First compile the code with following command.
```
mvn clean install
```
* Execute the binary file.
```
java -jar target/wso2.email.username.migration.tool-1.0.jar
```
