# Data bridge stored procedure

Welcome, you made the first step to access any JDBC data source from the Db2 for z/OS (e.g. WatsonX.data). Just a couple of steps and you are ready to go. 

## The purpose of this project

The idea of this project is to provide an easy and extensible way to access JDBC data source within Db2 for z/OS, so that those data sources can be combined with data in Db2 for z/OS. The main component to achieve this is a new stored procedure. It establishes the link between the JDBC data source and Db2 for z/OS. After the stored procedure is called, the data of the foreign data source can be accessed in Db2 for z/OS via a temporary table. 

## How to install the data bridge stored procedure

### Prerequisites

The project is written in a way that only minimal prerequisites are needed.

* Java 11 or above 
* Db2 for z/OS needs to be configured to support Java stored procedure. See [setting up WLM environment for java routines](https://www.ibm.com/docs/en/db2-for-zos/12?topic=functions-setting-up-environment-java-routines)

### Compile the code 

First, we need to compile the code. This can be done through the following command in the `src` directory. 

```
javac com/ibm/databridge/*
```

Next, we need to create a jar file that contains the compiled code. The following command will do the trick.

```
jar cf db2-data-bridge.jar com/ibm/databridge/*.class
```

### Install the JAR file in Db2 for z/OS

Use the SQLJ.DB2_INSTALL_JAR stored procedure, which is shipped with Db2 for z/OS to install the jar in Db2 for z/OS. 

```
CALL SQLJ.DB2_INSTALL_JAR(<jar-file-blob>, <jar-name>, 0)
```

For more details how to use the stored procedure see [SQLJ.DB2_INSTALL_JAR](https://www.ibm.com/docs/en/db2-for-zos/12?topic=db2-sqljdb2-install-jar).

| Parameter | Description |
| ----------- | ----------- |
| <jar-file-blob> | the jar file content |
| <jar-name> | can be selected freely, but need to be consistent with the next step |

### Create the Db2 data bridge stored procedure in Db2 for z/OS

After the jar file is known to Db2 for z/OS, we need to define the data bridge stored procedure. Use the following DDL for this. 

```
CREATE PROCEDURE <schema>.DATA_BRIDGE(propertiesFilename VARCHAR(1024), query CLOB(4M), tableName VARCHAR(128), OUT RETURN_CODE INT, OUT LOG_OUTPUT VARCHAR(32000)) 
	LANGUAGE JAVA
	EXTERNAL NAME '<jar-name>:com.ibm.databridge.Db2DataBridge.queryData'
	PARAMETER STYLE JAVA
	WLM ENVIRONMENT <wlm-name> 
	DYNAMIC RESULT SETS 1
	SECURITY <security-level>; 
```

| Parameter | Input/Output | Description |
| ----------- | ----------- | ----------- |
| propertiesFilename | Input | configuration of the data bridge stored procedure (includes connection parameters) |
| query | Input | the query to execute on the JDBC data source |
| RETURN_CODE | Output | 0 if the stored procedure run was successful. 8 in case of errors |
| LOG_OUTPUT | Output | diagnostics about the execution. Can be used to diagnose failures |

| Parameter | Description |
| ----------- | ----------- |
| jar-name | use the same name as in previous step |
| wlm-name | the name of the wlm environment the stored procedure runs in |
| security-level | define under which external security the stored procedure should run |


## How to use the data bridge stored procedure

### Add JDBC driver to the classpath

To access a JDBC data source, the data bridge stored procedure needs access to the corresponding JDBC driver. Download the JDBC of the database system you want to connect to and add it to classpath of the Java WLM environment. See [runtime environment for Java stored procedures](https://www.ibm.com/docs/en/db2-for-zos/12?topic=routines-runtime-environment-java). You can also add multiple, if you want to connect to different data sources. 

*Example*

```
CLASSPATH=/u/<username>/presto/presto-jdbc-0.286.jar:/u/<username>/java/postgresql-42.7.3.jar
```

### Setup the configuration for the call of the data gate stored procedure

Next, we need to create a properties file, which is handed over on the first parameter of the data bridge stored procedure call. It defines the connection to the JDBC data source, as well as some additional configuration settings for the stored procedure. The following properties can be set in that file:


| Parameter | Description |
| ----------- | ----------- |
| jdbc.driver | the class of the JDBC driver |
| jdbc.url | the connection string |
| jdbc.propertiesFile | reference to another properties file that will be forwarded to the JDBC driver |
| db2.default.truncate.size | If the source database string or binary data type is longer than what is allowed in Db2 for z/OS this parameter defines to which size to truncate the data |
| databridge.insert.batch.buffer.size | How many rows to insert together |
| databridge.diagnostics.showPropertiesSettingsInLogOutput | If set to "true", the properties file content will be included in the LOG_OUTPUT parameter of the stored procedure. If the stored procedure is defined with SECURITY DB2 or DEFINER it is not recommended to enable this parameter for security reasons. |


*Example I: Postgres data source*

```
jdbc.driver=org.postgresql.Driver
jdbc.propertiesFile=
jdbc.url=jdbc:postgresql://<myserver>:5432/mydb?user=<name>&password=<password>
db2.default.truncate.size=
```

*Example II: Presto connection with SSL setup*

```
jdbc.driver=com.facebook.presto.jdbc.PrestoDriver
jdbc.propertiesFile=/u/<username>/connections/presto-jdbc.properties
jdbc.url=jdbc:presto://<myserver>:8443
db2.default.table.name=
db2.default.truncate.size=1024
```

presto-jdbc.properties file content

```
SSL=true
SSLKeyStorePath=/u/<username>/connections/truststore.jks
SSLKeyStorePassword=<keystore-password>
user=<username>
password=<password>
```

### Run the stored procedure to access any JDBC data source

You made it here, congratulations! Everything is now setup and ready to run. The last step is to call the data bridge stored procedure. This can be done from any program that is capable to call a stored procedure. 

*Example in java*

```
CallableStatement pstmt = con.prepareCall("CALL <schema>.DATA_BRIDGE(?,?,?,?,?)");
pstmt.setString(1, "/u/<username>/connections/presto-connection.properties");
pstmt.setString(2, "select * from iceberg_data.tcph.lineitem LIMIT 10");	
pstmt.setString(3, "DATABRIDGE");		
pstmt.registerOutParameter(4, java.sql.Types.INTEGER);
pstmt.registerOutParameter(5, java.sql.Types.VARCHAR);
pstmt.execute();
```

After the stored procedure is called, you can access the data either direct via the stored procedure result set or via the temporary table name SESSION.<tableName> (for the above example <tableName> is "DATABRIDGE"). The temporary table name can be used to join the table with other tables in Db2 for z/OS in a subsequent SQL statement. Remember to turn off any auto-commit in this case, as declared temporary tables only exists within the commit scope.  

#### Restrictions 

* The source table cannot be bigger than the maximum table size in Db2 for z/OS. See [Db2 for z/OS limits](https://www.ibm.com/docs/en/db2-for-zos/12?topic=sql-limits-in-db2-zos).
* Some data types are not supported (See SQLStatementBuilder.java). 
* Restrictions for temporary tables in Db2 for z/OS apply (e.g. no CLOB support). 
* Not for all data types a 1:1 mapping exists. In those cases, modify the query to map it to a Db2 for z/OS supported data type, or use the built-in truncate method. 
* Table and column names are only allowed to contain quotes or semicolon. In addition Db2 for z/OS table and column restrictions apply. 

### How to diagnose issues

Use the LOG_OUTPUT parameter of the stored procedure to diagnose any issues. It will show an exception in case of issues. Analyze the exception to get to the root cause. 
* For Db2 for z/OS issues check the SQLCODE (See [Db2 for z/OS SQLCODES](https://www.ibm.com/docs/en/db2-for-zos/12?topic=codes-sql)). 
* For JDBC data source issues, check the documentation of the corresponding product.
 
## Epilog

Have fun with the access to the JDBC data sources and do something creative with it!

