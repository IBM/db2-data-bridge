/**
 * Copyright contributors to the db2-data-bridge project
 */
package com.ibm.databridge;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

public class Db2DataBridge {
			
	/**
	 * Establish a JDCB connection to the source database and returns the corresponding Connection object. 
	 * 
	 * @param configurationProperties connection properties (url, passwords, encryption, etc) 
	 * @return the connection object of the established connection
	 * @throws FileNotFoundException if the properties file could not be found 
	 * @throws IOException if the properties file could not be processed
	 * @throws ClassNotFoundException if the JDBC driver doesn't exist
	 * @throws SQLException if the connection to the source database couldn't be established
	 */
	public static Connection establishConnectionToSourceDatabase(java.util.Properties configurationProperties) throws FileNotFoundException, IOException, ClassNotFoundException, SQLException {
		
		Connection sourceConnection = null;
				
		//Load Source database JDBC Driver
		Class.forName(configurationProperties.getProperty("jdbc.driver"));
			
		//Get properties for source JDBC Driver
		var sourceDatabaseProperties = new java.util.Properties();	
		var jdbcPropertiesFileName = configurationProperties.getProperty("jdbc.propertiesFile");
		if (jdbcPropertiesFileName != null && !jdbcPropertiesFileName.isBlank()) {
			InputStream jdbcConfigFile = new FileInputStream(jdbcPropertiesFileName);
			sourceDatabaseProperties.load(jdbcConfigFile);
		}
				
		sourceConnection = DriverManager.getConnection(configurationProperties.getProperty("jdbc.url"), sourceDatabaseProperties); 	

		return sourceConnection;
	}

	/**
	 * Entry point for the stored procedure.
	 * 
	 * @param parmInputConfigurationPropertiesFilename configuration for the stored procedure
	 * @param parmInputQuery the query to run on the source system
	 * @param parmTableName the name of the table to put the data to
	 * @param parmOutputLog logging information of the SP run
	 * @param parmOutputResultSet the result set being returned by the stored procedure
	 */
	public static void queryData(String parmInputConfigurationPropertiesFilename, java.sql.Clob parmInputQuery, String parmTableName, int[] parmOutputReturnCode, String[] parmOutputLog, java.sql.ResultSet[] parmOutputResultSet) {
		
		var logger = new Logger(); 
		
		try {		
			logger.addLogEntry("Program started.");

			//Check stored procedure parameters
			if (parmInputQuery.length() > Integer.MAX_VALUE) {
				throw new Exception("Query text too long.");
			}

			//Process properties file
			InputStream configurationPropertiesFile = new FileInputStream(parmInputConfigurationPropertiesFilename);	
			var configurationProperties = new java.util.Properties();	
			configurationProperties.load(configurationPropertiesFile);
			
			logger.addLogEntry("Configuration file read."); 
	
			if (configurationProperties.getProperty("databridge.diagnostics.showPropertiesSettingsInLogOutput", "false").equalsIgnoreCase("true")) {
				logger.addLogEntry("=== Settings from config file:");
				configurationProperties.forEach((k, v) -> logger.addLogEntry(k + "=" + v));
				logger.addLogEntry("=== End of settings.");	
			}

			var truncatePropertySize = Integer.parseInt(configurationProperties.getProperty("db2.default.truncate.size", "0"));
			var insertBatchBufferSize = Integer.parseInt(configurationProperties.getProperty("databridge.insert.batch.buffer.size", "1000"));
			
			//Start the real work
			var sqlStatementBuilder = new SQLStatementBuilder(parmTableName, truncatePropertySize);
			var sourceConnection = establishConnectionToSourceDatabase(configurationProperties);
			
			logger.addLogEntry("Connection to source database established.");
			
			var targetConnection = DriverManager.getConnection("jdbc:default:connection"); 
			
			logger.addLogEntry("Connection to Db2 for z/OS established.");
			
			var sourceQueryStatement = sourceConnection.createStatement();		
	
			logger.addLogEntry("Run query.");
			
			var sourceDataResultSet = sourceQueryStatement.executeQuery(parmInputQuery.getSubString(1, (int) parmInputQuery.length()));
			
			logger.addLogEntry("Query executed.");
						
			var createTableStatementText = sqlStatementBuilder.buildCreateTableStatementFromResultSet(sourceDataResultSet);
			var insertStatement = sqlStatementBuilder.buildInsertStatementFromResultSet(sourceDataResultSet);
			
			var targetStatement = targetConnection.createStatement();
			
			logger.addLogEntry("Create temporary table in Db2 for z/OS.");
			logger.addLogEntry("SQL: " + createTableStatementText);
			
			targetStatement.execute(createTableStatementText);
			
			logger.addLogEntry("Insert data into temporary table.");
			logger.addLogEntry("SQL: " + insertStatement);
			
			var numRowsProcessed = 0L;
			var insertIntoTarget = targetConnection.prepareStatement(insertStatement);	
			//copy the data in batches 
			while (sourceDataResultSet.next()) {
				for (var columnNumberToProcess = 1; columnNumberToProcess <= sourceDataResultSet.getMetaData().getColumnCount(); columnNumberToProcess++) {
					insertIntoTarget.setObject(columnNumberToProcess, sourceDataResultSet.getObject(columnNumberToProcess));
				}	
				insertIntoTarget.addBatch();
				numRowsProcessed++; 
				if ((numRowsProcessed % insertBatchBufferSize) == 0) {
					insertIntoTarget.executeBatch();
				}
			}
			if ((numRowsProcessed % insertBatchBufferSize) != 0) {
				insertIntoTarget.executeBatch();
			}
			
			logger.addLogEntry("All data copied.");
			logger.addLogEntry("Return result set (Open Cursor).");
			parmOutputResultSet[0] = targetStatement.executeQuery(sqlStatementBuilder.buildSelectStatementForStoredProcedureResultSet());
			parmOutputReturnCode[0] = 0;
						
		} catch (Exception e) {			
			logger.addLogEntry("ERROR - Exception hit!");
			logger.addLogEntry(e.toString());
			parmOutputReturnCode[0] = 8;
		}
		//return log info as SP output parameter
		parmOutputLog[0] = logger.getLog();
	}	
	
}
