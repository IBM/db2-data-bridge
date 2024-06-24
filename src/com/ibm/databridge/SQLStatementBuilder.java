/**
 * Copyright contributors to the db2-data-bridge project
 */
package com.ibm.databridge;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.regex.Pattern;

public class SQLStatementBuilder {

	private String mTableName;
	private int mTruncateSize;
	
	public SQLStatementBuilder(String tableName, int truncateSize) throws Exception {		
		mTableName = validateSQLInput(tableName);
		mTruncateSize = truncateSize;
	}
	
	/**
	 * Validates that the input does not contain quotes or semicolon. Will throw an exception if a quote or semicolon is found. 
	 * @param text the string to check 
	 * @return the text (unprocessed)
	 * @throws Exception in case any other character is used
	 */
	private String validateSQLInput(String text) throws Exception {
		
		String anyCharactersExceptQuotesAndSemicolon = "[^'\";]+";	
		if (!Pattern.matches(anyCharactersExceptQuotesAndSemicolon, text)) {
			throw new Exception("Table or column name \"" + text + "\" contains quotes or semicolon.");
		}
		return text;
	}
	
	/**
	 * Checks if the precision is bigger than the maximum possible length of the column. 
	 * If truncation is enabled, set it to the truncated size. 
	 * @param precision
	 * @return
	 */
	private int limitPercisionIfConfigured(int precision) {
		return ((precision > 32672) && (mTruncateSize > 0)) ? mTruncateSize : precision;
	}
	
	/**
	 * Builds a CREATE TABLE statement with the same signature as the result set, so that the content of the result set could be put into that table.  
	 * 
	 * @param resultSet The result set used to extract the signature 
	 * @return A CREATE TABLE statement as String
	 * @throws SQLException in case the result set could not be accessed
	 * @throws Exception in case a data type is not supported or any column name contains invalid characters
	 */
	public String buildCreateTableStatementFromResultSet(ResultSet resultSet) throws Exception {	
		String result = "DECLARE GLOBAL TEMPORARY TABLE SESSION.\"" + mTableName + "\"(";	
		var resultSetMetaData = resultSet.getMetaData();
		for (var columnNumberToProcess = 1; columnNumberToProcess <= resultSetMetaData.getColumnCount(); columnNumberToProcess++) {					
			if (columnNumberToProcess > 1) {
				result += ", ";
			}
			result += "\"" + validateSQLInput(resultSetMetaData.getColumnName(columnNumberToProcess)) + "\" "; 
			
			int precision = resultSetMetaData.getPrecision(columnNumberToProcess);
			int scale = resultSetMetaData.getScale(columnNumberToProcess);
			
			switch(resultSetMetaData.getColumnType(columnNumberToProcess)) {
				case java.sql.Types.BIGINT:
					result += "BIGINT";
					break;
				case java.sql.Types.BINARY:
					result += "BINARY(" + limitPercisionIfConfigured(precision) + ")";
				case java.sql.Types.CHAR:
					result += "CHAR(" + limitPercisionIfConfigured(precision) + ")";
					break;
				case java.sql.Types.DATE:
					result += "DATE";
					break;
				case java.sql.Types.DECIMAL:
					result += "DECIMAL(" + precision + "," + scale + ")";
					break;
				case java.sql.Types.DOUBLE:
					result += "DOUBLE";
					break;
				case java.sql.Types.FLOAT:
					result += "FLOAT(" + precision + "," + scale + ")";
					break;
				case java.sql.Types.INTEGER:
					result += "INT";
					break;
				case java.sql.Types.NUMERIC:
					result += "NUMERIC(" + precision + "," + scale + ")";
					break;
				case java.sql.Types.REAL:
					result += "REAL";
					break;
				case java.sql.Types.ROWID:
					result += "ROWID";
					break;
				case java.sql.Types.SMALLINT:
					result += "SMALLINT";
					break;
				case java.sql.Types.TIME:
					result += "TIME";
					break;
				case java.sql.Types.TIME_WITH_TIMEZONE:
					result += "TIME WITH TIMEZONE";
					break;
				case java.sql.Types.TIMESTAMP:
					result += "TIMESTAMP";
					break;
				case java.sql.Types.TIMESTAMP_WITH_TIMEZONE:
					result += "TIMESTAMP WITH TIMEZONE";
					break;
				case java.sql.Types.TINYINT:
					result += "SMALLINT";
					break;
				case java.sql.Types.VARBINARY:
					result += "VARBINARY(" + limitPercisionIfConfigured(precision) + ")";
					break;
				case java.sql.Types.VARCHAR:
					result += "VARCHAR(" + limitPercisionIfConfigured(precision) + ")";
					break;
				default:
					throw new Exception("Data type " + resultSetMetaData.getColumnTypeName(columnNumberToProcess) + " not supported"); 
			}
		}
		result += ") CCSID UNICODE NOT LOGGED";
		return result;
	}
	
	
	/**
	 * Builds an insert statement with the same signature as the result set, so that it could be used to move the data from the result set into a table with the same signature. 
	 * 
	 * @param resultSet The result set used to extract the signature 
	 * @return An insert statement as String
	 * @throws SQLException  in case the result set could not be accessed
	 */
	public String buildInsertStatementFromResultSet(ResultSet resultSet) throws SQLException, Exception {	
		String result = "INSERT INTO SESSION.\"" + mTableName + "\" VALUES(";	
		// Process source table meta data to build statements for insertion of data
		for (var columnNumberToProcess = 1; columnNumberToProcess <= resultSet.getMetaData().getColumnCount(); columnNumberToProcess++) {					
			if (columnNumberToProcess > 1) {
				result += ", ";
			}
			result += "?";
		}
		result += ")";
		return result;
	}	
	
	/**
	 * Builds a SELECT statement, which returns the data from the temporary table. 
	 * 
	 * @return The select statement
	 */
	public String buildSelectStatementForStoredProcedureResultSet() {	
		return "SELECT * FROM SESSION." + mTableName;
	}	

}
