/**
 * Copyright contributors to the db2-data-bridge project
 */
package com.ibm.databridge;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;

/**
 * Minimal log infrastructure
 */
public class Logger {

	private static Logger gLogger;
	
	private String mLogBuffer = "";
	
	public static Logger getInstance() {
		if (gLogger == null) {
			gLogger = new Logger();
		}
		return gLogger;
	}

	public void addLogEntry(String logEntry) {
		mLogBuffer += LocalDateTime.now().format(DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss")) + " : " + logEntry + System.lineSeparator();
	}
	
	public String getLog() {
		return mLogBuffer;
	}
	
}
