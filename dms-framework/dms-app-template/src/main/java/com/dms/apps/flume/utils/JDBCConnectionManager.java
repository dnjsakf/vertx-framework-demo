package com.dms.apps.flume.utils;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

import org.apache.flume.Context;
import org.apache.flume.conf.Configurable;
import org.apache.flume.instrumentation.SinkCounter;

import com.google.common.base.Preconditions;

import lombok.extern.slf4j.Slf4j;

@Slf4j
public class JDBCConnectionManager implements Configurable {

	private static final int CONNECTION_VALID_TIMEOUT = 500;
	private String driver;
	private String url;
	private String username;
	private String password;
	private boolean autoCommit;
	private Connection connection;
	private SinkCounter counter;

	public JDBCConnectionManager() {}
	public JDBCConnectionManager(final SinkCounter counter) {
		this.counter = counter;
	}
	
	public void configure(final Context context) {
		driver = context.getString("driver");
		Preconditions.checkNotNull(driver, "Driver must be specified.");
		url = context.getString("url");
		Preconditions.checkNotNull(url, "URL must be specified.");
		username = context.getString("username");
		Preconditions.checkNotNull(username, "User must be specified.");
		password = context.getString("password");
		Preconditions.checkNotNull(password, "Driver must be specified.");
		autoCommit = context.getBoolean("auto-commit", false);
		Preconditions.checkNotNull(autoCommit, "AUto-Commit must be specified.");
	}

	public Connection getConnection() {
		return connection;
	}

	/**
	 * Start the connection manager: load the driver and create a connection.
	 * Note that the sink counter must have been started before calling this
	 * method.
	 */
	public void start() {
		try {
			Class.forName(driver);
			createConnection();
		} catch (final Exception e) {
			log.error("Unable to create JDBC connection to {}.", url, e);
			if( counter != null ){
	            counter.incrementConnectionFailedCount();
			}
			closeConnection();
			throw new IllegalArgumentException(e);
		}
	}

	/**
	 * Ensure that the current JDBC connection is valid. If it's not, create
	 * one.
	 * 
	 * @throws SQLException
	 * @throws ClassNotFoundException
	 */
	public void ensureConnectionValid() throws SQLException, ClassNotFoundException {
		log.debug("Testing JDBC connection validity to: {}.", url);
		if (connection == null || !connection.isValid(CONNECTION_VALID_TIMEOUT)) {
			closeConnection();
			createConnection();
		}
	}

	/**
	 * Closes the current JDBC connection.
	 */
	public void closeConnection() {
		if (connection != null) {
			log.debug("Closing JDBC connection to: {}.", url);

			try {
				connection.close();
			} catch (final SQLException e) {
				log.warn("Unable to close JDBC connection to {}.", url, e);
			}

			connection = null;
            if( counter != null ){
                counter.incrementConnectionClosedCount();
            }
		}
	}

	private void createConnection() throws ClassNotFoundException, SQLException {
		log.debug("Creating JDBC connection to: {}.", url);
		connection = DriverManager.getConnection(url, username, password);
		//connection.setAutoCommit(true);
		connection.setAutoCommit(autoCommit);
        if( counter != null ){
            counter.incrementConnectionCreatedCount();
        }
	}

}