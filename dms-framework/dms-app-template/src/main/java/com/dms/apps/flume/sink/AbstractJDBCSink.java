package com.dms.apps.flume.sink;

import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;

import org.apache.flume.Channel;
import org.apache.flume.Context;
import org.apache.flume.CounterGroup;
import org.apache.flume.Event;
import org.apache.flume.EventDeliveryException;
import org.apache.flume.Transaction;
import org.apache.flume.conf.Configurable;
import org.apache.flume.instrumentation.SinkCounter;
import org.apache.flume.sink.AbstractSink;

import com.dms.apps.flume.utils.JDBCConnectionManager;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import com.google.gson.JsonSyntaxException;

import lombok.extern.slf4j.Slf4j;

@Slf4j
public abstract class AbstractJDBCSink extends AbstractSink implements Configurable {

	private JsonParser jsonParser = new JsonParser();

	private CounterGroup counterGroup;
	private SinkCounter counter;

	private JDBCConnectionManager connectionManager;
	public PreparedStatement statement;
	private String prepareSQL = ""; /* Mapper에서 조회 */
	private int batchSize = 10000; /* 기본 batchSize */
	
	/**
	 * Statement 처리
	 * @param body
	 * @return
	 * @throws Exception
	 */
	// protected abstract int processJDBC(final byte[] body) throws Exception;
	protected abstract int processJDBC(final JsonObject body) throws Exception;

	/**
	 * Statement 인스턴스 생성
	 * @throws SQLException
	 */
	protected void prepareJDBC() throws SQLException {
		statement = getConnection().prepareStatement(prepareSQL);
	}

	/**
	 * Batch 실행
	 * @throws SQLException
	 */
	protected void completeJDBC() throws SQLException {
		if ( statement != null ) {
			statement.executeBatch();
		}
	}

	/**
	 * statement 닫기
	 */
	protected void abortJDBC() throws SQLException {
		if ( statement != null ) {
			statement.close();
		}
	}

	@Override
	public void configure(final Context context) {
		if (counter == null) {
			counter = new SinkCounter(getName());
		}
		if (counterGroup == null) {
			counterGroup = new CounterGroup();
		}

		context.put("driver", "org.postgresql.Driver");
		context.put("url", "jdbc:postgresql://localhost:5432/postgres");
		context.put("username", "dochi");
		context.put("password", "dochi123");

		connectionManager = new JDBCConnectionManager(counter);
		connectionManager.configure(context);

		prepareSQL = "INSERT INTO gw_data ( data, json_data, load_dttm ) VALUES ( ?, ?, current_timestamp )";
	}

	@Override
	public synchronized void start() {
		super.start();
		counter.start();
		connectionManager.start();
	}

	@Override
	public synchronized void stop() {
		// Channel channel = getChannel();
		// Transaction channelTransaction = channel.getTransaction();
		
		// try {
		// 	channelTransaction.begin();
			
		// 	Event event = channel.take();
			
		// 	channelTransaction.commit();
		// } catch(Exception e) {
		// 	e.printStackTrace();
		// 	channelTransaction.rollback();
		// } finally {
		// 	channelTransaction.close();
		// }
		connectionManager.closeConnection();
		counter.stop();
		super.stop();
	}


	@Override
	public Status process() throws EventDeliveryException {
		Status status = Status.READY;
		
		if( Status.READY == status ) {

			final Channel channel = getChannel();
			final Transaction channelTransaction = channel.getTransaction();
			
			try {
				// Start transactions, prepare for JDBC operations.
				channelTransaction.begin();
				connectionManager.ensureConnectionValid();
				
				
				byte[] body = null;

				String offset = null;
				int attemptCnt = 0;
				
				// Set Up
				prepareJDBC();
				while( attemptCnt < batchSize ) {
					final Event event = channel.take();
					if ( event == null ) {
						offset = null;
						break;
					}
					attemptCnt +=1;
					
					offset = event.getHeaders().get("offset");
					
					body = event.getBody();

					try {
						JsonObject jsonBody = jsonParser.parse(new String(body, StandardCharsets.UTF_8)).getAsJsonObject();
						statement.clearParameters();
						processJDBC(jsonBody);
						statement.addBatch();
					} catch ( JsonSyntaxException jse ){
						jse.printStackTrace();
					} catch ( Exception e ) {
					    e.printStackTrace();
					}
				}
				log.info("[{}] execute: {} / {}", getName(), attemptCnt, batchSize);
				// Clean up
				completeJDBC();
				
				// Update attempt counters.
				status = updateAttemptCounters(attemptCnt);
				
				// Commit.
				if( !connectionManager.getConnection().isClosed() && !connectionManager.getConnection().getAutoCommit() ) {
					connectionManager.getConnection().commit();
				}
				channelTransaction.commit();
				
				// Update success counters.
				updateSuccessCounters(attemptCnt);
				
			}  catch (Throwable e) {
				e.printStackTrace();

				try {
					abortJDBC();
					
					if( !connectionManager.getConnection().isClosed() && !connectionManager.getConnection().getAutoCommit() ) {
						connectionManager.getConnection().rollback();
					}
					channelTransaction.rollback();
					
				} catch (Exception e2) {
					log.error("Exception in rollback. Rollback might not have been successful.",e);
				} finally {
					updateFailureCounters();
				}
				
				status = Status.BACKOFF;

			} finally {
				channelTransaction.close();
			}
		}

		return status;
	}

	public Connection getConnection() {
		return connectionManager.getConnection();
	}

	private Status updateAttemptCounters(final int count) {
		counter.addToEventDrainAttemptCount(count);

		if (count == 0) {
			counter.incrementBatchEmptyCount();
			counterGroup.incrementAndGet("channel.underflow");
			return Status.BACKOFF;
		}

		if (count < batchSize) {
			counter.incrementBatchUnderflowCount();
			return Status.READY;
		}

		counter.incrementBatchCompleteCount();
		return Status.READY;
	}

	private void updateSuccessCounters(final int count) {
		counter.addToEventDrainSuccessCount(count);
		counterGroup.incrementAndGet("transaction.success");
	}

	private void updateFailureCounters() {
		counterGroup.incrementAndGet("transaction.rollback");
	}

}