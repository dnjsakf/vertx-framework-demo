package com.dms.apps;

import org.apache.flume.Channel;
import org.apache.flume.Context;
import org.apache.flume.CounterGroup;
import org.apache.flume.Event;
import org.apache.flume.EventDeliveryException;
import org.apache.flume.Transaction;
import org.apache.flume.conf.Configurable;
import org.apache.flume.instrumentation.SinkCounter;
import org.apache.flume.sink.AbstractSink;

import lombok.extern.slf4j.Slf4j;
import scala.collection.script.Update;

@Slf4j
public class FlumeCustomSink extends AbstractSink implements Configurable {

	private CounterGroup counterGroup;
	private SinkCounter counter;

    @Override
    public Status process() throws EventDeliveryException {
        Status status = Status.READY;
        
		if( Status.READY == status ) {
			final Channel channel = getChannel();
			final Transaction channelTransaction = channel.getTransaction();

            try {
				channelTransaction.begin();
                
                final Event event = channel.take();
                
				byte[] body = event.getBody();

                log.info("Sink!!! {}", new String(body));

                status = updateAttemptCounters(0);
                
				channelTransaction.commit();
				
				// Update success counters.
				updateSuccessCounters(1);

            } catch ( UnsupportedOperationException e ){
				channelTransaction.rollback();
                throw new UnsupportedOperationException("Unimplemented method 'process'");
            } catch ( Exception e ){
				channelTransaction.rollback();
            }

        }

        return status;
    }

    @Override
    public void configure(Context context) {
		if (counter == null) {
			counter = new SinkCounter(getName());
		}
		if (counterGroup == null) {
			counterGroup = new CounterGroup();
		}
    }


	private Status updateAttemptCounters(final int count) {
		counter.addToEventDrainAttemptCount(count);

		if (count == 0) {
			counter.incrementBatchEmptyCount();
			counterGroup.incrementAndGet("channel.underflow");
			return Status.BACKOFF;
		}

		// Else, count <= batchSize and the batch is full.
		counter.incrementBatchCompleteCount();
		return Status.READY;
	}
    
	private void updateSuccessCounters(final int count) {
		counter.addToEventDrainSuccessCount(count);
		counterGroup.incrementAndGet("transaction.success");
	}

}