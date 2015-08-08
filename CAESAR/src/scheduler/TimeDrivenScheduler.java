package scheduler;

import java.util.HashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicInteger;
import run.*;
import distributor.*;
import iogenerator.*;

/**
 * As soon as all events with the same time stamp become available,
 * time driven scheduler submits them for execution.
 * @author Olga Poppe 
 */
public class TimeDrivenScheduler extends Scheduler implements Runnable {
	
	int sec;
	boolean splitQueries;
	
	boolean event_derivation_omission;
	boolean early_mandatory_projections;
	boolean early_condensed_filtering;
	boolean reduced_stream_history_traversal;
			
	public TimeDrivenScheduler (boolean sq, AtomicInteger dp, HashMap<RunID,Run> rs, EventQueues rq, ExecutorService e, 
			CountDownLatch tn, CountDownLatch d, int maxX, boolean bothD, int lastS, long start, AtomicDouble met,
			boolean ed, boolean pr, boolean fi, boolean sh) {	
		
		super(dp,rs,rq,e,tn,d,maxX,bothD,lastS,start,met);
		
		sec = 0;
		splitQueries = sq;
		
		event_derivation_omission = ed;
		early_mandatory_projections = pr;
		early_condensed_filtering = fi;
		reduced_stream_history_traversal = sh;		
	}
	
	/**
	 * As long as not all events are processed, iterate over all run task queues and pick tasks to execute in round-robin manner.
	 */	
	public void run() {	
		
		// Local variables
		double curr_sec = -1;
				
		// Get the permission to schedule current second
		while (curr_sec <= lastSec && eventqueues.getDistributorProgress(curr_sec, startOfSimulation, accidentWarningsFailed, tollNotificationsFailed)) {
		
			try {
				// Schedule the current second
				all_queries_all_runs (splitQueries, curr_sec, false, false,
					event_derivation_omission, early_mandatory_projections, early_condensed_filtering, reduced_stream_history_traversal);
				//one_query_all_runs_wrapper(curr_sec, 1, false, false); // 1 query, 1 queue for QDS testing				
					
				/*** If the stream is over, wait for acknowledgment of the previous transactions and terminate ***/				
				if (curr_sec == lastSec) {	
					transaction_number.await();						
					done.countDown();					
				} 
				curr_sec++;	
				
			} catch (final InterruptedException e) { e.printStackTrace(); }
		}		
		System.out.println("Scheduler is done.");
	}	
}