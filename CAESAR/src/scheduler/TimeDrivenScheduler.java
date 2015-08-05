package scheduler;

import java.util.HashMap;
import java.util.Random;
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
			
	public TimeDrivenScheduler (boolean sq, AtomicInteger dp, HashMap<Double,Long> distrProgrPerSec, HashMap<RunID,Run> rs, EventQueues rq, ExecutorService e, 
			CountDownLatch tn, CountDownLatch d, int maxX, boolean bothD, int lastS, long start, AtomicDouble met,
			boolean ed, boolean pr, boolean fi, boolean sh) {	
		
		super(dp,distrProgrPerSec,rs,rq,e,tn,d,maxX,bothD,lastS,start,met);
		
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
		long curr_app_time = 0;
		double scheduler_wakeup_time = 0;
		
		Random random = new Random();
		int min = 6;
		int max = 12;
		
		int batch_limit = random.nextInt(max - min + 1) + min;
		if (batch_limit > lastSec) batch_limit = lastSec;	
		//System.out.println("Scheduler's batch limit: " + batch_limit);
		
		try {
		
			// Get the permission to schedule current second
			while (curr_sec <= lastSec && runqueues.getDistributorProgress(curr_sec, startOfSimulation, accidentWarningsFailed, tollNotificationsFailed)) {
						
				if (curr_sec <= batch_limit) {
					
					//System.out.println("Scheduler schedules second: " + curr_sec);
			
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
					
				} else {
				
					/*** Sleep if the current batch was read ahead of current application time ***/
					curr_app_time = System.currentTimeMillis() - startOfSimulation;
					
					if (curr_app_time < batch_limit*1000) {
		 			
						int sleep_time = new Double(batch_limit*1000 - curr_app_time).intValue();		 			
						Thread.sleep(sleep_time);						
						scheduler_wakeup_time = (System.currentTimeMillis() - startOfSimulation)/1000 - batch_limit;						
					} else {
						scheduler_wakeup_time = 0;
					}				
 					
					/*** Rest the batch limit ***/
					int old_batch_limit = batch_limit;
					batch_limit += random.nextInt(max - min + 1) + min + scheduler_wakeup_time;		 			
					if (batch_limit > lastSec) batch_limit = lastSec;		
					if (scheduler_wakeup_time>1) 
						System.out.println("Scheduler wakeup time is " + scheduler_wakeup_time + 
											". Batch limit increases from " + old_batch_limit +
											" to " + batch_limit + ".");
				}												
			}			
		} catch (final InterruptedException e) { e.printStackTrace(); }
		
		System.out.println("Scheduler is done.");
	}	
}