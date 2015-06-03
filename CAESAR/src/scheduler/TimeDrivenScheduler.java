package scheduler;

import java.util.ArrayList;
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
			
	public TimeDrivenScheduler (AtomicInteger dp, HashMap<RunID,Run> rs, RunQueues rq, ExecutorService e, 
			CountDownLatch tn, CountDownLatch d, ArrayList<XwayDirPair> xds, int lastS, long start) {		
		super(dp,rs,rq,e,tn,d,xds,lastS,start);
		sec = 0;
	}
	
	/**
	 * As long as not all events are processed, iterate over all run task queues and pick tasks to execute in round-robin manner.
	 */	
	public void run() {	
		
		// Local variables
		double curr_sec = -1;
		
		// Get the permission to schedule current second
		while (curr_sec <= lastSec && runqueues.getDistributorProgress(curr_sec)) {  
			try {	
				// Schedule the current second
				all_queries_all_runs (curr_sec, false, false);
				//one_query_all_runs_wrapper(curr_sec, 1, false, false); // 1 query, 1 queue for QDS testing
				
				// Output the current progress every 5 min 
				if (curr_sec == sec+300) {
					System.out.println("Scheduler: " + curr_sec);
					sec += 300;
				}			 
				/*** If the stream is over, wait for acknowledgment of the previous transactions and terminate ***/					
				if (curr_sec == lastSec) {						
					transaction_number.await();						
					done.countDown();												
				} 
				curr_sec++;
				
			} catch (final InterruptedException ex) { ex.printStackTrace(); }
		}		
		System.out.println("Scheduler is done.");
	}	
}