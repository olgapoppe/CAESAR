package scheduler;

import java.util.HashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicInteger;
import run.*;
import distributor.*;

/**
 * As soon as all events with the same time stamp become available,
 * time driven scheduler submits them for execution.
 * @author Olga Poppe 
 */
public class TimeDrivenScheduler extends Scheduler implements Runnable {		
			
	TimeDrivenScheduler (AtomicInteger dp, HashMap<RunID,Run> rs, RunQueues rq, ExecutorService e, 
						 CountDownLatch tn, CountDownLatch d, int last, long start) {		
		super(dp,rs,rq,e,tn,d,last,start);
	}
	
	/**
	 * As long as not all events are processed, iterate over all run task queues and pick tasks to execute in round-robin manner.
	 */	
	public void run() {	
		
		/*** Local variables ***/
		double curr_sec = -1;
							
		while (!shutdown) {
			try {				 
				/*** If events with new time stamp are available, schedule their processing after the previous transactions are acknowledged ***/
				if (runqueues.get(curr_sec)) { 
					
					curr_sec++;
					
					System.out.println("Scheduler: " + curr_sec);
					
					all_queries_all_runs (curr_sec, false, false);	// 2 waitings
					
					//one_query_all_runs_wrapper(curr_sec, 1, false, false); // 1 waiting, 1 query, 1 queue for QDS testing
					
				} 
				/*** If the stream is over, wait for acknowledgment of the previous transactions and sleep ***/					
				if (curr_sec == lastSec) {
						
					transaction_number.await();						
					done.countDown();												
				}						
			} catch (final InterruptedException ex) { ex.printStackTrace(); }
		}
		
		System.out.println("scheduler done");
	}	
}