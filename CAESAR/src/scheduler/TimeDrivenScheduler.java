package scheduler;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicInteger;
import run.*;
import window.TimeInterval;
import distributor.*;

/**
 * As soon as all events with the same time stamp become available,
 * time driven scheduler submits them for execution.
 * @author Olga Poppe 
 */
public class TimeDrivenScheduler extends Scheduler implements Runnable {	
	
	ArrayList<TimeInterval> expensive_windows;
	int query_number;
			
	public TimeDrivenScheduler (int max_xway, boolean both_dirs, double lastSec,
			HashMap<RunID,Run> runs, EventQueues eventqueues, ExecutorService executor, 
			AtomicInteger distrProgr, HashMap<Double,Double> distrFinishT, HashMap<Double,Double> schedStartT, CountDownLatch trans_numb, CountDownLatch done,  
			long start, boolean opt, AtomicInteger total_exe_time, int q_number, ArrayList<TimeInterval> ew) {	
		
		super(max_xway, both_dirs, lastSec, runs, eventqueues, executor, distrProgr, distrFinishT, schedStartT, trans_numb, done, start, opt, total_exe_time);
		
		expensive_windows = ew;
		query_number = q_number;
	}
	
	/**
	 * As long as not all events are processed, iterate over all run task queues and pick tasks to execute in round-robin manner.
	 */	
	public void run() {	
		
		double curr_sec = -1;
				
		/*** Get the permission to schedule current second ***/
		while (curr_sec <= lastSec && eventqueues.getDistributorProgress(curr_sec)) {
		
			try {
				/*** Set scheduler start time for the current second ***/
				double now = (System.currentTimeMillis() - startOfSimulation)/new Double(1000);
				schedStartTimes.put(curr_sec, now);
				
				//System.out.println("Scheduling time of second " + curr_sec + " is " + now);
				
				/*** Find out query number ***/
				int number = -1;
				if (expensive_windows.isEmpty()) {
					number = query_number;
				} else {
					for (TimeInterval i : expensive_windows) {
						if (i.contains(curr_sec)) {
							number = i.query_number;
							break;
				}}}
				
				/*** Schedule the current second ***/
				if (number>-1) all_queries_all_runs(curr_sec, number);
									
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