package scheduler;

import java.util.HashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicInteger;
import run.*;
import distributor.*;
import iogenerator.*;

public class ExpensiveWindowScheduler extends Scheduler implements Runnable {
	
	int window_length;
	int window_number;
	int query_number;
	
	public ExpensiveWindowScheduler (int max_xway, boolean both_dirs, int lastSec,
			HashMap<RunID,Run> runs, EventQueues eventqueues, ExecutorService executor, 
			AtomicInteger distrProgr, HashMap<Double,Double> distrFinishT, HashMap<Double,Double> schedStartT, CountDownLatch trans_numb, CountDownLatch done,  
			long start, boolean opt, AtomicDouble max_exe_time,
			int wl, int wn, int qn) {	
		
		super(max_xway, both_dirs, lastSec, runs, eventqueues, executor, distrProgr, distrFinishT, schedStartT, trans_numb, done, start, opt, max_exe_time);
		
		window_length = wl;
		window_number = wn;
		query_number = qn;
	}
	
	/**
	 * As long as not all events are processed, iterate over all run task queues and pick tasks to execute in round-robin manner.
	 */	
	public void run() {	
		
		double curr_sec = -1;
		boolean execute = false;
		double window_bound = window_length;
		double window_count = 1;
						
		/*** Get the permission to schedule current second ***/
		while (curr_sec <= lastSec && eventqueues.getDistributorProgress(curr_sec, startOfSimulation)) {
		
			try {
				/*** Set scheduler start time for the current second ***/
				double now = (System.currentTimeMillis() - startOfSimulation)/new Double(1000);
				schedStartTimes.put(curr_sec, now);
				
				//System.out.println("Scheduling time of second " + curr_sec + " is " + now);
				
				/*** Update window bound, window count and execute ***/
				if (curr_sec>window_bound) {			
					
					window_bound += window_length;
					window_count++;
					
					if (execute) {
						execute = false;
					} else {
						if (window_count % window_number == 0) {
							execute = true;
					}}						
					System.out.println("Current second: " + curr_sec + " Window " + window_count + " with bound: " + window_bound + " Execute: " + execute);
				}
				/*** Schedule the current second or drop events with this time stamp ***/
				for (int i=1; i<query_number; i++) {
					all_queries_all_runs(curr_sec, execute, true);
				}											
				all_queries_all_runs(curr_sec, execute, false);
									
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
