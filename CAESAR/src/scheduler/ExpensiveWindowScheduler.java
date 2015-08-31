package scheduler;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicInteger;
import run.*;
import transaction.*;
import distributor.*;
import event.*;
import iogenerator.*;

public class ExpensiveWindowScheduler extends Scheduler implements Runnable {
	
	int expensive_window_length;
	int window_number;
	int query_number;
	
	public ExpensiveWindowScheduler (int max_xway, boolean both_dirs, int lastSec,
			HashMap<RunID,Run> runs, EventQueues eventqueues, ExecutorService executor, 
			AtomicInteger distrProgr, HashMap<Double,Double> distrFinishT, HashMap<Double,Double> schedStartT, CountDownLatch trans_numb, CountDownLatch done,  
			long start, boolean opt, AtomicDouble max_exe_time,
			int wl, int wn, int qn) {	
		
		super(max_xway, both_dirs, lastSec, runs, eventqueues, executor, distrProgr, distrFinishT, schedStartT, trans_numb, done, start, opt, max_exe_time);
		
		expensive_window_length = wl;
		window_number = wn;
		query_number = qn;
	}
	
	/**
	 * As long as not all events are processed, iterate over all event queues and pick transactions to execute in round-robin manner.
	 */	
	public void run() {	
		
		double curr_sec = -1;
		boolean execute = (window_number > 1) ? false : true;
		double cheap_window_length = Math.ceil(lastSec/(window_number+1) - expensive_window_length);
		double window_bound = cheap_window_length;
		//double total_window_count = 0;
		double expensive_window_count = 0;
						
		/*** Get the permission to schedule current second ***/
		while (curr_sec <= lastSec && eventqueues.getDistributorProgress(curr_sec, startOfSimulation)) {
		
			try {
				/*** Set scheduler start time for the current second ***/
				double now = (System.currentTimeMillis() - startOfSimulation)/new Double(1000);
				schedStartTimes.put(curr_sec, now);				
				//System.out.println("Scheduling time of second " + curr_sec + " is " + now);
				
				/*********************************************************************************************************************************************/
				/*** Update window bound, window count and execute ***/
				if (window_number > 1 && curr_sec > window_bound) {			
					
					//total_window_count++;
					
					if (execute || expensive_window_count == window_number) {
						execute = false;
						window_bound += cheap_window_length;
					} else {
						//if (total_window_count % window_number == 0) {
							execute = true;
							window_bound += expensive_window_length;
							expensive_window_count++;
					}//}						
					System.out.println("Window: " + curr_sec + "-"  + window_bound + " Execute: " + execute);
				}
				/*********************************************************************************************************************************************/
				/*** Schedule the current second or drop events with this time stamp ***/
				all_queries_all_runs(curr_sec, execute, query_number);
									
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
	
	/*** 
	 * Get all transactions with time stamp sec and submit them for execution
	 * @param sec			time stamp
	 * @param execute		true if events are executed and false if they are dropped
	 * @param query_number	number of query replications
	 * @return				number of transactions submitted for execution
	 */	
	public int all_queries_all_runs (double sec, boolean execute, int query_number) {
		
		/*** Get transactions to schedule ***/
		ArrayList<Transaction> transactions = one_query_all_runs(sec, execute, query_number);
		int number = transactions.size();
		
		/*** Wait for the previous transactions to complete ***/
		try { transaction_number.await(); } 
		catch (final InterruptedException e) { e.printStackTrace(); }
					
		/*** Schedule all transactions at current second ***/
		transaction_number = new CountDownLatch(number);			
		for (Transaction t : transactions) { 				
			t.transaction_number = transaction_number;
			executor.execute(t); 
		}
		return number;
	}
	
	/**
	 * Iterate over all event queues and generate transactions with time stamp sec
	 * @param sec	transaction time stamp			
	 */
	public ArrayList<Transaction> one_query_all_runs (double sec, boolean execute, int query_number) {
		
		ArrayList<Transaction> transactions = new ArrayList<Transaction>();		
				
		for (int xway=0; xway<=max_xway; xway++) {
			
			for (double seg=0; seg<=99; seg++) {
				
				RunID runid0 = new RunID(xway,0,seg);
				Transaction t0 = one_query_one_run(sec, runid0, execute, query_number);
				if (t0!=null) transactions.add(t0);	
								
				if (xway != max_xway || both_dirs) {
					
					RunID runid1 = new RunID(xway,1,seg); 
					Transaction t1 = one_query_one_run(sec, runid1, execute, query_number);
					if (t1!=null) transactions.add(t1);
		}}}
		return transactions;
	}	
	
	/**
	 * Wraps the processing of all events which have the given time stamp and  
	 * are relevant for the same run into one transaction 
	 * @param sec	transaction time stamp
	 * @param runid	identifier of the run the events of which are scheduled
	 */
	public Transaction one_query_one_run (double sec, RunID runid, boolean execute, int query_number) {
		
		if (eventqueues.contents.containsKey(runid)) {
			
			ConcurrentLinkedQueue<PositionReport> eventqueue = eventqueues.contents.get(runid);	
			
			if (eventqueue!=null && !eventqueue.isEmpty()) {			
				
				ArrayList<PositionReport> event_list = new ArrayList<PositionReport>();					
					
				PositionReport event = eventqueue.peek();
				while (event!=null && event.sec==sec) { 				
					eventqueue.poll();
					event.schedulerTime = (System.currentTimeMillis() - startOfSimulation)/new Double(1000);
					if (execute) { event_list.add(event); }				
					event = eventqueue.peek();
				}
				
				/*** If the event list is not empty, generate a transaction ***/
				if (!event_list.isEmpty()) {						
					Run run = runs.get(runid);
					return new TrafficManagement (run, event_list, runs, startOfSimulation, distrFinishTimes, schedStartTimes, max_exe_time, accidentWarningsFailed, tollNotificationsFailed, query_number);
		}}}
		return null;
	}
}
