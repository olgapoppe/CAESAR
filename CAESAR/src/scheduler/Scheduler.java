package scheduler;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import event.*;
import run.*;
import transaction.*;
import distributor.*;


/**
 * A scheduler iterates over run task queues, picks events from them and 
 * submits them to the thread pool for execution.
 * @author Olga Poppe 
 */
public abstract class Scheduler implements Runnable {
	
	int max_xway;
	boolean both_dirs;
	int firstSec;
	int lastSec;
		
	HashMap<RunID,Run> runs;
	public final EventQueues eventqueues;		
	ExecutorService executor;
	
	AtomicInteger distributorProgress;
	HashMap<Double,Double> distrFinishTimes;
	HashMap<Double,Double> schedStartTimes;
	CountDownLatch transaction_number;
	CountDownLatch done;	
	
	long startOfSimulation;
	boolean optimized;
	AtomicInteger total_exe_time;
	
	AtomicBoolean accidentWarningsFailed;
	AtomicBoolean tollNotificationsFailed;
		
	Scheduler (int max_x, boolean both_d, int lastS,
			HashMap<RunID,Run> rs, EventQueues evqueues, ExecutorService exe, 
			AtomicInteger distrProgr, HashMap<Double,Double> distrFinishT, HashMap<Double,Double> schedStartT, CountDownLatch trans_numb, CountDownLatch d,  
			long start, boolean opt, AtomicInteger total_exe) {
		
		max_xway = max_x;
		both_dirs = both_d;
		lastSec = lastS;	
				
		runs = rs;
		eventqueues = evqueues;		
		executor = exe;
		
		distributorProgress = distrProgr;
		distrFinishTimes = distrFinishT;
		schedStartTimes = schedStartT;
		transaction_number = trans_numb;
		done = d;
		
		startOfSimulation = start;
		optimized = opt;
		total_exe_time = total_exe;
		
		accidentWarningsFailed = new AtomicBoolean(false);
		tollNotificationsFailed = new AtomicBoolean(false);
	}	
	
	/*** 
	 * Get all transactions with time stamp sec and submit them for execution
	 * @param sec	time stamp
	 * @return		number of transactions submitted for execution
	 */	
	public int all_queries_all_runs (double sec) {
		
		// Get transactions to schedule
		ArrayList<Transaction> transactions = one_query_all_runs(sec);
		int number = transactions.size();
		
		try {
			// Wait for executor
			double startOfWaiting = (System.currentTimeMillis() - startOfSimulation)/new Double(1000);					
			transaction_number.await();			
			double endOfWaiting = (System.currentTimeMillis() - startOfSimulation)/new Double(1000);
			double durationOfWaiting = endOfWaiting - startOfWaiting;
			if (durationOfWaiting>1) 
				System.out.println(	"Scheduler waits from " + startOfWaiting + 
									" to " + endOfWaiting + 
									" for executor to processes second " + sec);
		} catch (final InterruptedException ex) { ex.printStackTrace(); }
			
		// Print out scheduler progress
		//double now = (System.currentTimeMillis() - startOfSimulation)/new Double(1000);
		//if (sec % 10 == 0) System.out.println("Scheduling time of second " + sec + " is " + now);
				
		// Schedule all transactions at current second
		transaction_number = new CountDownLatch(number);			
		for (Transaction t : transactions) { 				
			t.transaction_number = transaction_number;
			executor.execute(t); 
		}				
		return number;
	}
	
	/*public int all_queries_all_runs (double hp_run_sec, double lp_run_sec, boolean run_priorization, int x0, int x1, boolean catchup) {
		
		int number = 0;
		
		try {		
			// Schedule HP query of HP runs
			ArrayList<Transaction> transactions1 = one_query_HPruns(hp_run_sec, 1, run_priorization, x0, x1, catchup);
			transaction_number.await();
			transaction_number = new CountDownLatch(transactions1.size());
			for (Transaction t : transactions1) { 
				t.transaction_number = transaction_number;
				executor.execute(t); 
			}			
			// Schedule LP query of HP runs and HP query of LP runs
			ArrayList<Transaction> transactions2 = one_query_HPruns(hp_run_sec, 2, run_priorization, x0, x1, catchup);
			transactions2.addAll(one_query_LPruns(lp_run_sec, 1, run_priorization, x0, x1, catchup));
			transaction_number.await();
			transaction_number = new CountDownLatch(transactions2.size());
			for (Transaction t : transactions2) { 
				t.transaction_number = transaction_number;
				executor.execute(t); 
			}				
			// Schedule LP query of LP runs
			ArrayList<Transaction> transactions3 = one_query_LPruns(lp_run_sec, 2, run_priorization, x0, x1, catchup);
			transaction_number.await();
			number = transactions3.size();
			transaction_number = new CountDownLatch(number);
			for (Transaction t : transactions3) { 
				t.transaction_number = transaction_number;
				executor.execute(t); 
			}		
		} catch (final InterruptedException ex) { ex.printStackTrace(); }
		return number;
	}*/
	
	/*public void all_queries_all_runs (double hp_query_sec, double lp_query_sec, boolean run_priorization, boolean catchup) {
		
		try {	
			// Schedule HP query of all runs and LP query of all runs
			ArrayList<Transaction> transactions = one_query_all_runs(hp_query_sec, 1, run_priorization, catchup);
			transactions.addAll(one_query_all_runs(lp_query_sec, 2, run_priorization, catchup));
			transaction_number.await();
			transaction_number = new CountDownLatch(transactions.size());		
			for (Transaction t : transactions) { 
				t.transaction_number = transaction_number;
				executor.execute(t); 
			}				
		} catch (final InterruptedException ex) { ex.printStackTrace(); }
	}*/
	
	/*public int all_queries_HP_runs (double sec, boolean run_priorization, int x0, int x1, boolean catchup) {
		
		int number = 0;
		
		try {	
			// Schedule HP query of HP runs
			ArrayList<Transaction> transactions1 = one_query_HPruns(sec, 1, run_priorization, x0, x1, catchup);
			transaction_number.await();
			transaction_number = new CountDownLatch(transactions1.size());	
			for (Transaction t : transactions1) { 			
				t.transaction_number = transaction_number;
				executor.execute(t); 
			}		
			// Schedule LP query of HP runs
			ArrayList<Transaction> transactions2 = one_query_HPruns(sec, 2, run_priorization, x0, x1, catchup);
			transaction_number.await();
			number = transactions2.size();
			transaction_number = new CountDownLatch(number);		
			for (Transaction t : transactions2) {			
				t.transaction_number = transaction_number;
				executor.execute(t); 
			}		
		} catch (final InterruptedException ex) { ex.printStackTrace(); }
		return number;
	}*/
	
	/*public int all_queries_LP_runs (double sec, boolean run_priorization, int x0, int x1, boolean catchup) {
		
		int number = 0;
		
		try {	
			// Schedule HP query of LP runs
			ArrayList<Transaction> transactions1 = one_query_LPruns(sec, 1, run_priorization, x0, x1, catchup);
			transaction_number.await();
			transaction_number = new CountDownLatch(transactions1.size());
			for (Transaction t : transactions1) { 		
				t.transaction_number = transaction_number;
				executor.execute(t); 
			}	
			// Schedule LP query of LP runs
			ArrayList<Transaction> transactions2 = one_query_LPruns(sec, 2, run_priorization, x0, x1, catchup);
			transaction_number.await();
			number = transactions2.size();
			transaction_number = new CountDownLatch(number);
			for (Transaction t : transactions2) { 
				t.transaction_number = transaction_number;
				executor.execute(t); 
			}		
		} catch (final InterruptedException ex) { ex.printStackTrace(); }
		return number;
	}*/
	
	/*public int one_query_all_runs_wrapper (double sec, int query, boolean run_priorization, boolean catchup) {
		
		int number = 0;
		
		try {
			ArrayList<Transaction> transactions = one_query_all_runs (sec, query, run_priorization, catchup, false, false, false, false);
			transaction_number.await();
			number = transactions.size();
			transaction_number = new CountDownLatch(number);			
			for (Transaction t : transactions) { 
				t.transaction_number = transaction_number;
				executor.execute(t); 
			}				
		} catch (final InterruptedException ex) { ex.printStackTrace(); }
		return number;
	}*/
	
	/**
	 * Iterate over all event queues and generate transactions with time stamp sec
	 * @param sec	transaction time stamp			
	 */
	public ArrayList<Transaction> one_query_all_runs (double sec) {
		
		ArrayList<Transaction> transactions = new ArrayList<Transaction>();		
				
		for (int xway=0; xway<=max_xway; xway++) {
			
			for (double seg=0; seg<=99; seg++) {
				
				RunID runid0 = new RunID(xway,0,seg);
				Transaction t0 = one_query_one_run(sec, runid0);
				if (t0!=null) transactions.add(t0);	
								
				if (xway != max_xway || both_dirs) {
					
					RunID runid1 = new RunID(xway,1,seg); 
					Transaction t1 = one_query_one_run(sec, runid1);
					if (t1!=null) transactions.add(t1);
		}}}
		return transactions;
	}	
	
	/**
	 * Iterate over run task queues of HP runs and schedule transactions in round-robin manner.
	 * @param sec				transaction time stamp			
	 * @param query				1 - accident management, 2 - congestion management
	 * @param run_priorization	whether run priority is maintained
	 * @param x0				first high-priority run on road 0, direction 0
	 * @param x1				first high-priority run on road 0, direction 1
	 * @return int 				number of transactions submitted for execution
	 */
	/*public ArrayList<Transaction> one_query_HPruns (double sec, int query, boolean run_priorization, int x0, int x1, boolean catchup) {
		
		ArrayList<Transaction> transactions = new ArrayList<Transaction>();		
		
		// Direction is 0, segments are executed from 99 to x0
		if (x0>=0) {							
			for (double seg=99; seg>=x0; seg--) {
			
				RunID runid0 = new RunID(0,0,seg);
				Transaction t0 = one_query_one_run(sec, runid0, query, run_priorization, catchup, false, false, false, false);
				if (t0!=null) transactions.add(t0);							
		}}
		// Direction is 1, segments are executed from 0 to x1
		if (x1>=0) {							
			for (double seg=0; seg<=x1; seg++) {
			
				RunID runid1 = new RunID(0,1,seg);
				Transaction t1 = one_query_one_run(sec, runid1, query, run_priorization, catchup, false, false, false, false);
				if (t1!=null) transactions.add(t1);							
		}}		
		return transactions;
	}	*/
	
	/**
	 * Iterate over run task queues of LP runs and schedule transactions in round-robin manner.
	 * @param sec				transaction time stamp			
	 * @param query				1 - accident management, 2 - congestion management
	 * @param run_priorization	whether run priority is maintained
	 * @param x0				first high-priority run on road 0, direction 0
	 * @param x1				first high-priority run on road 0, direction 1
	 * @return int 				number of transactions submitted for execution
	 */
	/*public ArrayList<Transaction> one_query_LPruns (double sec, int query, boolean run_priorization, int x0, int x1, boolean catchup) {
		
		ArrayList<Transaction> transactions = new ArrayList<Transaction>();
		
		// Direction is 0, segments are executed from x0-1 or 99 to 0
		int from0 = (x0>=0) ? (x0-1) : 99;
		
		for (double seg=from0; seg>=0; seg--) {
				
			RunID runid0 = new RunID(0,0,seg);							
			Transaction t0 = one_query_one_run(sec, runid0, query, run_priorization, catchup, false, false, false, false);
			if (t0!=null) transactions.add(t0);	
		}
		// Direction is 1, segments are executed from x1+1 or 0 to 99
		int from1 = (x1>=0) ? (x1+1) : 0;
		
		for (double seg=from1; seg<=99; seg++) {					
								
			RunID runid1 = new RunID(0,1,seg);
			Transaction t1 = one_query_one_run(sec, runid1, query, run_priorization, catchup, false, false, false, false);
			if (t1!=null) transactions.add(t1);	
		}		
		return transactions;
	}*/
	
	/**
	 * Wraps the processing of all events which have the given time stamp and  
	 * are relevant for the same run into one transaction 
	 * @param sec	transaction time stamp
	 * @param runid	identifier of the run the events of which are scheduled
	 */
	public Transaction one_query_one_run (double sec, RunID runid) {
		
		if (eventqueues.contents.containsKey(runid)) {
			
			ConcurrentLinkedQueue<PositionReport> eventqueue = eventqueues.contents.get(runid);	
			
			if (eventqueue!=null && !eventqueue.isEmpty()) {			
				
				ArrayList<PositionReport> event_list = new ArrayList<PositionReport>();					
					
				PositionReport event = eventqueue.peek();
				while (event!=null && event.sec==sec) { 				
					eventqueue.poll();
					event.schedulerTime = (System.currentTimeMillis() - startOfSimulation)/new Double(1000);
					event_list.add(event);				
					event = eventqueue.peek();
				}
				
				/*** If the event list is not empty, generate a transaction and submit it for execution ***/
				if (!event_list.isEmpty()) {
						
					Run run = runs.get(runid);
					if (optimized) {
						return new TrafficManagement (run, event_list, runs, startOfSimulation, distrFinishTimes, schedStartTimes, total_exe_time, accidentWarningsFailed, tollNotificationsFailed, 1);
					} else {
						return new DefaultTrafficManagement (event_list, runs, startOfSimulation, distrFinishTimes, schedStartTimes, total_exe_time, accidentWarningsFailed, tollNotificationsFailed);
				}}				
		}}
		return null;
	}
}
