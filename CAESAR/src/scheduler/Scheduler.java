package scheduler;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import iogenerator.*;
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
	
	AtomicInteger distributorProgress;
	HashMap<Double,Long> distributorProgressPerSec;
	
	HashMap<RunID,Run> runs;
	public final RunQueues runqueues;		
	ExecutorService executor;
	
	CountDownLatch transaction_number;
	CountDownLatch done;
	ArrayList<XwayDirPair> xways_and_dirs;
	int lastSec;
	long startOfSimulation;
	
	AtomicBoolean accidentWarningsFailed;
	AtomicBoolean tollNotificationsFailed;
	
	Scheduler (AtomicInteger dp, HashMap<Double,Long> distrProgrPerSec, HashMap<RunID,Run> rs, RunQueues rq, ExecutorService e, 
			CountDownLatch tn, CountDownLatch d, ArrayList<XwayDirPair> xds, int lastS, long start) {
		
		distributorProgress = dp;
		distributorProgressPerSec = distrProgrPerSec;
		
		runs = rs;
		runqueues = rq;			
		
		executor = e;
		
		transaction_number = tn;
		done = d;
		xways_and_dirs = xds;
		lastSec = lastS;
		startOfSimulation = start;
		
		accidentWarningsFailed = new AtomicBoolean(false);
		tollNotificationsFailed = new AtomicBoolean(false);
	}	
	
	public int all_queries_all_runs (boolean splitQueries, double sec, boolean run_priorization, boolean catchup) {
		
		int number = 0;
		
		try {	
			if (!splitQueries) {
				
				// Schedule all queries
				ArrayList<Transaction> transactions = one_query_all_runs(sec, 0, run_priorization, catchup);
				number = transactions.size();
			
				//long startOfFirstWaiting = System.currentTimeMillis();					
				transaction_number.await();			
				//long durationOfFirstWaiting = System.currentTimeMillis() - startOfFirstWaiting;
			
				transaction_number = new CountDownLatch(number);			
				for (Transaction t : transactions) { 
					t.transaction_number = transaction_number;
					executor.execute(t); 
				}				
			} else {
				// Schedule HP query of all runs							
				ArrayList<Transaction> transactions1 = one_query_all_runs(sec, 1, run_priorization, catchup);
				number = transactions1.size();
			
				//long startOfFirstWaiting = System.currentTimeMillis();					
				transaction_number.await();			
				//long durationOfFirstWaiting = System.currentTimeMillis() - startOfFirstWaiting;
			
				transaction_number = new CountDownLatch(number);			
				for (Transaction t : transactions1) { 
					t.transaction_number = transaction_number;
					executor.execute(t); 
				}			
				// Schedule LP query of all runs
				ArrayList<Transaction> transactions2 = one_query_all_runs(sec, 2, run_priorization, catchup);
				number = transactions2.size();
			
				//long startOfSecondWaiting = System.currentTimeMillis();			
				transaction_number.await();			
				//long durationOfSecondWaiting = System.currentTimeMillis() - startOfSecondWaiting;
			
				//if (accidentWarningsFailed.get() || tollNotificationsFailed.get()) 
				//	System.out.println(sec + ": Scheduler waited for executor " + durationOfFirstWaiting + " and " + durationOfSecondWaiting + "ms");
			
				transaction_number = new CountDownLatch(number);				
				for (Transaction t : transactions2) { 
					t.transaction_number = transaction_number;
					executor.execute(t); 
			}}		
		} catch (final InterruptedException ex) { ex.printStackTrace(); }
		
		return number;
	}
	
	public int all_queries_all_runs (double hp_run_sec, double lp_run_sec, boolean run_priorization, int x0, int x1, boolean catchup) {
		
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
	}
	
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
	
	public int all_queries_HP_runs (double sec, boolean run_priorization, int x0, int x1, boolean catchup) {
		
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
	}
	
	public int all_queries_LP_runs (double sec, boolean run_priorization, int x0, int x1, boolean catchup) {
		
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
	}
	
	public int one_query_all_runs_wrapper (double sec, int query, boolean run_priorization, boolean catchup) {
		
		int number = 0;
		
		try {
			ArrayList<Transaction> transactions = one_query_all_runs (sec, query, run_priorization, catchup);
			transaction_number.await();
			number = transactions.size();
			transaction_number = new CountDownLatch(number);			
			for (Transaction t : transactions) { 
				t.transaction_number = transaction_number;
				executor.execute(t); 
			}				
		} catch (final InterruptedException ex) { ex.printStackTrace(); }
		return number;
	}
	
	/**
	 * Iterate over all run task queues and schedule transactions in round-robin manner.
	 * @param sec				transaction time stamp			
	 * @param query				1 - accident management, 2 - congestion management
	 * @param run_priorization	whether run priority is maintained
	 * @return int 				number of transactions submitted for execution
	 */
	public ArrayList<Transaction> one_query_all_runs (double sec, int query, boolean run_priorization, boolean catchup) {
		
		ArrayList<Transaction> transactions = new ArrayList<Transaction>();		
				
		for (XwayDirPair pair : xways_and_dirs) {
			
			for (double seg=0; seg<=99; seg++) {
				
				if (pair.dir == 0 || pair.dir == 2 ) {
			
					RunID runid0 = new RunID(pair.xway,0,seg);
					Transaction t0 = one_query_one_run(sec, runid0, query, run_priorization, catchup);
					if (t0!=null) transactions.add(t0);	
				}				
				if (pair.dir == 1 || pair.dir == 2 ) { 
					
					RunID runid1 = new RunID(pair.xway,1,seg); 
					Transaction t1 = one_query_one_run(sec, runid1, query, run_priorization, catchup);
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
	public ArrayList<Transaction> one_query_HPruns (double sec, int query, boolean run_priorization, int x0, int x1, boolean catchup) {
		
		ArrayList<Transaction> transactions = new ArrayList<Transaction>();		
		
		// Direction is 0, segments are executed from 99 to x0
		if (x0>=0) {							
			for (double seg=99; seg>=x0; seg--) {
			
				RunID runid0 = new RunID(0,0,seg);
				Transaction t0 = one_query_one_run(sec, runid0, query, run_priorization, catchup);
				if (t0!=null) transactions.add(t0);							
		}}
		// Direction is 1, segments are executed from 0 to x1
		if (x1>=0) {							
			for (double seg=0; seg<=x1; seg++) {
			
				RunID runid1 = new RunID(0,1,seg);
				Transaction t1 = one_query_one_run(sec, runid1, query, run_priorization, catchup);
				if (t1!=null) transactions.add(t1);							
		}}		
		return transactions;
	}	
	
	/**
	 * Iterate over run task queues of LP runs and schedule transactions in round-robin manner.
	 * @param sec				transaction time stamp			
	 * @param query				1 - accident management, 2 - congestion management
	 * @param run_priorization	whether run priority is maintained
	 * @param x0				first high-priority run on road 0, direction 0
	 * @param x1				first high-priority run on road 0, direction 1
	 * @return int 				number of transactions submitted for execution
	 */
	public ArrayList<Transaction> one_query_LPruns (double sec, int query, boolean run_priorization, int x0, int x1, boolean catchup) {
		
		ArrayList<Transaction> transactions = new ArrayList<Transaction>();
		
		// Direction is 0, segments are executed from x0-1 or 99 to 0
		int from0 = (x0>=0) ? (x0-1) : 99;
		
		for (double seg=from0; seg>=0; seg--) {
				
			RunID runid0 = new RunID(0,0,seg);							
			Transaction t0 = one_query_one_run(sec, runid0, query, run_priorization, catchup);
			if (t0!=null) transactions.add(t0);	
		}
		// Direction is 1, segments are executed from x1+1 or 0 to 99
		int from1 = (x1>=0) ? (x1+1) : 0;
		
		for (double seg=from1; seg<=99; seg++) {					
								
			RunID runid1 = new RunID(0,1,seg);
			Transaction t1 = one_query_one_run(sec, runid1, query, run_priorization, catchup);
			if (t1!=null) transactions.add(t1);	
		}		
		return transactions;
	}
	
	/**
	 * Wraps the processing of all events with the given time stamp and that 
	 * are relevant for the same run into one transaction and submits this transaction for execution.
	 * @param sec				transaction time stamp
	 * @param runid				identifier of the run the tasks of which are scheduled
	 * @param query				1 - accident management, 2 - congestion management
	 * @param run_priorization	whether run priority is maintained
	 * @return boolean 			indicating whether this transaction was submitted for execution
	 */
	public Transaction one_query_one_run (double sec, RunID runid, int query, boolean run_priorization, boolean catchup) {
		
		if (0<query && query<2) System.err.println("Non-existing query is called by scheduler.");
		
		if (runqueues.contents.containsKey(runid)) {
			
			LinkedBlockingQueue<PositionReport> runtaskqueue = runqueues.contents.get(runid);	
			
			if (runtaskqueue!=null && !runtaskqueue.isEmpty()) {			
				
				ArrayList<PositionReport> event_list = new ArrayList<PositionReport>();		
				
				/*** Traffic management ***/
				if (query == 0) {
					
					// Put all events with the same time stamp as this transaction into the event list
					PositionReport event = runtaskqueue.peek();					
					while (event!=null && event.sec==sec) { 				
						runtaskqueue.poll();
						event.schedulerTime = (System.currentTimeMillis() - startOfSimulation)/1000;
						event_list.add(event);				
						event = runtaskqueue.peek();
					}					
					// If the event list is not empty, generate a transaction and submit it for execution
					if (!event_list.isEmpty()) {
						
						Run run = runs.get(runid);
						return new TrafficManagement (run, event_list, runs, startOfSimulation, accidentWarningsFailed, tollNotificationsFailed, distributorProgressPerSec);											
				}}
				
				/*** Accident management ***/
				if (query == 1) {
					
					// Put all events with the same time stamp as this transaction into the event list
					Iterator<PositionReport> iterator = runtaskqueue.iterator();
					/*if (catchup) {
						while(iterator.hasNext()) {							
							PositionReport event = iterator.next();						
							if (event.sec<=sec) {
								event.schedulerTime = (System.currentTimeMillis() - startOfSimulation)/1000;
								event_list.add(event);						
							} else {
								if (event.sec>sec) 
									break;
					}}} else {*/
						while(iterator.hasNext()) {						
							PositionReport event = iterator.next();						
							if (event.sec==sec) {
								event.schedulerTime = (System.currentTimeMillis() - startOfSimulation)/1000;
								event_list.add(event);						
							} else {
								if (event.sec>sec) break;
							}
						}
					//}
					// If the event list is not empty, generate a transaction and submit it for execution
					if (!event_list.isEmpty()) {
						
						Run run = runs.get(runid);
						return new AccidentManagement (run, event_list, runs, startOfSimulation, run_priorization, accidentWarningsFailed, distributorProgressPerSec);											
				}}
				
				/*** Congestion management ***/
				if (query == 2) { 
					
					// Put all events with the same time stamp as this transaction into the event list
					PositionReport event = runtaskqueue.peek();
					/*if (catchup) {
						while (event!=null && event.sec<=sec) { 							
							runtaskqueue.poll();
							event.schedulerTime = (System.currentTimeMillis() - startOfSimulation)/1000;
							event_list.add(event);				
							event = runtaskqueue.peek();
					}} else {*/
						while (event!=null && event.sec==sec) { 				
							runtaskqueue.poll();
							event.schedulerTime = (System.currentTimeMillis() - startOfSimulation)/1000;
							event_list.add(event);				
							event = runtaskqueue.peek();
						}
					//}
					// If the event list is not empty, generate a transaction and submit it for execution
					if (!event_list.isEmpty()) {
					
						Run run = runs.get(runid);
						return new CongestionManagement (run, event_list, runs, startOfSimulation, tollNotificationsFailed, distributorProgressPerSec);					
				}}
		}}
		return null;
	}
}
