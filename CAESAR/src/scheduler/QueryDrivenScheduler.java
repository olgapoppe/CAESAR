package scheduler;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicInteger;
import run.*;
import transaction.*;
import event.*;
import distributor.*;
import iogenerator.*;

/**
 * As soon as all events with the same time stamp become available,
 * query driven scheduler always submits HP queries for execution 
 * more often than LP queries.
 * @author Olga Poppe 
 */
public class QueryDrivenScheduler extends Scheduler implements Runnable {
	
	public final RunQueues HPrunqueues;
	int HPquery_frequency;
	int LPquery_frequency;

	public QueryDrivenScheduler (AtomicInteger dp, HashMap<Double,Long> distrProgrPerSec, HashMap<RunID,Run> rs, RunQueues rq, RunQueues hprq, ExecutorService e, 
			CountDownLatch tn, CountDownLatch d, ArrayList<XwayDirPair> xds, int lastS, long start, AtomicDouble max_late, int hpqf, int lpqf) {		
		super(dp,distrProgrPerSec,rs,rq,e,tn,d,xds,lastS,start,max_late);
		HPrunqueues = hprq;
		HPquery_frequency = hpqf;
		LPquery_frequency = lpqf;
	}
	
	/**
	 * As long as not all events are processed, iterate over all run task queues and pick tasks to execute in round-robin manner.
	 * HP queries are always executed faster than LP queries.
	 */	
	public void run() {	
		
		// Local variables
		double hp_sec = -1;
		double lp_sec = -1;
		int hp_count = 0;
		int lp_count = 0;
					
		// Get the permission to schedule current second
		while (hp_sec <= lastSec && runqueues.getDistributorProgress(hp_sec, startOfSimulation, accidentWarningsFailed, tollNotificationsFailed)) { 
			try {				 
				/*** HP queries are always processed faster ***/
				if (hp_count < HPquery_frequency && lp_count < LPquery_frequency && hp_sec > lp_sec) {
					
					//System.out.println("All queries process " + hp_sec);
					
					all_queries_all_runs(true, hp_sec, false, true, false, false, false, false);
					hp_sec++;
					lp_sec = hp_sec;
					hp_count++;
					lp_count++;					
						
				} else {
					if (hp_count < HPquery_frequency) {
						
						//System.out.println("HP queries process " + hp_sec);
						
						one_query_all_runs_wrapper(hp_sec, 1, false, false);
						hp_sec++;
						hp_count++;						 
					}
					if (hp_count == HPquery_frequency && lp_count == LPquery_frequency) {
						
						hp_count = 0;
						lp_count = 0;
					}
				}				
				/*** If the stream is over, catch up LP queries, wait for acknowledgment of the previous transactions and sleep. ***/					
				if (hp_sec-1 == lastSec) {
						
					/*System.out.println("HP queries are done. LP queries are processed from second " + lp_sec);
						
					while (lp_sec < lastSec) {						
						lp_sec++;
						one_query_all_runs_wrapper(lp_sec, 2, false, false);
					}*/
					transaction_number.await();						
					done.countDown();						
				}				
			} catch (final InterruptedException ex) { ex.printStackTrace(); }	
		}
		System.out.println("Scheduler is done.");
	}	
	
	public Transaction one_query_one_run (double sec, RunID runid, int query, boolean run_priorization, boolean catchup) {
		
		if (query!=1 && query!=2) System.err.println("Non-existing query is called by scheduler.");
		
		/*** Accident management ***/
		if (query == 1) {
		
			if (HPrunqueues.contents.containsKey(runid)) {
			
				LinkedBlockingQueue<PositionReport> HPruntaskqueue = HPrunqueues.contents.get(runid);	
			
				if (HPruntaskqueue!=null && !HPruntaskqueue.isEmpty()) {			
				
					ArrayList<PositionReport> event_list = new ArrayList<PositionReport>();			
					
					// Put all events with the same time stamp as this transaction into the event list
					PositionReport event = HPruntaskqueue.peek();
					while (event!=null && event.sec<=sec) { 				
						HPruntaskqueue.poll();
						event_list.add(event);				
						event = HPruntaskqueue.peek();
					}
					// If the event list is not empty, generate a transaction and submit it for execution
					if (!event_list.isEmpty()) {
						
						Run run = runs.get(runid);
						return new AccidentManagement (run, event_list, runs, startOfSimulation, run_priorization, accidentWarningsFailed, max_latency, distributorProgressPerSec);						
		}}}} else {				
		/*** Congestion management ***/		 
			
			if (runqueues.contents.containsKey(runid)) {
				
				LinkedBlockingQueue<PositionReport> runtaskqueue = runqueues.contents.get(runid);	
			
				if (runtaskqueue!=null && !runtaskqueue.isEmpty()) {			
				
					ArrayList<PositionReport> event_list = new ArrayList<PositionReport>();	
					
					// Put all events with the same time stamp as this transaction into the event list
					PositionReport event = runtaskqueue.peek();
					while (event!=null && event.sec<=sec) { 				
						runtaskqueue.poll();
						event_list.add(event);				
						event = runtaskqueue.peek();
					}
					// If the event list is not empty, generate a transaction and submit it for execution
					if (!event_list.isEmpty()) {
					
						Run run = runs.get(runid);
						return new CongestionManagement (run, event_list, runs, startOfSimulation, tollNotificationsFailed, max_latency, distributorProgressPerSec);						
		}}}}
		return null;
	}
}
