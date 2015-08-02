package iogenerator;

import java.util.HashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicInteger;
import run.*;
import scheduler.*;
import distributor.*;

public class EventPreprocessor implements Runnable {	
	
	boolean splitQueries;	
	int scheduling_strategy; 
	int HP_frequency;
	int LP_frequency;
	
	boolean event_derivation_omission;
	boolean early_mandatory_projections;
	boolean early_condensed_filtering;
	boolean reduced_stream_history_traversal;
	
	String filename;
	int max_xway;
	boolean both_dirs;
	int lastSec;
	
	boolean count_and_rate;
	AtomicDouble max_exe_time;
	
	HashMap<RunID,Run> runs;
	ExecutorService executor;
	CountDownLatch done;
	
	EventPreprocessor(boolean sq,  
			int ss, int HP_freq, int LP_freq,
			boolean ed, boolean pr, boolean fi, boolean sh,
			String f, int mX, boolean bD, int lS,
			boolean cr, AtomicDouble met,
			HashMap<RunID,Run> rs, ExecutorService e, CountDownLatch d) {
		
		splitQueries = sq;
		scheduling_strategy = ss;
		HP_frequency = HP_freq;
		LP_frequency = LP_freq;	
		
		event_derivation_omission = ed;
		early_mandatory_projections = pr;
		early_condensed_filtering = fi;
		reduced_stream_history_traversal = sh;
		
		filename = f;
		max_xway = mX;
		both_dirs = bD;
		lastSec = lS;
		
		count_and_rate = cr;
		max_exe_time = met;
		 
		runs = rs;
		executor = e;		
		done = d;
	}
	
	public void run () {
		
		/*** Create shared data structures ***/
		AtomicInteger distributorProgress = new AtomicInteger(-1);	
		HashMap<Double,Long> distributorProgressPerSec = new HashMap<Double,Long>();
		
		EventQueues eventqueues = new EventQueues(distributorProgress);
		EventQueues HPeventqueues = new EventQueues(distributorProgress);	
		
		CountDownLatch transaction_number = new CountDownLatch(0);				
		AtomicInteger xway0dir0firstHPseg = new AtomicInteger(-1);
		AtomicInteger xway0dir1firstHPseg = new AtomicInteger(-1);	
		long startOfSimulation = System.currentTimeMillis();		
		
		/*** Depending on scheduling strategy, create and start event distributing and query scheduling threads.
		 *   Distributor reads from the file and writes into runs and run queues.
		 *   Scheduler reads from runs and run queues and submits tasks to executor. ***/
		EventDistributor distributor;
		Scheduler scheduler;
		
		if (scheduling_strategy < 3) {
			
			distributor = new SingleQueueDistributor(distributorProgress, distributorProgressPerSec, filename, runs, eventqueues,
												xway0dir0firstHPseg, xway0dir1firstHPseg, lastSec, startOfSimulation, count_and_rate);		
			
			if (scheduling_strategy == 1) {
			
				System.out.println("TIME DRIVEN SCHEDULER.");
			scheduler = new TimeDrivenScheduler(splitQueries, distributorProgress, distributorProgressPerSec, runs, eventqueues, executor, 
												transaction_number, done, max_xway, both_dirs, lastSec, startOfSimulation,
												max_exe_time,
												event_derivation_omission, early_mandatory_projections, early_condensed_filtering, reduced_stream_history_traversal);
			} else {			
			
				System.out.println("RUN DRIVEN SCHEDULER.");
				scheduler = new RunDrivenScheduler(	distributorProgress, distributorProgressPerSec, runs, eventqueues, executor, 
												transaction_number, done, max_xway, both_dirs, lastSec, startOfSimulation, max_exe_time,
												xway0dir0firstHPseg, xway0dir1firstHPseg, HP_frequency, LP_frequency);
			}
		} else {
			
			distributor = new DoubleQueueDistributor(distributorProgress, distributorProgressPerSec, filename, runs, eventqueues, HPeventqueues,
												xway0dir0firstHPseg, xway0dir1firstHPseg, lastSec, startOfSimulation, count_and_rate);	
			
			if (scheduling_strategy == 3) {
			
				System.out.println("QUERY DRIVEN SCHEDULER.");
				scheduler = new QueryDrivenScheduler(distributorProgress, distributorProgressPerSec, runs, eventqueues, HPeventqueues, executor, 
												transaction_number, done, max_xway, both_dirs, lastSec, startOfSimulation, max_exe_time,
												HP_frequency, LP_frequency);
			} else {
			
				System.out.println("RUN AND QUERY DRIVEN SCHEDULER.");
				scheduler = new RunAndQueryDrivenScheduler(distributorProgress, distributorProgressPerSec, runs, eventqueues, HPeventqueues, executor, 
												transaction_number, done, max_xway, both_dirs, lastSec, startOfSimulation, max_exe_time,
												xway0dir0firstHPseg, xway0dir1firstHPseg, HP_frequency, LP_frequency);
		}}
		Thread prodThread = new Thread(distributor);
		prodThread.setPriority(10);
		prodThread.start();
		
		Thread consThread = new Thread(scheduler);
		consThread.setPriority(10);
		consThread.start();
	}
}
