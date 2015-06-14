package iogenerator;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicInteger;
import run.*;
import scheduler.*;
import distributor.*;

public class EventPreprocessor implements Runnable {
	
	String filename;
	int scheduling_strategy; 
	HashMap<RunID,Run> runs;
	ExecutorService executor;
	
	CountDownLatch done;
	ArrayList<XwayDirPair> xways_and_dirs;
	int lastSec;
	int HP_frequency;
	int LP_frequency;
	
	EventPreprocessor(String f, int ss, HashMap<RunID,Run> rs, ExecutorService e, 
		CountDownLatch d, ArrayList<XwayDirPair> xds, int lS, int HP_freq, int LP_freq) {
		
		filename = f;
		scheduling_strategy = ss; 
		runs = rs;
		executor = e;
		
		done = d;
		xways_and_dirs = xds;
		lastSec = lS;
		HP_frequency = HP_freq;
		LP_frequency = LP_freq;		
	}
	
	public void run () {
		
		/*** Create shared data structures ***/
		AtomicInteger distributorProgress = new AtomicInteger(-1);	
		HashMap<Double,Long> distributorProgressPerSec = new HashMap<Double,Long>();
		
		RunQueues runqueues = new RunQueues(distributorProgress);
		RunQueues HPrunqueues = new RunQueues(distributorProgress);	
		
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
			
			distributor = new SingleQueueDistributor(distributorProgress, distributorProgressPerSec, filename, runs, runqueues,
												xway0dir0firstHPseg, xway0dir1firstHPseg, lastSec, startOfSimulation);		
			
			if (scheduling_strategy == 1) {
			
				System.out.println("TIME DRIVEN SCHEDULER.");
				scheduler = new TimeDrivenScheduler(distributorProgress, distributorProgressPerSec, runs, runqueues, executor, 
												transaction_number, done, xways_and_dirs, lastSec, startOfSimulation);
			} else {			
			
				System.out.println("RUN DRIVEN SCHEDULER.");
				scheduler = new RunDrivenScheduler(	distributorProgress, distributorProgressPerSec, runs, runqueues, executor, 
												transaction_number, done, xways_and_dirs, lastSec, startOfSimulation, 
												xway0dir0firstHPseg, xway0dir1firstHPseg, HP_frequency, LP_frequency);
			}
		} else {
			
			distributor = new DoubleQueueDistributor(distributorProgress, distributorProgressPerSec, filename, runs, runqueues, HPrunqueues,
												xway0dir0firstHPseg, xway0dir1firstHPseg, lastSec, startOfSimulation);	
			
			if (scheduling_strategy == 3) {
			
				System.out.println("QUERY DRIVEN SCHEDULER.");
				scheduler = new QueryDrivenScheduler(distributorProgress, distributorProgressPerSec, runs, runqueues, HPrunqueues, executor, 
												transaction_number, done, xways_and_dirs, lastSec, startOfSimulation,
												HP_frequency, LP_frequency);
			} else {
			
				System.out.println("RUN AND QUERY DRIVEN SCHEDULER.");
				scheduler = new RunAndQueryDrivenScheduler(distributorProgress, distributorProgressPerSec, runs, runqueues, HPrunqueues, executor, 
												transaction_number, done, xways_and_dirs, lastSec, startOfSimulation,
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
