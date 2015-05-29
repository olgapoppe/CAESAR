package iogenerator;

//import java.util.ArrayList;
import java.util.HashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicInteger;
//import parser.TupleParser;
import run.*;
import scheduler.*;
import distributor.*;
import driver.*;
//import event.*;

public class EventPreprocessor implements Runnable {
	
	String filename;
	int scheduling_strategy; 
	HashMap<RunID,Run> runs;
	ExecutorService executor;
	
	CountDownLatch done;
	int firstXway;
	int lastXway;
	boolean lastXwayUnidir;
	int lastSec;
	int HP_frequency;
	int LP_frequency;
	
	EventPreprocessor(String f, int ss, HashMap<RunID,Run> rs, ExecutorService e, 
			CountDownLatch d, int firstR, int lastR, boolean lXU, int lS, int HP_freq, int LP_freq) {
		
		filename = f;
		scheduling_strategy = ss; 
		runs = rs;
		executor = e;
		
		done = d;
		firstXway = firstR;
		lastXway = lastR;
		lastXwayUnidir = lXU;
		lastSec = lS;
		HP_frequency = HP_freq;
		LP_frequency = LP_freq;		
	}
	
	public void run () {
		
		/*** Create shared data structures ***/
		AtomicInteger distributorProgress = new AtomicInteger(-1);	
		
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
			
			distributor = new SingleQueueDistributor(distributorProgress, filename, runs, runqueues,
												xway0dir0firstHPseg, xway0dir1firstHPseg, lastSec, startOfSimulation);		
			
			if (scheduling_strategy == 1) {
			
				System.out.println("TIME DRIVEN SCHEDULER.");
				scheduler = new TimeDrivenScheduler(distributorProgress, runs, runqueues, executor, 
												transaction_number, done, firstXway, lastXway, lastXwayUnidir, lastSec, startOfSimulation);
			} else {			
			
				System.out.println("RUN DRIVEN SCHEDULER.");
				scheduler = new RunDrivenScheduler(	distributorProgress, runs, runqueues, executor, 
												transaction_number, done, firstXway, lastXway, lastXwayUnidir, lastSec, startOfSimulation, 
												xway0dir0firstHPseg, xway0dir1firstHPseg, HP_frequency, LP_frequency);
			}
		} else {
			
			distributor = new DoubleQueueDistributor(distributorProgress, filename, runs, runqueues, HPrunqueues,
												xway0dir0firstHPseg, xway0dir1firstHPseg, lastSec, startOfSimulation);	
			
			if (scheduling_strategy == 3) {
			
				System.out.println("QUERY DRIVEN SCHEDULER.");
				scheduler = new QueryDrivenScheduler(distributorProgress, runs, runqueues, HPrunqueues, executor, 
												transaction_number, done, firstXway, lastXway, lastXwayUnidir, lastSec, startOfSimulation,
												HP_frequency, LP_frequency);
			} else {
			
				System.out.println("RUN AND QUERY DRIVEN SCHEDULER.");
				scheduler = new RunAndQueryDrivenScheduler(distributorProgress, runs, runqueues, HPrunqueues, executor, 
												transaction_number, done, firstXway, lastXway, lastXwayUnidir, lastSec, startOfSimulation,
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
