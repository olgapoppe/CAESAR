package scheduler;

import java.util.HashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;
import distributor.*;
import run.*;
import iogenerator.*;

public class Main {
	
	/**
	 * This is the main method.	
	 * @param args is an array containing one of the following values:
	 * 1 - Time driven scheduler
	 * 2 - Run driven scheduler
	 * 3 - Query driven scheduler
	 * 4 - Run and query driven scheduler
	 */
	public static void main (String[] args) { 
		
		/*** Validate the input parameter ***/
		int scheduling_strategy = Integer.parseInt(args[0]);
		if (scheduling_strategy<1 || scheduling_strategy>4) {
			System.out.println("Input parameter must be an integer from 1 to 4.");
			return;
		}
		
		/*** Set local variables ***/
		// fixed
		int lastSec = 10784;		
		int thread_number = Runtime.getRuntime().availableProcessors() - 2;	
		
		// variable
		int HP_frequency = 3;	// must be >= 1	
		int LP_frequency = 1;	// must be >= 1
		
		/*** Pick the input file ***/
		//String filename = "src/input/10events.dat";
		//String filename = "src/input/small.txt";
		//String filename = "src/input/datafile20seconds.dat";
		String filename = "../../input.dat";
		//String filename = "../../Dropbox/LR/InAndOutput/1xway/input7.dat";				
		
		/*** Create shared data structures ***/
		AtomicInteger distributorProgress = new AtomicInteger(-1);
		AtomicInteger driverProgress = new AtomicInteger(-1);
		EventQueue events = new EventQueue(driverProgress);		
		HashMap<RunID,Run> runs = new HashMap<RunID,Run>();		
		RunQueues runqueues = new RunQueues(distributorProgress);
		RunQueues HPrunqueues = new RunQueues(distributorProgress);
		ExecutorService executor = Executors.newFixedThreadPool(thread_number);		
		
		CountDownLatch transaction_number = new CountDownLatch(0);
		CountDownLatch done = new CountDownLatch(1); 
		long startOfSimulation = System.currentTimeMillis();		
		AtomicInteger xway0dir0firstHPseg = new AtomicInteger(-1);
		AtomicInteger xway0dir1firstHPseg = new AtomicInteger(-1);	
		
		/*** Create and start data driver ***/
		DataDriver dataDriver = new DataDriver(driverProgress,filename,events,lastSec);
		Thread drThread = new Thread(dataDriver);
		drThread.setPriority(10);
		drThread.start();
		
		/*** Depending on scheduling strategy, create and start event distributing and query scheduling threads ***/
		EventDistributor distributor;
		Scheduler scheduler;
		
		if (scheduling_strategy < 3) {
						
			distributor = new SingleQueueDistributor(distributorProgress, events, runs, runqueues,
												xway0dir0firstHPseg, xway0dir1firstHPseg, lastSec);		
			
			if (scheduling_strategy == 1) {
			
				System.out.println("TIME DRIVEN SCHEDULER.");
				scheduler = new TimeDrivenScheduler(distributorProgress, runs, runqueues, executor, 
												transaction_number, done, lastSec, startOfSimulation);
			} else {			
			
				System.out.println("RUN DRIVEN SCHEDULER.");
				scheduler = new RunDrivenScheduler(	distributorProgress, runs, runqueues, executor, 
												transaction_number, done, lastSec, startOfSimulation, 
												xway0dir0firstHPseg, xway0dir1firstHPseg, 
												HP_frequency, LP_frequency);
			}
		} else {
			
			distributor = new DoubleQueueDistributor(distributorProgress, events, runs, runqueues, HPrunqueues,
												xway0dir0firstHPseg, xway0dir1firstHPseg, lastSec);	
			
			if (scheduling_strategy == 3) {
			
				System.out.println("QUERY DRIVEN SCHEDULER.");
				scheduler = new QueryDrivenScheduler(distributorProgress, runs, runqueues, HPrunqueues, executor, 
												transaction_number, done, lastSec, startOfSimulation,
												HP_frequency, LP_frequency);
			} else {
			
				System.out.println("RUN AND QUERY DRIVEN SCHEDULER.");
				scheduler = new RunAndQueryDrivenScheduler(distributorProgress, runs, runqueues, HPrunqueues, executor, 
												transaction_number, done, lastSec, startOfSimulation,
												xway0dir0firstHPseg, xway0dir1firstHPseg, 
												HP_frequency, LP_frequency);
		}}
		Thread prodThread = new Thread(distributor);
		prodThread.setPriority(10);
		prodThread.start();
		Thread consThread = new Thread(scheduler);
		consThread.setPriority(10);
		consThread.start();
							
		try {
			/*** Wait till all input events are processed and terminate the executor ***/
			done.await();		
			executor.shutdown();	
			System.out.println("Executor is done.");
									
			/*** Generate output files ***/
			OutputFileGenerator.write2File (runs, startOfSimulation, distributor.min_stream_rate, distributor.max_stream_rate, HP_frequency, LP_frequency);
			System.out.println("Main is done.");
			
		} catch (InterruptedException e) { e.printStackTrace(); }
	}	
}
