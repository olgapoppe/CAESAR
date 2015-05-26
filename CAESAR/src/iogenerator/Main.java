package iogenerator;

import java.util.HashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;
import distributor.*;
import driver.*;
import run.*;
import scheduler.*;
 
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
		// input data dependent
		int lastXway = 0;
		boolean lastXwayUnidir = true;
		int lastSec = 20; //10784;		
				
		// scheduler dependent
		int HP_frequency = 3;	// must be >= 1	
		int LP_frequency = 1;	// must be >= 1
		
		/*** Pick the input file ***/
		//String filename = "src/input/few_events.dat";
		//String filename = "src/input/small.txt";
		String filename = "input/datafile20seconds.dat";
		//String filename = "src/input/input_till_sec_1500.dat";
		//String filename = "../../Dropbox/LR/InAndOutput/2xways/merged0andHalf.dat";			
				
		/*** Define shared objects and data structures ***/
		int thread_number = Runtime.getRuntime().availableProcessors() - 3; 
		ExecutorService executor = Executors.newFixedThreadPool(thread_number);	
		
		AtomicInteger driverProgress = new AtomicInteger(-1);
		AtomicInteger distributorProgress = new AtomicInteger(-1);	
		CountDownLatch transaction_number = new CountDownLatch(0);				
		AtomicInteger xway0dir0firstHPseg = new AtomicInteger(-1);
		AtomicInteger xway0dir1firstHPseg = new AtomicInteger(-1);	
		CountDownLatch done = new CountDownLatch(1);
		long startOfSimulation = System.currentTimeMillis();
		
		EventQueue events = new EventQueue(driverProgress);		
		HashMap<RunID,Run> runs = new HashMap<RunID,Run>();
		RunQueues runqueues = new RunQueues(distributorProgress);
		RunQueues HPrunqueues = new RunQueues(distributorProgress);					
		
		/*** Create and start data driver ***/
		DataDriver dataDriver = new DataDriver(driverProgress, filename, events, lastSec, startOfSimulation);
		Thread drThread = new Thread(dataDriver);
		drThread.setPriority(10);
		drThread.start();
		
		/*** Depending on scheduling strategy, create and start event distributing and query scheduling threads ***/
		EventDistributor distributor;
		Scheduler scheduler;
		
		if (scheduling_strategy < 3) {
			
			distributor = new SingleQueueDistributor(distributorProgress, events, runs, runqueues,
												xway0dir0firstHPseg, xway0dir1firstHPseg, lastSec, startOfSimulation);		
			
			if (scheduling_strategy == 1) {
			
				System.out.println("TIME DRIVEN SCHEDULER.");
				scheduler = new TimeDrivenScheduler(distributorProgress, runs, runqueues, executor, 
												transaction_number, done, lastXway, lastXwayUnidir, lastSec, startOfSimulation);
			} else {			
			
				System.out.println("RUN DRIVEN SCHEDULER.");
				scheduler = new RunDrivenScheduler(	distributorProgress, runs, runqueues, executor, 
												transaction_number, done, lastXway, lastXwayUnidir, lastSec, startOfSimulation, 
												xway0dir0firstHPseg, xway0dir1firstHPseg, HP_frequency, LP_frequency);
			}
		} else {
			
			distributor = new DoubleQueueDistributor(distributorProgress, events, runs, runqueues, HPrunqueues,
												xway0dir0firstHPseg, xway0dir1firstHPseg, lastSec, startOfSimulation);	
			
			if (scheduling_strategy == 3) {
			
				System.out.println("QUERY DRIVEN SCHEDULER.");
				scheduler = new QueryDrivenScheduler(distributorProgress, runs, runqueues, HPrunqueues, executor, 
												transaction_number, done, lastXway, lastXwayUnidir, lastSec, startOfSimulation,
												HP_frequency, LP_frequency);
			} else {
			
				System.out.println("RUN AND QUERY DRIVEN SCHEDULER.");
				scheduler = new RunAndQueryDrivenScheduler(distributorProgress, runs, runqueues, HPrunqueues, executor, 
												transaction_number, done, lastXway, lastXwayUnidir, lastSec, startOfSimulation,
												xway0dir0firstHPseg, xway0dir1firstHPseg, HP_frequency, LP_frequency);
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
			OutputFileGenerator.write2File (runs, HP_frequency, LP_frequency);  			
			System.out.println("Main is done.");
			
		} catch (InterruptedException e) { e.printStackTrace(); }
	}	
}
