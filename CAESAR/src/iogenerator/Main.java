package iogenerator;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;

import distributor.EventDistributor;
import distributor.EventQueues;
import distributor.SingleQueueDistributor;
import run.*;
import scheduler.Scheduler;
import scheduler.TimeDrivenScheduler;
 
public class Main {
	
	/**
	 * Create and call the chain: Input files -> Drivers/Distributors -> Schedulers -> Executor pool -> Output files
	 * 
	 * @param args: EXECUTORS
	 * 0			number of executors
	 *  
	 * 				OPTIMIZATION
	 * 1			optimized: 1 for yes, 0 for no
	 * 
	 *				STATISTICS
	 * 2			count and rate computation: 0 for no, 1 for yes
	 * 
	 *  			INPUT
	 * 3			last second : 10784
	 * 4			path : src/input/ or ../../input/
	 * 5			input file names in first_xway-last_xway(dir) format				
	 * 				for an event processor for each input file: xway:dir-xway:dir
	 * 6			extension : .txt or .dat
	 */
	public static void main (String[] args) { 
		
		/*** Print current time to know when the experiment started ***/
		Date dNow = new Date( );
	    SimpleDateFormat ft = new SimpleDateFormat ("E yyyy.MM.dd 'at' hh:mm:ss a zzz");
	    System.out.println("Current Date: " + ft.format(dNow));
	    
	    /*** Validate the number of input parameters ***/
	    if (args.length < 7) {
			System.out.println("At least 7 input parameters are expected.");
			return;
		} 
		
		/*** EXECUTORS ***/
		int number_of_executors = Integer.parseInt(args[0]);
		System.out.println("Number of executors: " + number_of_executors);
		ExecutorService executor = Executors.newFixedThreadPool(number_of_executors);
		
		/*** OPTIMIZATION ***/
		boolean optimized = args[1].equals("1");
				
		/*** STATISTICS ***/
		boolean count_and_rate = args[2].equals("1");
		AtomicDouble max_exe_time = new AtomicDouble(0);
		
		/*** INPUT ***/
		int lastSec = Integer.parseInt(args[3]);
		
		String path = args[4];
		String file = args[5];
		String extension = args[6];			
		String filename = path + file + extension;
		
		String[] last_xway_dir;
		if (file.contains("-")) {
			String[] bounds = file.split("-");
			last_xway_dir = bounds[1].split(";");			
		} else {
			last_xway_dir = file.split(";");
		}
		int max_xway = Integer.parseInt(last_xway_dir[0]);
		boolean both_dirs = (Integer.parseInt(last_xway_dir[1])==2);		
		System.out.println("Max xway: " + max_xway + "\nLast xway is two-directional: " + both_dirs);
		
		/*** Create shared data structures ***/		
		AtomicInteger distributorProgress = new AtomicInteger(-1);	
		HashMap<Double,Double> distrFinishTimes = new HashMap<Double,Double>();
		HashMap<Double,Double> schedStartTimes = new HashMap<Double,Double>();
				
		EventQueues eventqueues = new EventQueues(distributorProgress);
		HashMap<RunID,Run> runs = new HashMap<RunID,Run>();
				
		CountDownLatch transaction_number = new CountDownLatch(0);	
		CountDownLatch done = new CountDownLatch(1);
		long startOfSimulation = System.currentTimeMillis();		
		
		/*** Create and start event distributing and query scheduling threads.
		 *   Distributor reads from the file and writes into runs and event queues.
		 *   Scheduler reads from runs and run queues and submits tasks to executor. ***/
		EventDistributor distributor = new SingleQueueDistributor(
				filename, lastSec, 
				runs, eventqueues, 
				startOfSimulation, distributorProgress, distrFinishTimes, count_and_rate);				
				
		Scheduler scheduler = new TimeDrivenScheduler(
				max_xway, both_dirs, lastSec,
				runs, eventqueues, executor, 
				distributorProgress, distrFinishTimes, schedStartTimes, transaction_number, done, 
				startOfSimulation, optimized, max_exe_time);
		
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
			OutputFileGenerator.write2File (runs, lastSec, count_and_rate, max_exe_time);  			
			System.out.println("Main is done.");
			
		} catch (InterruptedException e) { e.printStackTrace(); }
	}	
}