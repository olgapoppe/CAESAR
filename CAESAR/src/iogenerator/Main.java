package iogenerator;

import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;
import distributor.*;
import run.*;
import scheduler.*;
import window.WindowDistribution;
 
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
	 * 2			count and rate computation: 1 for yes, 0 for no
	 * 
	 *  			INPUT
	 * 3			last second : 10784
	 * 4			path : src/input/ or ../../input/
	 * 5			input file names in first_xway-last_xway(dir) format				
	 * 				for an event processor for each input file: xway:dir-xway:dir
	 * 6			extension : .txt or .dat
	 * 
	 * 				CONTEXT WINDOWS, all 0s if original benchmark is executed
	 * 7			lambda in seconds for Poisson distribution
	 * 8			window distribution: 0 for uniform, 1 for Poisson
	 * 9			window length in seconds
	 * 10			number of windows
	 * 11			number of queries 
	 */
	public static void main (String[] args) { 
		
		/*** Print current time to know when the experiment started ***/
		Date dNow = new Date( );
	    SimpleDateFormat ft = new SimpleDateFormat ("E yyyy.MM.dd 'at' hh:mm:ss a zzz");
	    System.out.println("Current Date: " + ft.format(dNow));
	    
	    /*** Validate the number of input parameters ***/
	    if (args.length < 12) {
			System.out.println("At least 12 input parameters are expected.");
			return;
		} 
		
		/*** EXECUTORS ***/
		int number_of_executors = Integer.parseInt(args[0]);
		//System.out.println("Number of executors: " + number_of_executors);
		ExecutorService executor = Executors.newFixedThreadPool(number_of_executors);
		
		/*** OPTIMIZATION ***/
		boolean optimized = args[1].equals("1");
				
		/*** STATISTICS ***/
		boolean count_and_rate = args[2].equals("1");
		AtomicDouble total_exe_time = new AtomicDouble(0);
		
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
		System.out.println(	"Max xway: " + max_xway + 
							"\nLast xway is two-directional: " + both_dirs);
		
		/*** CONTEXT WINDOWS ***/
		int lambda = Integer.parseInt(args[7]);
		int window_distribution = Integer.parseInt(args[8]);
		int window_length =  Integer.parseInt(args[9]);
		int window_number =  Integer.parseInt(args[10]);
		int query_number =  Integer.parseInt(args[11]);
		System.out.println(	"Lambda: " + lambda +
							"\nWindow distribution: " + window_distribution +
							"\nWindow length: " + window_length + 
							"\nWindow number: " + window_number + 
							"\nQuery replications: " + query_number);
		
		/*** Create shared data structures ***/		
		AtomicInteger distributorProgress = new AtomicInteger(-1);	
		HashMap<Double,Double> distrFinishTimes = new HashMap<Double,Double>();
		HashMap<Double,Double> schedStartTimes = new HashMap<Double,Double>();
				
		EventQueues eventqueues = new EventQueues(distributorProgress);
		HashMap<RunID,Run> runs = new HashMap<RunID,Run>();
				
		CountDownLatch transaction_number = new CountDownLatch(0);	
		CountDownLatch done = new CountDownLatch(1);
		long startOfSimulation = System.currentTimeMillis();	
		
		/*** Get expensive windows and reset last second ***/
		ArrayList<Integer> expensive_windows = new ArrayList<Integer>();
		if (window_number > 0) {
			
			/*** Get expensive windows ***/
			int window_center = lambda/window_length + 1;		
			expensive_windows = (window_distribution == 0) ? 
					WindowDistribution.getUniformNumbers(lastSec, window_length, window_number) : 
					WindowDistribution.getPoissonNumbers(lastSec, window_length, window_number, window_center);
			String s = "";
			if (window_distribution == 1) s = "Window center: " + window_center + " ";			
			System.out.println(s + "Expensive windows: " + expensive_windows.toString());
		
			/*** Reset last second if the last expensive window ends before ***/
			int max_expensive_window = -1;
			for (int w : expensive_windows) {
				if (max_expensive_window < w) max_expensive_window = w;
			}
			int new_lastSec = (max_expensive_window+1)*window_length;
			if (lastSec > new_lastSec) lastSec = new_lastSec;
		}
		
		/*** Create and start event distributing and query scheduling threads.
		 *   Distributor reads from the file and writes into runs and event queues.
		 *   Scheduler reads from runs and run queues and submits tasks to executor. ***/
		EventDistributor distributor = new SingleQueueDistributor(
				filename,  lastSec, 
				runs, eventqueues, 
				startOfSimulation, distributorProgress, distrFinishTimes, count_and_rate);				
				
		Scheduler scheduler;
		if (window_length == 0 && window_number == 0 && query_number == 0) {
			scheduler = new TimeDrivenScheduler(
					max_xway, both_dirs, lastSec,
					runs, eventqueues, executor, 
					distributorProgress, distrFinishTimes, schedStartTimes, transaction_number, done, 
					startOfSimulation, optimized, total_exe_time);
		} else {
			scheduler = new ExpensiveWindowScheduler(
					max_xway, both_dirs, lastSec,
					runs, eventqueues, executor, 
					distributorProgress, distrFinishTimes, schedStartTimes, transaction_number, done, 
					startOfSimulation, optimized, total_exe_time,
					window_length, window_number, query_number, expensive_windows);
		}
		
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
			OutputFileGenerator.write2File (runs, lastSec, count_and_rate, total_exe_time);  			
			System.out.println("Main is done.");
			
		} catch (InterruptedException e) { e.printStackTrace(); }
	}	
}