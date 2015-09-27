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
import window.*;
 
public class Main {
	
	/**
	 * Create and call the chain: Input file -> Driver/Distributor -> Scheduler -> Executor pool -> Output files
	 * 
	 * @param args: EXECUTORS
	 * 0			executor number
	 *  
	 * 				OPTIMIZATION
	 * 1			optimized: 1 for yes, 0 for no
	 * 
	 *				STATISTICS
	 * 2			count and rate computation: 1 for yes, 0 for no
	 * 
	 *  			INPUT DATA
	 * 3			last second : 10784
	 * 4			path : src/input/ or ../../input/
	 * 5			input file names in first_xway-last_xway(dir) format				
	 * 				for an event processor for each input file: xway:dir-xway:dir
	 * 6			extension : .txt or .dat
	 * 
	 * 				CONTEXT WINDOWS, all 0s if original benchmark is executed
	 * 7			window center
	 * 8			window distribution: 0 for uniform, 1 for Poisson
	 * 9			window length (sec)
	 * 10			window number
	 * 11			query number 
	 * 
	 * 				OVERLAPPING CONTEXT WINDOWS, all 0s if context windows do not overlap
	 * 12			shared : 1 for yes, 0 for no
	 * 13			overlap length (sec)
	 * 14			group number of overlapping windows (number of groups of overlapping windows)
	 * 15			group size (number of overlapping windows)
	 */
	public static void main (String[] args) { 
		
		/*** Print current time to know when the experiment started ***/
		Date dNow = new Date( );
	    SimpleDateFormat ft = new SimpleDateFormat ("E yyyy.MM.dd 'at' hh:mm:ss a zzz");
	    System.out.println("Current Date: " + ft.format(dNow));
	    
	    /*** Validate the number of input parameters ***/
	    if (args.length < 16) {
			System.out.println("At least 16 input parameters are expected.");
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
		AtomicInteger total_exe_time = new AtomicInteger(0);
		
		/*** INPUT ***/
		double lastSec = Integer.parseInt(args[3]);
		
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
		int center = Integer.parseInt(args[7]);
		int window_distribution = Integer.parseInt(args[8]);
		int window_length = Integer.parseInt(args[9]);
		int window_number = Integer.parseInt(args[10]);
		int query_number = Integer.parseInt(args[11]);
		System.out.println("Query replications: " + query_number);
		
		boolean shared = args[12].equals("1");
		int overlap_length = Integer.parseInt(args[13]);
		int group_number = Integer.parseInt(args[14]);
		int group_size = Integer.parseInt(args[15]);
				
		/*** SHARED DATA STRUCTURES ***/		
		AtomicInteger distributorProgress = new AtomicInteger(-1);	
		HashMap<Double,Double> distrFinishTimes = new HashMap<Double,Double>();
		HashMap<Double,Double> schedStartTimes = new HashMap<Double,Double>();
				
		EventQueues eventqueues = new EventQueues(distributorProgress);
		HashMap<RunID,Run> runs = new HashMap<RunID,Run>();
				
		CountDownLatch transaction_number = new CountDownLatch(0);	
		CountDownLatch done = new CountDownLatch(1);
		long startOfSimulation = System.currentTimeMillis();	
			
		ArrayList<TimeInterval> expensive_windows = new ArrayList<TimeInterval>();
		int lambda = 0;
		
		/*** EXPENSIVE WINDOWS ***/
		if (overlap_length == 0 && group_number == 0 && group_size == 0) {
			if (window_number > 0) {
			
				/*** Get expensive windows ***/
				lambda = center/window_length + 1;		
				String s = "";
				if (window_distribution == 1) s = 	"Center: " + center + 
													"\nLambda: " + lambda + "\n";			
				System.out.println(	s + "Window distribution: " + window_distribution +
										"\nWindow length: " + window_length + 
										"\nWindow number: " + window_number);
				
				expensive_windows = (window_distribution == 0) ?
									WindowDistribution.getTimeIntervalsForUniformDistribution(lastSec, window_length, window_number, query_number) :
									WindowDistribution.getTimeIntervalsForPoissonDistribution(lastSec, window_length, window_number, lambda, query_number);								
			}
		/*** OVERLAPPING WINDOWS ***/
		} else {			
				
			System.out.println(	"Shared overlapping windows: " + shared +
								"\nWindow length: " + window_length + 
								"\nWindow overlap length: " + overlap_length +
								"\nOverlapping window group number: " + group_number +
								"\nWindow group size: " + group_size);
			
			expensive_windows = WindowDistribution.getTimeIntervalsForSharedWindows (shared, lastSec, window_length, query_number,
					overlap_length, group_number, group_size);
		}
		System.out.println("\nExpensive windows: " + expensive_windows.toString() + "\n----------------------------------");	
		
		/*** Reset LAST SECOND if the last expensive window ends before ***/
		double new_lastSec = 0;
		for (TimeInterval i : expensive_windows) {
			if (new_lastSec < i.end) new_lastSec = i.end;
		}
		if (new_lastSec > 0 && lastSec > new_lastSec) {
			lastSec = new_lastSec;
			//System.out.println("Last second: " + lastSec);
		}
		
		/*** Create and start event distributing and query scheduling THREADS.
		 *   Distributor reads from the file and writes into runs and event queues.
		 *   Scheduler reads from runs and run queues and submits tasks to executor. ***/
		EventDistributor distributor = new SingleQueueDistributor(
				filename,  lastSec, 
				runs, eventqueues, 
				startOfSimulation, distributorProgress, distrFinishTimes, count_and_rate, expensive_windows);				
				
		Scheduler scheduler = new TimeDrivenScheduler(
				max_xway, both_dirs, lastSec,
				runs, eventqueues, executor, 
				distributorProgress, distrFinishTimes, schedStartTimes, transaction_number, done, 
				startOfSimulation, optimized, total_exe_time, query_number, expensive_windows);		
		
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
			OutputFileGenerator.write2File (runs, lastSec, count_and_rate, 
					max_xway, both_dirs, 
					center, lambda, window_distribution, window_length, window_number, expensive_windows,
					query_number, total_exe_time);  
			System.out.println("Main is done.");
			
		} catch (InterruptedException e) { e.printStackTrace(); }
	}	
}