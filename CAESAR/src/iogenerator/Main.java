package iogenerator;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import run.*;
 
public class Main {
	
	/**
	 * Create and call the chain: Input files -> Drivers/Distributors -> Schedulers -> Executor pool -> Output files
	 * 
	 * @param args: EXECUTORS
	 * 0			number of executors
	 *  
	 * 				SCHEDULING
	 * 1			split queries: 0 for false, 1 for true
	 * 2			scheduling strategy: 1 for TDS, 2 for RDS, 3 for QDS and 4 for RQDS
	 * 3			HP frequency 
	 * 4			LP frequency
	 *
	 * 				OPTIMIZATION
	 * 5			event derivation omission: 1 for yes, 0 for no
	 * 6			early mandatory projections: 1 for yes, 0 for no
	 * 7			early condensed filtering: 1 for yes, 0 for no
	 * 8			reduced stream history traversal: 1 for yes, 0 for no
	 *
	 *				STATISTICS
	 * 9			count and rate computation: 0 for no, 1 for yes
	 * 
	 *  			INPUT
	 * 10			last second : 10784
	 * 11			path : src/input/ or ../../input/
	 * 12			input file names in first_xway-last_xway(dir) format				
	 * 				for an event processor for each input file: xway:dir-xway:dir
	 * 13			extension : .txt or .dat
	 */
	public static void main (String[] args) { 
		
		/*** Print current time to know when the experiment started ***/
		Date dNow = new Date( );
	    SimpleDateFormat ft = new SimpleDateFormat ("E yyyy.MM.dd 'at' hh:mm:ss a zzz");
	    System.out.println("Current Date: " + ft.format(dNow));
		
		/*** Validate the number of input parameters ***/
		if (args.length < 14) {
			System.out.println("At least 14 input parameters are expected.");
			return;
		}	
		
		/*** EXECUTORS ***/
		int number_of_executors = Integer.parseInt(args[0]);
		System.out.println("Number of executors: " + number_of_executors);
		ExecutorService executor = Executors.newFixedThreadPool(number_of_executors);
		
		/*** SCHEDULING ***/
		boolean splitQueries = args[1].equals("1");
		
		int scheduling_strategy = Integer.parseInt(args[2]);
		if (scheduling_strategy<1 || scheduling_strategy>4) {
			System.out.println("Second input parameter is a scheduling strategy which is an integer from 1 to 4.");
			return;
		}
		int HP_frequency = Integer.parseInt(args[3]);	
		int LP_frequency = Integer.parseInt(args[4]);
		
		/*** OPTIMIZATION ***/
		boolean ed = args[5].equals("1");
		boolean pr = args[6].equals("1");
		boolean fi = args[7].equals("1");
		boolean sh = args[8].equals("1");		
		
		/*** STATISTICS ***/
		boolean count_and_rate = args[9].equals("1");
		AtomicDouble max_exe_time = new AtomicDouble(0);
		
		/*** INPUT ***/
		int lastSec = Integer.parseInt(args[10]);
		String path = args[11].replaceAll("dot", ".");
		String file = args[12];
		String extension = args[13].replaceAll("dot", ".");		
		String filename = path + file + extension;
		
		String[] last_xway_dir;
		if (file.contains("-")) {
			String[] bounds = file.split("-");
			last_xway_dir = bounds[1].split("d");			
		} else {
			last_xway_dir = file.split("d");
		}
		int max_xway = Integer.parseInt(last_xway_dir[0]);
		boolean both_dirs = (Integer.parseInt(last_xway_dir[1])==2);		
		System.out.println("Max xway: " + max_xway + "\nLast xway is two-directional: " + both_dirs);
		
		/*** Define shared objects ***/
		HashMap<RunID,Run> runs = new HashMap<RunID,Run>();
		CountDownLatch done = new CountDownLatch(1);
				
		/*** Start driver, distributer and scheduler ***/
		EventPreprocessor preprocessor = new EventPreprocessor (
				splitQueries,  scheduling_strategy, HP_frequency, LP_frequency,
				ed, pr, fi, sh,
				filename, max_xway, both_dirs, lastSec, 
				count_and_rate, max_exe_time, 
				runs, executor, done);
		Thread ppThread = new Thread(preprocessor);
		ppThread.setPriority(10);
		ppThread.start();	
		
		try {			
			/*** Wait till all input events are processed and terminate the executor ***/
			done.await();		
			executor.shutdown();	
			System.out.println("Executor is done.");
									
			/*** Generate output files ***/
			OutputFileGenerator.write2File (runs, HP_frequency, LP_frequency, lastSec, count_and_rate, max_exe_time);  			
			System.out.println("Main is done.");
			
		} catch (InterruptedException e) { e.printStackTrace(); }
	}	
}