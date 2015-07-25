package iogenerator;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;

import run.*;
 
public class Main {
	
	/**
	 * Create and call the chain: Input files -> Drivers/Distributors -> Schedulers -> Executor pool -> Output files
	 * 
	 * @param args: SCHEDULING
	 * 0			split queries: 0 for false, 1 for true
	 * 1			scheduling strategy: 1 for TDS, 2 for RDS, 3 for QDS and 4 for RQDS
	 * 2			HP frequency 
	 * 3			LP frequency
	 *
	 * 				OPTIMIZATION
	 * 4			event derivation omission: 1 for yes, 0 for no
	 * 5			early mandatory projections: 1 for yes, 0 for no
	 * 6			early condensed filtering: 1 for yes, 0 for no
	 * 7			reduced stream history traversal: 1 for yes, 0 for no
	 *
	 *				STATISTICS
	 * 8			count and rate computation: 0 for no, 1 for yes
	 * 
	 *  			INPUT
	 * 9			last second : 10784
	 * 10			path : src/input/ or ../../input/
	 * 11			extension : .txt or .dat
	 * 12			input file names in xway;dir-xway;dir-xway;dir format				
	 */
	public static void main (String[] args) { 
		
		/*** Validate the number of input parameters ***/
		if (args.length < 13) {
			System.out.println("At least 13 input parameters are expected.");
			return;
		}	
		
		/*** SCHEDULING ***/
		boolean splitQueries = args[0].equals("1");
		
		int scheduling_strategy = Integer.parseInt(args[1]);
		if (scheduling_strategy<1 || scheduling_strategy>4) {
			System.out.println("Second input parameter is a scheduling strategy which is an integer from 1 to 4.");
			return;
		}
		int HP_frequency = Integer.parseInt(args[2]);	
		int LP_frequency = Integer.parseInt(args[3]);
		
		/*** OPTIMIZATION ***/
		boolean ed = args[4].equals("1");
		boolean pr = args[5].equals("1");
		boolean fi = args[6].equals("1");
		boolean sh = args[7].equals("1");		
		
		/*** STATISTICS ***/
		boolean count_and_rate = args[8].equals("1");
		
		/*** INPUT ***/
		int lastSec = Integer.parseInt(args[9]);
		String path = args[10];
		String extension = args[11];
		
		//int numberOfInputFiles = args.length - 6;
		//int numberOfHelperThreads = numberOfInputFiles * 2;
		//int numberOfExeThreads = Runtime.getRuntime().availableProcessors() - numberOfHelperThreads;
		ExecutorService executor = Executors.newFixedThreadPool(4); //numberOfExeThreads);	
		
		ArrayList<CountDownLatch> dones = new ArrayList<CountDownLatch>();
		ArrayList<HashMap<RunID,Run>> runtables = new ArrayList<HashMap<RunID,Run>>();
		
		AtomicInteger max_latency = new AtomicInteger(0);
		
		for (int i=12; i<args.length; i++) {
			
			/*** Input file ***/	
			String filename = path + args[i] + extension;
			
			/*** Highways and directions ***/
			String[] inputs = args[i].split("-");
			ArrayList<XwayDirPair> xways_and_dirs = new ArrayList<XwayDirPair> ();
			
			for (String input : inputs) {
				
				String[] components = input.split(";");			
				int xway = Integer.parseInt(components[0]);
				int dir = Integer.parseInt(components[1]);
				XwayDirPair xd = new XwayDirPair(xway,dir);
				xways_and_dirs.add(xd);				
				System.out.println(xd.toString());
			}			
			/*** Define shared objects ***/
			HashMap<RunID,Run> runs = new HashMap<RunID,Run>();
			runtables.add(runs);
			CountDownLatch done = new CountDownLatch(1);
			dones.add(done);
			
			/*** Start driver, distributer and scheduler ***/
			EventPreprocessor preprocessor = new EventPreprocessor (
					splitQueries,  scheduling_strategy, HP_frequency, LP_frequency,
					ed, pr, fi, sh,
					filename, xways_and_dirs, lastSec, 
					count_and_rate,
					runs, executor, done, max_latency);
			Thread ppThread = new Thread(preprocessor);
			ppThread.setPriority(10);
			ppThread.start();			
		}	
		try {			
			/*** Wait till all input events are processed and terminate the executor ***/
			for (CountDownLatch done : dones) {
				done.await();		
			}
			// Print maximal latency
			System.out.println("Maximal latency: " + max_latency);
			
			executor.shutdown();	
			System.out.println("Executor is done.");
									
			/*** Generate output files ***/
			OutputFileGenerator.write2File (runtables, HP_frequency, LP_frequency, lastSec, count_and_rate);  			
			System.out.println("Main is done.");
			
		} catch (InterruptedException e) { e.printStackTrace(); }
	}	
}