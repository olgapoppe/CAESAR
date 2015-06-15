package iogenerator;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import run.*;
 
public class Main {
	
	/**
	 * Create and call the chain: Input files -> Drivers/Distributors -> Schedulers -> Executor pool -> Output files
	 * @param args: scheduling strategy: 1 for TDS, 2 for RDS, 3 for QDS and 4 for RQDS
	 * 				HP frequency 
	 * 				LP frequency 
	 * 				last second : 10784
	 * 				path : src/input/ or ../../input/
	 * 				extension : .txt or .dat
	 * 				input file names in xway;dir-xway;dir-xway;dir format				
	 */
	public static void main (String[] args) { 
		
		/*** Validate the input parameter ***/
		if (args.length < 5) {
			System.out.println("At least 5 input parameters are expected.");
			return;
		}	
		// Scheduling determined parameters
		int scheduling_strategy = Integer.parseInt(args[0]);
		if (scheduling_strategy<1 || scheduling_strategy>4) {
			System.out.println("First input parameter is a scheduling strategy which is an integer from 1 to 4.");
			return;
		}
		int HP_frequency = Integer.parseInt(args[1]);	
		int LP_frequency = Integer.parseInt(args[2]);		
		
		// Input file determined parameters
		int lastSec = Integer.parseInt(args[3]);
		String path = args[4];
		String extension = args[5];
		
		//int numberOfInputFiles = args.length - 6;
		//int numberOfHelperThreads = numberOfInputFiles * 2;
		//int numberOfExeThreads = Runtime.getRuntime().availableProcessors() - numberOfHelperThreads;
		ExecutorService executor = Executors.newFixedThreadPool(2); //numberOfExeThreads);	
		
		ArrayList<CountDownLatch> dones = new ArrayList<CountDownLatch>();
		ArrayList<HashMap<RunID,Run>> runtables = new ArrayList<HashMap<RunID,Run>>();
		
		for (int i=6; i<args.length; i++) {
			
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
			EventPreprocessor preprocessor = new EventPreprocessor (filename, scheduling_strategy, runs, executor, 
					done, xways_and_dirs, lastSec, HP_frequency, LP_frequency);
			Thread ppThread = new Thread(preprocessor);
			ppThread.setPriority(10);
			ppThread.start();			
		}						
		try {
			/*** Wait till all input events are processed and terminate the executor ***/
			for (CountDownLatch done : dones) {
				done.await();		
			}
			executor.shutdown();	
			System.out.println("Executor is done.");
									
			/*** Generate output files ***/
			OutputFileGenerator.write2File (runtables, HP_frequency, LP_frequency);  			
			System.out.println("Main is done.");
			
		} catch (InterruptedException e) { e.printStackTrace(); }
	}	
}