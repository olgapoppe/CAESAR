package iogenerator;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import run.*;
 
public class Main {
	
	/**
	 * Create and call the chain: Input files -> Drivers -> Distributors -> Schedulers -> Executor pool -> Output files
	 * @param args: scheduling strategy: 1 for TDS, 2 for RDS, 3 for QDS and 4 for RQDS
	 * 				HP frequency 
	 * 				LP frequency 
	 * 				last second
	 * 				input file names 				
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
		int lastSec = Integer.parseInt(args[3]); // 10784
		
		//int numberOfInputFiles = args.length - 4;
		//int numberOfHelperThreads = numberOfInputFiles * 2;
		//int numberOfExeThreads = Runtime.getRuntime().availableProcessors() - numberOfHelperThreads;
		ExecutorService executor = Executors.newFixedThreadPool(2); //numberOfExeThreads);
		
		//String path = "src/input/"; // local path
		//String extension = ".txt";
		String path = "../../input/"; // remote path
		String extension = ".dat";
		
		ArrayList<CountDownLatch> dones = new ArrayList<CountDownLatch>();
		ArrayList<HashMap<RunID,Run>> runtables = new ArrayList<HashMap<RunID,Run>>();
		
		for (int i=4; i<args.length; i++) {
			
			/*** Input file ***/	
			String filename = path + args[i] + extension;
			
			/*** Local variables ***/
			String[] limits = args[i].split("-");
			int firstXway = Integer.parseInt(limits[0]);
			int lastXway = Integer.parseInt(limits[1]);
			boolean lastXwayUnidir = limits[1].contains(".");
			
			/*** Define shared objects ***/
			HashMap<RunID,Run> runs = new HashMap<RunID,Run>();
			runtables.add(runs);
			CountDownLatch done = new CountDownLatch(1);
			dones.add(done);
			
			/*** Start driver, distributer and scheduler ***/
			EventPreprocessor preprocessor = new EventPreprocessor (filename, scheduling_strategy, runs, executor, 
					done, firstXway, lastXway, lastXwayUnidir, lastSec, HP_frequency, LP_frequency);
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