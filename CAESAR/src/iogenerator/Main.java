package iogenerator;

import java.util.HashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import run.*;
 
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
		int firstXway1 = 0;
		int lastXway1 = 4;
		int firstXway2 = 5;
		int lastXway2 = 9;		
		boolean lastXwayUnidir = false;
		int lastSec = 10784;		
				
		// scheduler dependent
		int HP_frequency = 3;	// must be >= 1	
		int LP_frequency = 1;	// must be >= 1
		
		/*** Pick the input file ***/
		//String filename = "src/input/few_events.dat";
		//String filename = "src/input/small.txt";
		//String filename1 = "src/input/datafile20seconds.dat";
		//String filename2 = "src/input/datafile20seconds.dat";
		//String filename = "src/input/input_till_sec_1500.dat";
		String filename1 = "../../merged0to4.dat";			
		String filename2 = "../../merged5to9.dat";
		
		/*** Define shared objects ***/
		HashMap<RunID,Run> runs1 = new HashMap<RunID,Run>();
		HashMap<RunID,Run> runs2 = new HashMap<RunID,Run>();
		CountDownLatch done1 = new CountDownLatch(1);
		CountDownLatch done2 = new CountDownLatch(1);
				
		int thread_number = Runtime.getRuntime().availableProcessors() - 6;
		ExecutorService executor = Executors.newFixedThreadPool(thread_number);	
		
		/*** Start drivers, distributers and schedulers ***/
		EventPreprocessor preprocessor1 = new EventPreprocessor (filename1, scheduling_strategy, runs1, executor, 
				done1, firstXway1, lastXway1, lastXwayUnidir, lastSec, HP_frequency, LP_frequency);
		Thread ppThread1 = new Thread(preprocessor1);
		ppThread1.setPriority(10);
		ppThread1.start();
		
		EventPreprocessor preprocessor2 = new EventPreprocessor (filename2, scheduling_strategy, runs2, executor, 
				done2, firstXway2, lastXway2, lastXwayUnidir, lastSec, HP_frequency, LP_frequency);
		Thread ppThread2 = new Thread(preprocessor2);
		ppThread2.setPriority(10);
		ppThread2.start();		
							
		try {
			/*** Wait till all input events are processed and terminate the executor ***/
			done1.await();		
			done2.await();
			executor.shutdown();	
			System.out.println("Executor is done.");
									
			/*** Generate output files ***/
			OutputFileGenerator.write2File (runs1, runs2, HP_frequency, LP_frequency);  			
			System.out.println("Main is done.");
			
		} catch (InterruptedException e) { e.printStackTrace(); }
	}	
}