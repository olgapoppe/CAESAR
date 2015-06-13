package distributor;

import java.io.File;
import java.io.FileNotFoundException;
import java.util.HashMap;
import java.util.Scanner;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicInteger;
import run.*;
import event.*;

public class SingleQueueDistributor extends EventDistributor {	
		
	public SingleQueueDistributor (AtomicInteger dp, HashMap<Double,Double> distrProgrPerSec, String f, HashMap<RunID,Run> rs, RunQueues rq, AtomicInteger x1, AtomicInteger x2, int last, long start) {
		super(dp, distrProgrPerSec, f, rs, rq, x1, x2, last, start);		 
	}

	/** 
	 * Read the input file, parse the events, 
	 * generate new runs if they do not exist yet and
	 * distribute events into run task queues.
	 */	
	public void run() {	
		
		Scanner scanner;
		try {
			// Input file
			scanner = new Scanner(new File(filename));
			
			// Time
			double curr_app_sec = 0;
			double curr_sec = 0;
																
			// First event
			String line = scanner.nextLine();
	 		PositionReport event = PositionReport.parse(line);	
	 								
			while (curr_app_sec <= lastSec) {
				
				// Put events with time stamp curr_app_sec into the run queue 		
		 		while (event != null && event.sec == curr_app_sec) {
		 			
		 			if (event.correctPositionReport()) {
						
						/******************************************* Run *******************************************/
						RunID runid = new RunID (event.xway, event.dir, event.seg); 
						Run run;        		
						if (runs.containsKey(runid)) {
							run = runs.get(runid);             			          			
						} else {
							AtomicInteger firstHPseg = (runid.dir == 0) ? xway0dir0firstHPseg : xway0dir1firstHPseg;
							run = new Run(runid, event.sec, event.min, firstHPseg);
							runs.put(runid, run);
						}  			 	
						/*************************************** Run queues ****************************************/
						// Append event's distributor time and write it into the run queue
						event.distributorTime = (System.currentTimeMillis() - startOfSimulation)/1000;
						
						LinkedBlockingQueue<PositionReport> runtaskqueue = runqueues.contents.get(runid);
						if (runtaskqueue == null) {    
							runtaskqueue = new LinkedBlockingQueue<PositionReport>();
							runqueues.contents.put(runid, runtaskqueue);		 				
						}
						runtaskqueue.add(event);	 	
					}
		 			// Reset event
		 			if (scanner.hasNextLine()) {		 				
		 				line = scanner.nextLine();   
		 				event = PositionReport.parse(line);		 				
		 			} else {
		 				event = null;		 				
		 			}
		 		}			
		 		// Update distributer progress
		 		curr_sec = (System.currentTimeMillis() - startOfSimulation)/1000;
		 		double value = (curr_app_sec > curr_sec) ? curr_app_sec : curr_sec;
		 		distributorProgressPerSec.put(curr_app_sec, value);
		 		
		 		runqueues.setDistributorProgress(curr_app_sec);
		 		
		 		// Sleep if curr_sec is smaller than curr_app_sec
		 		if (curr_sec < curr_app_sec && curr_app_sec < lastSec) {
		 			
		 			int sleep_time = new Double(curr_app_sec - curr_sec).intValue();
		 			
		 			//System.out.println("Driver sleeps " + sleep_time + " seconds.");
		 			
		 			Thread.sleep(sleep_time * 1000);
		 		}
		 		curr_app_sec++;
			}			
			/*** Clean-up ***/		
			scanner.close();				
			System.out.println("Distributor is done.");
		}
		catch (InterruptedException e) { e.printStackTrace(); }
		catch (FileNotFoundException e1) { e1.printStackTrace(); }				 						 
	}
}
