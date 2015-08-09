package distributor;

import java.io.File;
import java.io.FileNotFoundException;
import java.util.HashMap;
import java.util.Random;
import java.util.Scanner;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicInteger;
import run.*;
import event.*;

public class SingleQueueDistributor extends EventDistributor {	
		
	public SingleQueueDistributor (AtomicInteger dp, String f, HashMap<RunID,Run> rs, EventQueues rq, 
			AtomicInteger x1, AtomicInteger x2, int last, long start, boolean cr) {
		super(dp, f, rs, rq, x1, x2, last, start, cr);		 
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
			double prev_batch_limit = -1;
			double now = 0;
			double curr_sec = -1;
						
			Random random = new Random();
			int min = 6;
			int max = 14;
			
			double batch_limit = random.nextInt(max - min + 1) + min;
 			if (batch_limit > lastSec) batch_limit = lastSec;	
 			//System.out.println("Batch limit: " + batch_limit);
 			
 			double distributor_wakeup_time = 0;
			
			// First event
			String line = scanner.nextLine();
	 		PositionReport event = PositionReport.parse(line);	
	 								
			/*** Put events within the current batch into the run queue ***/ 		
		 	while (true) { 
		 		
		 		/*** Put events within the current batch into the run queue ***/ 		
		 		while (event != null && event.sec <= batch_limit) {
		 			
		 			if (event.correctPositionReport()) {
		 				
		 				/*** Create run if it does not exist yet ***/
		 				RunID runid = new RunID (event.xway, event.dir, event.seg); 
		 				Run run;
						      		
		 				if (!runs.containsKey(runid) || runs.get(runid) == null) {
							
		 					AtomicInteger firstHPseg = (runid.dir == 0) ? xway0dir0firstHPseg : xway0dir1firstHPseg;
		 					run = new Run(runid, event.sec, event.min, firstHPseg, count_and_rate);
		 					runs.put(runid, run);						
		 				} else {
		 					run = runs.get(runid);
		 				}
		 				if (count_and_rate) run.output.position_reports_count++;
						
		 				/*** Set the event distributor time ***/						
		 				event.distributorTime = (System.currentTimeMillis() - startOfSimulation)/1000;
						
		 				/*** Put the event into the event queue ***/
		 				ConcurrentLinkedQueue<PositionReport> eventqueue = eventqueues.contents.get(runid);
		 				if (eventqueue == null) {    
		 					eventqueue = new ConcurrentLinkedQueue<PositionReport>();
		 					eventqueues.contents.put(runid, eventqueue);		 				
		 				}
		 				eventqueue.add(event);	
		 				if (count_and_rate && eventqueue.size() > run.output.maxLengthOfEventQueue) run.output.maxLengthOfEventQueue = eventqueue.size();
		 				//System.out.println(event.toString());											
		 			}
		 			
		 			/*** Set distributer progress ***/	
		 			if (curr_sec < event.sec) {
		 				
		 				now = (System.currentTimeMillis() - startOfSimulation)/1000;
		 				if (curr_sec>50) { // Avoid null run exception when the stream is read too fast
		 					eventqueues.setDistributorProgress(curr_sec);
		 					if (curr_sec % 10 == 0) System.out.println("Distribution time of second " + curr_sec + " is " + now);
		 				}		 				
			 			curr_sec = event.sec;
		 			}
		 			
		 			/*** Reset event ***/
		 			if (scanner.hasNextLine()) {		 				
		 				line = scanner.nextLine();   
		 				event = PositionReport.parse(line);		 				
		 			} else {
		 				event = null;		 				
		 			}
		 		}		 			
		 		/*** Set distributor progress ***/
		 		now = (System.currentTimeMillis() - startOfSimulation)/new Double(1000);			
				eventqueues.setDistributorProgress(batch_limit); 				
				if (curr_sec % 10 == 0) System.out.println("Distribution time of second " + curr_sec + " is " + now);		 			
		 			
	 			if (batch_limit < lastSec) { 			
	 				
	 				/*** Sleep if curr_ms is smaller than batch_limit ms ***/
	 				now = System.currentTimeMillis() - startOfSimulation;
 					
 					if (now < batch_limit*1000) {
		 			
 						int sleep_time = new Double(batch_limit*1000 - now).intValue();		 			
 						//System.out.println("Distributor sleeps " + sleep_time + " ms");		 			
 						Thread.sleep(sleep_time);
 						distributor_wakeup_time = (System.currentTimeMillis() - startOfSimulation)/1000 - batch_limit;
 					}
 					
 					/*** Rest batch_limit ***/
 					prev_batch_limit = batch_limit;
 					batch_limit += random.nextInt(max - min + 1) + min + distributor_wakeup_time;		 			
	 				if (batch_limit > lastSec) batch_limit = lastSec;
	 				//System.out.println("Batch limit: " + batch_limit);
	 				
	 				if (distributor_wakeup_time>1) {
						System.out.println("Distributor wakeup time is " + distributor_wakeup_time + 
											". Batch limit increases from " + prev_batch_limit +
											" to " + batch_limit + ".");
					}	 				
	 			} else { /*** Terminate ***/	 				
	 				break;
	 			}						
			}		 				
			/*** Clean-up ***/		
			scanner.close();				
			System.out.println("Distributor is done.");		
		} 
		catch (FileNotFoundException e) { e.printStackTrace(); }	
		catch (final InterruptedException e) { e.printStackTrace(); }
	}
}
