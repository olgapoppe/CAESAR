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
		
	public SingleQueueDistributor (AtomicInteger dp, HashMap<Double,Long> distrProgrPerSec, String f, HashMap<RunID,Run> rs, EventQueues rq, 
			AtomicInteger x1, AtomicInteger x2, int last, long start, boolean cr) {
		super(dp, distrProgrPerSec, f, rs, rq, x1, x2, last, start, cr);		 
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
			double prev_sec = -1;
			long now = 0;
			int next_min_2_sleep = 10;
			
			// First event
			String line = scanner.nextLine();
	 		PositionReport event = PositionReport.parse(line);	
	 								
			/*** Put events within the current batch into the run queue ***/ 		
		 	while (event != null) { 
		 			
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
						
					/*** Put the event into the run queue ***/						
					event.distributorTime = (System.currentTimeMillis() - startOfSimulation)/1000;
						
					LinkedBlockingQueue<PositionReport> eventqueue = eventqueues.contents.get(runid);
					if (eventqueue == null) {    
						eventqueue = new LinkedBlockingQueue<PositionReport>();
						eventqueues.contents.put(runid, eventqueue);		 				
					}
					eventqueue.add(event);	
					if (count_and_rate && eventqueue.size() > run.output.maxLengthOfEventQueue) run.output.maxLengthOfEventQueue = eventqueue.size();
					//System.out.println(event.toString());											
				}
		 		/*** Get current application time  ***/
		 		now = (System.currentTimeMillis() - startOfSimulation)/1000;
		 			
		 		/*** Set distributor progress per second ***/
		 		if (prev_sec < event.sec) {
		 				
		 			eventqueues.setDistributorProgress(prev_sec);					
					
					//System.out.println("Distributor progress for " + prev_sec + " is " + now);
					distributorProgressPerSec.put(prev_sec,now);
						
					prev_sec = event.sec;
				}	
		 		
		 		/*** Sleep for 5 minutes if event distribution is more than 10 minutes ahead of application time ***/
		 		if (event.min == next_min_2_sleep && (prev_sec-1) - now > 10) {
		 			
		 			System.out.println("Distribution is done till " + (prev_sec-1) + ". Distributor sleeps 5 min.");		 			
					Thread.sleep(300000);
		 			
		 			next_min_2_sleep += 10;
		 		}
		 			
		 		/*** Reset event ***/
				if (scanner.hasNextLine()) {		 				
					line = scanner.nextLine();   
					event = PositionReport.parse(line);		 				
				} else {
					event = null;		 				
				}	 			
			}	
		 	/*** Set distributor progress for the last second ***/
		 	eventqueues.setDistributorProgress(prev_sec);
		 	
		 	now = (System.currentTimeMillis() - startOfSimulation)/1000;
			//System.out.println("Distributor progress for " + prev_sec + " is " + now);
			distributorProgressPerSec.put(prev_sec,now); 			
		 				
			/*** Clean-up ***/		
			scanner.close();				
			System.out.println("Distributor is done.");		
		} 
		catch (FileNotFoundException e) { e.printStackTrace(); }	
		catch (final InterruptedException e) { e.printStackTrace(); }
	}
}
