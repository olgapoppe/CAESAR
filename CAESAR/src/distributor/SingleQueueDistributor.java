package distributor;

import java.io.File;
import java.io.FileNotFoundException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Random;
import java.util.Scanner;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicInteger;
import run.*;
import event.*;
import window.*;

public class SingleQueueDistributor extends EventDistributor {	
	
	ArrayList<TimeInterval> expensive_windows;
		
	public SingleQueueDistributor (String filename, double lastSec, 
			HashMap<RunID,Run> runs, EventQueues eventqueues, 
			long start, AtomicInteger distrProgress, HashMap<Double,Double> distrFinishTimes, boolean cr, ArrayList<TimeInterval> ew) {
		
		super(filename, lastSec, runs, eventqueues, start, distrProgress, distrFinishTimes, cr);
		
		expensive_windows = ew;
	}

	/** 
	 * If there are expensive windows, events within them are distributed. Events between the expensive window are skipped.
	 * If there are no expensive windows, all events are distributed. 
	 */
	public void run() {	
		try {
			// Input file
			Scanner scanner = new Scanner(new File(filename));
		
			/*** The input file is processed completely ***/
			if (expensive_windows.isEmpty()) {
				
				// First event
				String line = scanner.nextLine();
		 		PositionReport event = PositionReport.parse(line);
				distribute_all_events(scanner, event, -1, lastSec, 0);
				
			} else { 
				
				/*** Only expensive windows are processed ***/		
				// First event
				String line = scanner.nextLine();
		 		PositionReport event = PositionReport.parse(line);	
		 		double total_skipped_time = 0;
		 		double curr_sec = 0;
		 				 		
				for (TimeInterval window : expensive_windows) {
					
					// Skip all events before the beginning of the window
					int count = 0;
					while (event.sec < window.start) {	
						if (curr_sec < event.sec) {
							total_skipped_time++;
							curr_sec++;
						}
						count++;
				 		//System.out.println(count + " " + event.toString());
				 		line = scanner.nextLine();
				 		event = PositionReport.parse(line);
					}
					System.out.println(count + " events skipped before second " + window.start);
					
					// Distribute all events in the window 					
					distribute_all_events(scanner, event, window.start, window.end, total_skipped_time);	
					curr_sec = window.end;
				}			
			}	
			/*** Clean-up ***/		
			scanner.close();				
			System.out.println("\nDistributor is done.");	
 		
		} catch (FileNotFoundException e) { e.printStackTrace(); }
	}
	
	/** 
	 * Read the input file, parse the events, 
	 * generate new runs if they do not exist yet and
	 * distribute events into run task queues.
	 */	
	void distribute_all_events (Scanner scanner, PositionReport event, double curr_sec, double last_sec, double skipped_time) {	
		
		try {	
			// Local variables
			double system_time = 0;
			double distributor_wakeup_time = 0;
			//int event_count = 0;
			
			// First batch			
			Random random = new Random();
			int min = 6;
			int max = 14;	
			
			double end = curr_sec + random.nextInt(max - min + 1) + min;
			TimeInterval batch = new TimeInterval(curr_sec, end);
						
 			if (batch.end > last_sec) batch.end = last_sec;	
 			//System.out.println("\n-------------------------\nBatch end: " + batch.end);
 			
 			/*** Put events within the current batch into the run queue ***/		
	 		while (true) { 
	 		
	 			/*** Put events within the current batch into the run queue ***/ 		
	 			while (event != null && event.sec <= batch.end) {
	 			
	 				if (event.correctPositionReport()) {
	 				
	 					/*** Create run if it does not exist yet ***/
	 					RunID runid = new RunID (event.xway, event.dir, event.seg); 
	 					Run run;
					      		
	 					if (!runs.containsKey(runid) || runs.get(runid) == null) {							
	 						run = new Run(runid, event.sec, event.min, count_and_rate);
	 						runs.put(runid, run);						
	 					} else {
	 						run = runs.get(runid);
	 					}
	 					if (count_and_rate) run.output.position_reports_count++;
					
	 					/*** Set the event distributor time ***/						
	 					event.distributorTime = (System.currentTimeMillis() - startOfSimulation)/new Double(1000);
					
	 					/*** Put the event into the event queue ***/
	 					ConcurrentLinkedQueue<PositionReport> eventqueue = eventqueues.contents.get(runid);
	 					if (eventqueue == null) {    
	 						eventqueue = new ConcurrentLinkedQueue<PositionReport>();
	 						eventqueues.contents.put(runid, eventqueue);		 				
	 					}
	 					eventqueue.add(event);	
	 					if (count_and_rate && eventqueue.size() > run.output.maxLengthOfEventQueue) run.output.maxLengthOfEventQueue = eventqueue.size();
	 					//System.out.println(event.toString());		
	 					
	 					//event_count++;
	 				}
	 			
	 				/*** Set distributer progress ***/	
	 				if (curr_sec < event.sec) {		
	 					
	 					//System.out.println(event_count + " events are processed at " + curr_sec);
	 					//event_count = 0;
	 				
	 					if (curr_sec>300) { // Avoid null run exception when the stream is read too fast
	 						eventqueues.setDistributorProgress(curr_sec);
	 						//if (curr_sec % 10 == 0) System.out.println("Distribution time of second " + curr_sec + " is " + now);
	 					}
	 					system_time = (System.currentTimeMillis() - startOfSimulation)/new Double(1000);
	 					distrFinishTimes.put(curr_sec, system_time);
	 					curr_sec = event.sec;
	 				}
	 			
	 				/*** Reset event ***/
	 				if (scanner.hasNextLine()) {		 				
	 					String line = scanner.nextLine();   
	 					event = PositionReport.parse(line);		 				
	 				} else {
	 					event = null;		 				
	 				}
	 			}		 			
	 			/*** Set distributor progress ***/		 					
	 			eventqueues.setDistributorProgress(batch.end);					
	 			system_time = (System.currentTimeMillis() - startOfSimulation)/new Double(1000);
				//System.out.println("Distribution time of second " + batch_limit + " is " + now);
				distrFinishTimes.put(batch.end, system_time);
				curr_sec = batch.end;
	 			
				if (batch.end < last_sec) { 			
 				
					/*** Sleep if now is smaller than batch_limit ms ***/
					system_time = System.currentTimeMillis() - startOfSimulation;
					//System.out.println("Skipped time is " + skipped_time + " sec.\nSystem time is " + system_time/1000);
					
					if (system_time < (batch.end - skipped_time)*1000) { // !!!
	 			
						int sleep_time = new Double((batch.end - skipped_time)*1000 - system_time).intValue(); // !!!	 			
						//System.out.println("Distributor sleeps " + sleep_time + " ms at " + curr_sec );		 			
						Thread.sleep(sleep_time);
						distributor_wakeup_time = (System.currentTimeMillis() - startOfSimulation)/1000 - (batch.end - skipped_time); // !!!
					} 
					
					/*** Rest batch_limit ***/
					double new_start = batch.end + 1;
					double new_end = batch.end + random.nextInt(max - min + 1) + min + distributor_wakeup_time;
					batch = new TimeInterval(new_start, new_end);
					if (batch.end > last_sec) batch.end = last_sec;
					//System.out.println("-------------------------\nBatch end: " + batch.end);
 				
					if (distributor_wakeup_time > 1) {
						System.out.println(	"Distributor wakeup time is " + distributor_wakeup_time + 
											". New batch is " + batch.toString() + ".");
					}	 				
				} else { /*** Terminate ***/	 				
					break;
				}						
	 		}	 		
		} catch (final InterruptedException e) { e.printStackTrace(); }
	} 	
}
