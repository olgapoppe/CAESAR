package distributor;

import java.util.HashMap;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicInteger;
import run.*;
import event.*;

public class SingleQueueDistributor extends EventDistributor {
	
	public SingleQueueDistributor (AtomicInteger dp, LinkedBlockingQueue<PositionReport> e, HashMap<RunID,Run> rs, HashMap<RunID,LinkedBlockingQueue<PositionReport>> rtq, 
								   AtomicInteger x1, AtomicInteger x2) {
		super(dp, e, rs, rtq, x1, x2);
	}

	/** 
	 * Read the input file, parse the events, 
	 * generate new runs if they do not exist yet and
	 * distribute events into run task queues.
	 */	
	public void run() {	
		
		try {
			/*** Local variables ***/
			double curr_min = 0;		
			Double curr_sec = new Double(-1);
			int event_count = 0;
			
			while (!shutdown) {					 			
				while (!events.isEmpty()) {
			
					/******************************************* Event *******************************************/
					PositionReport event = events.poll();  
   			 	
					/*** Current minute ***/
					if (event.min > curr_min) {   			 		
						System.out.println("Minute: " + event.min);
						curr_min = event.min;  			 		   			 		
					}   		 		
					if (event.type == 0) {
		 			
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
						/************************************* Run task queues *************************************/
						LinkedBlockingQueue<PositionReport> runtaskqueue = runtaskqueues.get(runid);
						if (runtaskqueue == null) {    
							runtaskqueue = new LinkedBlockingQueue<PositionReport>();
							runtaskqueues.put(runid, runtaskqueue);		 				
						}
						runtaskqueue.add(event);	 			
		 			
						/*** Max number of stored events per run ***/
						int size = runtaskqueue.size();
						if (run.output.maxNumberOfStoredEvents < size) run.output.maxNumberOfStoredEvents = size;	 
		 			
						if (event.sec > curr_sec) {			 				
		 						 				
							/********************************** Distributer progress **********************************/
							distributorProgress.set(curr_sec.intValue());
		 							 					
							/*** Min and max stream rate ***/
							if (curr_sec >= 0) {
								if (min_stream_rate > event_count) min_stream_rate = event_count;
								if (max_stream_rate < event_count) max_stream_rate = event_count;
							}		 				
							curr_sec = event.sec;
							event_count = 1;
		 				
						} else { 
							event_count++;
						}	 			
					}
				}
				/************************************** Last second in the batch **************************************/
				distributorProgress.set(curr_sec.intValue());
				
				System.out.println("Distributor: " + curr_sec);
		 	
				/*** Min and max stream rate ***/
				if (min_stream_rate > event_count) min_stream_rate = event_count;
				if (max_stream_rate < event_count) max_stream_rate = event_count;	
		 	
				Thread.sleep(2000); // how long??? 	
			}	
		} catch (InterruptedException e1) { e1.printStackTrace(); } 						 
	}
}
