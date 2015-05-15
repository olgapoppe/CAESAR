package distributor;

import java.util.HashMap;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicInteger;

import driver.EventQueue;
import run.*;
import event.*;

public class SingleQueueDistributor extends EventDistributor {
		
	public SingleQueueDistributor (AtomicInteger dp, EventQueue e, HashMap<RunID,Run> rs, RunQueues rq, AtomicInteger x1, AtomicInteger x2, int last) {
		super(dp, e, rs, rq, x1, x2, last);
	}

	/** 
	 * Read the input file, parse the events, 
	 * generate new runs if they do not exist yet and
	 * distribute events into run task queues.
	 */	
	public void run() {	
		
		// Local variables
		Double curr_sec = new Double(-1);
								
		while (curr_sec <= lastSec && events.getDriverProgress(curr_sec)) { // Get the permission to distribute curr_sec
					
			/**************************************** Event ****************************************/
			// First event
			PositionReport event = events.contents.peek();	
				
			while (event != null && event.sec <= curr_sec) {		
				
				events.contents.poll();	
				
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
					LinkedBlockingQueue<PositionReport> runtaskqueue = runqueues.contents.get(runid);
					if (runtaskqueue == null) {    
						runtaskqueue = new LinkedBlockingQueue<PositionReport>();
						runqueues.contents.put(runid, runtaskqueue);		 				
					}
					runtaskqueue.add(event);	 	
				}
				// Reset event
				event = events.contents.peek();						
			}
			// Set distributor progress and current second
			runqueues.setDistributorProgress(curr_sec);
			curr_sec++;
		}								
		System.out.println("Distributor is done.");		 						 
	}
}
