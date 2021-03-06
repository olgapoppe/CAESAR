/*package transaction;

import iogenerator.AtomicDouble;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import run.*;
import event.*;

*//** 
 * A congestion managing transaction processes all events in the event sequence by their respective run
 * using a fully optimized query plan containing queries related to congestion.  
 * @author Olga Poppe
 *//*
public class CongestionManagement extends Transaction {
	
	Run run;	
	AtomicBoolean tollNotificationsFailed;
		
	public CongestionManagement (Run r, ArrayList<PositionReport> eventList, HashMap<RunID,Run> rs, long start,
			AtomicDouble met, AtomicBoolean tnf) {
		super(eventList,rs,start,met);
		run = r;		
		tollNotificationsFailed = tnf;		
	}
	
	*//**
	 * Execute these events by this run.
	 *//*	
	public void run() {	
		
		double segWithAccAhead;
		
		for (PositionReport event : events) {
			
			if (event == null) System.out.println("NULL EVENT!!!");
			if (run == null) System.out.println("NULL RUN!!!" + event.toString());			
			if (run.vehicles.get(event.vid) == null) System.out.println("NO SUCH VEHICLE!!!" + event.toString());
		
			// READ: If a new vehicle on a travel lane arrives, lookup accidents ahead
			if (run.vehicles.get(event.vid).appearance_sec == event.sec && event.lane < 4) {
				segWithAccAhead = (run.accidentsAhead.containsKey(event.min)) ? run.accidentsAhead.get(event.min) : -1;			
			} else {
				segWithAccAhead = -1;
			}
			// WRITE: Update this run and remove old data
			run.congestionManagement(event, delay, startOfSimulation, segWithAccAhead, tollNotificationsFailed); 	
			run.collectGarbage(event.min);					
		}		
		// Count down the number of transactions
		transaction_number.countDown();
	}
}
*/