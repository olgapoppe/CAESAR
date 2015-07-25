package transaction;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import run.*;
import event.PositionReport;

/** 
 * A traffic managing transaction processes all events in the event sequence by their respective run
 * using a fully optimized query plan containing all queries.  
 * @author Olga Poppe
 */
public class TrafficManagement extends Transaction {
	
	Run run;
	AtomicBoolean accidentWarningsFailed;
	AtomicBoolean tollNotificationsFailed;
		
	public TrafficManagement (Run r, ArrayList<PositionReport> eventList, HashMap<RunID,Run> rs, long start, 
			AtomicBoolean awf, AtomicBoolean tnf, AtomicInteger max_late, HashMap<Double,Long> distrProgrPerSec) {
		super(eventList,rs,start,distrProgrPerSec, max_late);
		run = r;
		accidentWarningsFailed = awf;
		tollNotificationsFailed = tnf;		
	}
		
	/**
	 * Execute these events by this run.
	 */	
	public void run() {	
			
		double segWithAccAhead;
		
		for (PositionReport event : events) {
				
			if (event == null) System.out.println("NULL EVENT!!!");
			if (run == null) System.out.println("NULL RUN!!!" + event.toString());			
						
			// READ: If a new vehicle on a travel lane arrives, lookup accidents ahead
			if (run.vehicles.get(event.vid) == null && event.lane < 4) {
					
				if (event.min > run.time.minOfLastUpdateOfAccidentAhead) {
						
					segWithAccAhead = run.getSegWithAccidentAhead(runs, event);
					if (segWithAccAhead!=-1) run.accidentsAhead.put(event.min, segWithAccAhead);
					run.time.minOfLastUpdateOfAccidentAhead = event.min;
				} else {
					segWithAccAhead = (run.accidentsAhead.containsKey(event.min)) ? run.accidentsAhead.get(event.min) : -1;
				}			
			} else {
				segWithAccAhead = -1;
			}
			// WRITE: Update this run and remove old data
			long distrProgr = distributorProgressPerSec.get(event.sec);
			run.trafficManagement(event, startOfSimulation, segWithAccAhead, accidentWarningsFailed, tollNotificationsFailed, max_latency, distrProgr); 	
			run.collectGarbage(event.min);					
		}		
		// Count down the number of transactions
		transaction_number.countDown();
	}
}