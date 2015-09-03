package transaction;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import run.*;
import event.*;


/** 
 * A traffic managing transaction processes all events in the event sequence by their respective run
 * using a fully optimized query plan containing all queries.  
 * @author Olga Poppe
 */
public class TrafficManagement extends Transaction {
	
	Run run;
	
	HashMap<Double,Double> distrFinishTimes;
	HashMap<Double,Double> schedStartTimes;
	
	AtomicBoolean accidentWarningsFailed;
	AtomicBoolean tollNotificationsFailed;
	
	int query_number;
	
	public TrafficManagement (Run r, ArrayList<PositionReport> eventList, 
			HashMap<RunID,Run> rs, long start,
			HashMap<Double,Double> distrFinishT, HashMap<Double,Double> schedStartT,
			AtomicLong tet, AtomicBoolean awf, AtomicBoolean tnf,
			int qn) {
		
		super(eventList,rs,start,tet);
		
		run = r;
		
		distrFinishTimes = distrFinishT;
		schedStartTimes = schedStartT;
		
		accidentWarningsFailed = awf;
		tollNotificationsFailed = tnf;	
		
		query_number = qn;
	}
		
	/**
	 * Execute these events by this run.
	 */	
	public void run() {	
			
		double segWithAccAhead;				
		long start = System.currentTimeMillis() - startOfSimulation;
						
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
			//System.out.println("Execute event " + event.toString());				
			// Query replication loop
			//for (int i=1; i<query_number; i++) {
				
			try { Thread.sleep(query_number*5); } 
			catch (InterruptedException e) { e.printStackTrace(); }
			
				//run.fake_trafficManagement(event, segWithAccAhead, startOfSimulation, distrFinishTimes, schedStartTimes, accidentWarningsFailed, tollNotificationsFailed); 	
				//run.fake_collectGarbage(event.min);	// Has effect only when called for the first time for this event 	
			//}				
			run.trafficManagement(event, segWithAccAhead, startOfSimulation, distrFinishTimes, schedStartTimes, accidentWarningsFailed, tollNotificationsFailed); 	
			run.collectGarbage(event.min);			
		}	
		long end = System.currentTimeMillis() - startOfSimulation;			
		long duration = (end - start)/new Long(1000); // convert ms to sec		
		total_exe_time.set(total_exe_time.get() + duration);		
		
		// Count down the number of transactions
		transaction_number.countDown();			
	}
}