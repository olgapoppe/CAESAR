package transaction;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import run.*;
import event.*;
import iogenerator.*;

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
			AtomicDouble met, AtomicBoolean awf, AtomicBoolean tnf,
			int qn) {
		
		super(eventList,rs,start,met);
		
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
		double max_exe_time_in_this_transaction = 0;
						
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
			double app_time_start = (System.currentTimeMillis() - startOfSimulation)/new Double(1000);
			
			//System.out.println("Fake execute event " + event.toString() + " " + (query_number-1) + " times and really execute it afterwards.");
				
			// Query replication loop
			for (int i=1; i<query_number; i++) {
				
				int count = 0;
				while (count<10000) { count++; }
			
				//run.fake_trafficManagement(event, segWithAccAhead, startOfSimulation, distrFinishTimes, schedStartTimes, accidentWarningsFailed, tollNotificationsFailed); 	
				//run.fake_collectGarbage(event.min);	// Has effect only when called for the first time for this event 	
			}				
			run.trafficManagement(event, segWithAccAhead, startOfSimulation, distrFinishTimes, schedStartTimes, accidentWarningsFailed, tollNotificationsFailed); 	
			run.collectGarbage(event.min);
						
			double app_time_end = (System.currentTimeMillis() - startOfSimulation)/new Double(1000);			
			double exe_time = app_time_end - app_time_start;
			if (max_exe_time_in_this_transaction < exe_time) {
				max_exe_time_in_this_transaction = exe_time;	
				//System.out.println("Max exe time per transaction in second " + event.sec + " is " + exe_time);
			}
		}	
		// Increase maximal execution time
		if (max_exe_time.get() < max_exe_time_in_this_transaction) {
			max_exe_time.set(max_exe_time_in_this_transaction);
			//System.out.println("Max exe time becomes " + max_exe_time.get());
		}
		
		// Count down the number of transactions
		transaction_number.countDown();			
	}
}