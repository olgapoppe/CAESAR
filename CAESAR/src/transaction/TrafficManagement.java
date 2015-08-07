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
	
	AtomicBoolean accidentWarningsFailed;
	AtomicBoolean tollNotificationsFailed;
	
	public TrafficManagement (Run r, ArrayList<PositionReport> eventList, HashMap<RunID,Run> rs, long start,
			AtomicDouble met, AtomicBoolean awf, AtomicBoolean tnf, HashMap<Double,Long> distrProgrPerSec) {
		super(eventList,rs,start,met,distrProgrPerSec);
		run = r;
		accidentWarningsFailed = awf;
		tollNotificationsFailed = tnf;			
	}
		
	/**
	 * Execute these events by this run.
	 */	
	public void run() {	
			
		double segWithAccAhead;
		double max_exe_time_in_this_transaction = 0;
		double sec = 0;
		
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
			double app_time_start = (System.currentTimeMillis() - startOfSimulation)/new Double(1000);
			
			run.trafficManagement(event, startOfSimulation, segWithAccAhead, accidentWarningsFailed, tollNotificationsFailed, distrProgr); 	
			run.collectGarbage(event.min);
			
			double app_time_end = (System.currentTimeMillis() - startOfSimulation)/new Double(1000);			
			double exe_time = app_time_end - app_time_start;
			if (max_exe_time_in_this_transaction < exe_time) max_exe_time_in_this_transaction = exe_time;
			
			sec = event.sec;
		}	
		// Increase maximal execution time
		if (max_exe_time.get() < max_exe_time_in_this_transaction) max_exe_time.set(max_exe_time_in_this_transaction);
		
		// Count down the number of transactions
		transaction_number.countDown();		
		
		/*double now = (System.currentTimeMillis() - startOfSimulation)/new Double(1000);
		if (now>sec && transaction_number.getCount()==0) {			
			System.out.println("Executor is done with sec: " + sec + " at: " + now);
		}*/
	}
}