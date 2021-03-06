/*package transaction;

import iogenerator.AtomicDouble;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import run.*;
import event.*;

*//** 
 * A accident managing transaction processes all events in the event sequence by their respective run
 * using a fully optimized query plan containing queries related to accident.  
 * @author Olga Poppe
 *//*
public class AccidentManagement extends Transaction {
	
	Run run;
	boolean run_priorization;	
	AtomicBoolean accidentWarningsFailed;	
	
	public AccidentManagement (Run r, ArrayList<PositionReport> eventList, HashMap<RunID,Run> rs, long start, AtomicDouble met, boolean rp, 
			AtomicBoolean awf) {
		super(eventList,rs,start,met);
		run = r;
		run_priorization = rp;		
		accidentWarningsFailed = awf;		
	}
	
	*//**
	 * Execute these events by this run.
	 *//*	
	public void run() {
		
		double segWithAccAhead;
		
		for (PositionReport event : events) {	
			
			if (event == null) System.out.println("NULL EVENT!!!");
			if (run == null) System.out.println("NULL RUN!!!" + event.toString());	
		
			// READ: If a new vehicle on a travel lane arrives, lookup or update accidents ahead
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
			// WRITE: Update this run	
			//System.out.println(event.sec + " " + distrTimeStamp);	
			run.accidentManagement(event, scheduling_time, startOfSimulation, segWithAccAhead, run_priorization, accidentWarningsFailed); 									
		}		
		// Count down the number of transactions
		transaction_number.countDown();
	}
}
*/