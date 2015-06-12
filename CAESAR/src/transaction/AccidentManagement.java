package transaction;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.concurrent.atomic.AtomicBoolean;

import run.*;
import event.PositionReport;

public class AccidentManagement extends Transaction {
	
	boolean run_priorization;
	AtomicBoolean accidentWarningsFailed;
	
	public AccidentManagement (Run r, ArrayList<PositionReport> eventList, HashMap<RunID,Run> rs, long start, boolean rp, AtomicBoolean awf, HashMap<Double,Double> distrProgrPerSec) {
		super(r,eventList,rs,start,distrProgrPerSec);
		run_priorization = rp;
		accidentWarningsFailed = awf;
	}
	
	/**
	 * Execute these events by this run.
	 */	
	public void run() {
		
		double segWithAccAhead;
		
		for (PositionReport event : events) {			
		
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
			double distrTimeStamp = distributorProgressPerSec.get(event.sec);
			//System.out.println(event.sec + " " + distrTimeStamp);			
			run.accidentManagement(event, startOfSimulation, segWithAccAhead, run_priorization, accidentWarningsFailed, distrTimeStamp); 									
		}		
		// Count down the number of transactions
		transaction_number.countDown();
	}
}
