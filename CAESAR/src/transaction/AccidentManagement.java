package transaction;

import java.util.ArrayList;
import java.util.HashMap;
import run.*;
import event.PositionReport;

public class AccidentManagement extends Transaction {
	
	boolean run_priorization;
	
	public AccidentManagement (Run r, ArrayList<PositionReport> eventList, HashMap<RunID,Run> rs, long start, boolean rp) {
		super(r,eventList,rs,start);
		run_priorization = rp;
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
			run.accidentManagement(event, startOfSimulation, segWithAccAhead, run_priorization); 									
		}		
		// Count down the number of transactions
		transaction_number.countDown();
	}
}