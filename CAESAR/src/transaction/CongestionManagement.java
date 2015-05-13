package transaction;

import java.util.ArrayList;
import java.util.HashMap;
import run.*;
import event.PositionReport;

public class CongestionManagement extends Transaction {
	
	public CongestionManagement (Run r, ArrayList<PositionReport> eventList, HashMap<RunID,Run> rs, long start) {
		super(r,eventList,rs,start);
	}
	
	/**
	 * Execute these events by this run.
	 */	
	public void run() {	
		
		double segWithAccAhead;
		
		for (PositionReport event : events) {
			
			if (run.vehicles.get(event.vid)==null) System.out.println(event.toString());
		
			// READ: If a new vehicle on a travel lane arrives, lookup accidents ahead
			if (run.vehicles.get(event.vid).appearance_sec == event.sec && event.lane < 4) {
				segWithAccAhead = (run.accidentsAhead.containsKey(event.min)) ? run.accidentsAhead.get(event.min) : -1;			
			} else {
				segWithAccAhead = -1;
			}
			// WRITE: Update this run and remove old data
			run.congestionManagement(event, startOfSimulation, segWithAccAhead); 	
			run.collectGarbage(event.min);					
		}		
		// Count down the number of transactions
		transaction_number.countDown();
	}
}
