package transaction;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.concurrent.atomic.AtomicInteger;
import run.*;
import event.PositionReport;

public class ActivityMonitoring extends Transaction {
	
	Run run;
	
	int query_number;

	public ActivityMonitoring (Run r, ArrayList<PositionReport> eventList, 
			HashMap<RunID,Run> rs, long start,
			AtomicInteger tet, 
			int qn) {
		
		super(eventList,rs,start,tet);
		
		run = r;
		
		query_number = qn;
	}
	
	/**
	 * Execute these events by this run.
	 */	
	public void run() {	
			
		for (PositionReport event : events) {
				
			System.out.println(event.toString());					
		}			
		
		// Count down the number of transactions
		transaction_number.countDown();			
	}
}
