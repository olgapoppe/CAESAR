package transaction;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.concurrent.atomic.AtomicBoolean;

import run.*;
import event.PositionReport;

public class CongestionManagement extends Transaction {
	
	AtomicBoolean tollNotificationsFailed;
	
	public CongestionManagement (Run r, ArrayList<PositionReport> eventList, HashMap<RunID,Run> rs, long start, AtomicBoolean tnf, HashMap<Double,Long> distrProgrPerSec) {
		super(r,eventList,rs,start,distrProgrPerSec);
		tollNotificationsFailed = tnf;
	}
	
	/**
	 * Execute these events by this run.
	 */	
	public void run() {	
		
		double segWithAccAhead;
		
		for (PositionReport event : events) {
			
			if (event == null) System.out.println("NULL EVENT!!!");
			if (run == null) System.out.println("NULL RUN!!!");			
			if (run.vehicles.get(event.vid)==null) System.out.println("NO SUCH VEHICLE!!!");
		
			// READ: If a new vehicle on a travel lane arrives, lookup accidents ahead
			if (run.vehicles.get(event.vid).appearance_sec == event.sec && event.lane < 4) {
				segWithAccAhead = (run.accidentsAhead.containsKey(event.min)) ? run.accidentsAhead.get(event.min) : -1;			
			} else {
				segWithAccAhead = -1;
			}
			// WRITE: Update this run and remove old data
			double distrTimeStamp = distributorProgressPerSec.get(event.sec);
			run.congestionManagement(event, startOfSimulation, segWithAccAhead, tollNotificationsFailed, distrTimeStamp); 	
			run.collectGarbage(event.min);					
		}		
		// Count down the number of transactions
		transaction_number.countDown();
	}
}
