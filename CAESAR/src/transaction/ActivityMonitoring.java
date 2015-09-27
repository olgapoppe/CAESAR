package transaction;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import run.*;
import event.PositionReport;

public class ActivityMonitoring extends Transaction {
	
	Run run;	
	
	HashMap<Double,Double> distrFinishTimes;
	HashMap<Double,Double> schedStartTimes;
	
	AtomicBoolean accidentWarningsFailed;
	AtomicBoolean tollNotificationsFailed;
	
	int query_number;

	public ActivityMonitoring (Run r, ArrayList<PositionReport> eventList, 
			HashMap<RunID,Run> rs, long start,
			HashMap<Double,Double> distrFinishT, HashMap<Double,Double> schedStartT,
			AtomicInteger tet, AtomicBoolean awf, AtomicBoolean tnf,
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
		
		for (PositionReport event : events) {
			
			if (event == null) System.out.println("NULL EVENT!!!");
			if (run == null) System.out.println("NULL RUN!!!" + event.toString());
			
			run.activityMonitoring(query_number, event, 0, startOfSimulation, distrFinishTimes, schedStartTimes, accidentWarningsFailed, tollNotificationsFailed); 	
			run.collectGarbage(event.min);	
		}			
		
		// Count down the number of transactions
		transaction_number.countDown();			
	}
}
