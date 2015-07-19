package transaction;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Set;
import java.util.Vector;
import java.util.concurrent.atomic.AtomicBoolean;

import run.Run;
import run.RunID;
import run.Vehicle;
import event.PositionReport;
import event.TollNotification;

public class DefaultTrafficManagement extends Transaction {	
	
	AtomicBoolean accidentWarningsFailed;
	AtomicBoolean tollNotificationsFailed;
		
	public DefaultTrafficManagement (ArrayList<PositionReport> eventList, HashMap<RunID,Run> rs, long start, 
			AtomicBoolean awf, AtomicBoolean tnf, HashMap<Double,Long> distrProgrPerSec) {
		super(eventList,rs,start,distrProgrPerSec);		
		accidentWarningsFailed = awf;
		tollNotificationsFailed = tnf;
	}
		
	/**
	 * Execute these events by this run.
	 */	
	public void run() {	
			
		for (PositionReport event : events) {
			
			long distrProgr = distributorProgressPerSec.get(event.sec);			
			defaultTrafficManagement(event, startOfSimulation, accidentWarningsFailed, tollNotificationsFailed, distrProgr);								
		}		
		// Count down the number of transactions
		transaction_number.countDown();
	}
	
	public void defaultTrafficManagement (PositionReport event, long startOfSimulation, AtomicBoolean accidentWarningsFailed, AtomicBoolean tollNotificationsFailed, long distrProgr) {
		
		// Set auxiliary variables
		double next_min = event.min+1;
		   
		// History update: avgSpd
		RunID runid = new RunID(event.xway,event.dir,event.seg); // RL
		Run run = runs.get(runid);		
		if (run.time.min < event.min) // FI 
			run.avgSpd = default_getAvgSpdFor5Min(event,event.min); // HU		
		
		// History update: min
		RunID runid1 = new RunID(event.xway,event.dir,event.seg); // RL
		Run run1 = runs.get(runid1);	
		if (run1.time.min < event.min) // FI 
			run1.time.min = event.min; // TU
		
		// History update: sec
		RunID runid2 = new RunID(event.xway,event.dir,event.seg); // RL
		Run run2 = runs.get(runid2);		
		run2.time.sec = event.sec; // TU
						
		// Get previous info about the vehicle
		Vehicle vehicle = vehicles.get(event.vid);
				
		/************************************************* If the vehicle is new in the segment *************************************************/
		if (vehicle.appearance_sec == event.sec) {
					
			// Update vehicle speeds and vehCounts
			Vector<Double> new_speeds_per_min = new Vector<Double>();
			new_speeds_per_min.add(event.spd);
			vehicle.spds.put(event.min,new_speeds_per_min);			    	   

			double new_count = vehCounts.containsKey(next_min) ? vehCounts.get(next_min)+1 : 1;
			vehCounts.put(next_min, new_count);
					    
			// Derive complex events
			if (event.lane < 4) {    
						
				TollNotification tollNotification;
						
				if 	(!isAccident && congested(event.min)) { 
		
					double vehCount = lookUpVehCount(event.min);
					tollNotification = new TollNotification(event, avgSpd, vehCount, startOfSimulation, tollNotificationsFailed, distrProgr); 
							
				} else {
					tollNotification = new TollNotification(event, avgSpd, startOfSimulation, tollNotificationsFailed, distrProgr);				
				}
				output.tollNotifications.add(tollNotification);
			}
		/*********************************************** If the vehicle was in the segment before ***********************************************/
		} else {			
				
			// Update vehCounts
			// Update existingVehicle: time
			vehicle.sec = event.sec;    		
			if (event.min > vehicle.min) {
						
				vehicle.min = event.min;
				
				double new_count = vehCounts.containsKey(next_min) ? vehCounts.get(next_min)+1 : 1;
				vehCounts.put(next_min, new_count);			
			}
			// Update existingVehicle: spd, spds
			vehicle.spd = event.spd;
			if (vehicle.spds.containsKey(event.min)) {    

				vehicle.spds.get(event.min).add(event.spd);    			
					
			} else {             					
			
				Vector<Double> new_speeds_per_min = new Vector<Double>();
				new_speeds_per_min.add(event.spd);
				vehicle.spds.put(event.min, new_speeds_per_min);			
			}		
		} 	
		//event.executorTime = (System.currentTimeMillis() - startOfSimulation)/1000;		
	}
	
	/**
	 * Compute the rolling average speed of all vehicles for the last 5 minutes
	 * @param min
	 * @return rolling average speed
	 */
	public double default_getAvgSpdFor5Min (PositionReport event, double min) {
		
		double sum = 0;
		double count = 0;
		// Look-up or compute average speeds for 5 minutes before
		for (int i=1; i<=5 && min-i>0; i++) {	
			double avgSpdPerMin = default_lookUpOrComputeAvgSpd(event,min-i);			
			if (avgSpdPerMin>-1) {
				sum += avgSpdPerMin;	
				count++;			
		}}
		return (min==1 || (sum==0 && count==0)) ? -1 : sum/count;	
	}
	
	/**
	 * Look up or compute average speed of all vehicles for a given minute 
	 * @param min
	 * @return average speed
	 */
	public double default_lookUpOrComputeAvgSpd (PositionReport event, double min) {
		
		RunID runid = new RunID(event.xway,event.dir,event.seg); // RL
		Run run = runs.get(runid); 
		
		double result = 0;
		
		if (min > run.time.min) { // FI
			if (run.avgSpds.containsKey(min)) {
				result = run.avgSpds.get(min); // PR
			} else {
				result = default_getAvgSpd(event, min);
				run.avgSpds.put(min,result); // HU
		}}
		return result;
	}
	
	/**
	 * Compute average speed of all vehicles for a given minute
	 * @param min
	 * @return average speed
	 */
	public double default_getAvgSpd (PositionReport event, double min) {
		
		RunID runid = new RunID(event.xway,event.dir,event.seg); // RL
		Run run = runs.get(runid);
		
		double sum = 0;
		double count = 0;		
		Set<Double> vids = run.vehicles.keySet();	
		
		if (min > run.time.min) { // FI
			for (Double vid : vids) {				
				Vehicle vehicle = run.vehicles.get(vid);
				double spd = vehicle.getAvgSpd(min);
				if (spd>-1) {
					sum += spd;	
					count++;
		}}}
		return (sum==0 && count==0) ? -1 : sum/count;		
	}	
}
