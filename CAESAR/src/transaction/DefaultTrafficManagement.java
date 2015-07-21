package transaction;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Set;
import java.util.Vector;
import java.util.concurrent.atomic.AtomicBoolean;
import run.*;
import event.*;

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
		   
		// Update of avgSpd
		Run run = runLookUp(event); // RL 0		
		if (run.time.min < event.min) // FI 
			run.avgSpd = default_getAvgSpdFor5Min(event,event.min); // HU		
		
		// Update of min
		runLookUp(event); // RL 1	
		if (run.time.min < event.min) // FI 
			run.time.min = event.min; // TU
		
		// Update of sec
		runLookUp(event); // RL 2		
		run.time.sec = event.sec; // TU
						
		/************************************************* If the vehicle is new in the segment *************************************************/
		// New car detection
		runLookUp(event); // RL 3
		
		if (run.vehicles.get(event.vid) == null) { // FI
			
			NewCar newCar = new NewCar(event); // ED
					
			// Update of vehicles
			runLookUp(event); // RL 4
			
			Vehicle newVehicle = new Vehicle (event);
			Vector<Double> new_speeds_per_min = new Vector<Double>();
			new_speeds_per_min.add(event.spd); 
			
			newVehicle.spds.put(event.min,new_speeds_per_min);
			run.vehicles.put(event.vid,newVehicle); // HU  	   

			// Update of vehCounts
			runLookUp(event); // RL 5
			
			double new_count = run.vehCounts.containsKey(next_min) ? run.vehCounts.get(next_min)+1 : 1;
			run.vehCounts.put(next_min, new_count); // HU
					    
			// New traveling car detection
			if (event.lane < 4) { // FI  
				
				NewCar newTravelingCar = new NewTravelingCar(event); // ED
				
				// Accident ahead detection
				runLookUp(event); // RL 13
				
				double segWithAccAhead;
				if (event.min > run.time.minOfLastUpdateOfAccidentAhead) {
					
					segWithAccAhead = run.getSegWithAccidentAhead(runs, event); // FI
					if (segWithAccAhead!=-1) run.accidentsAhead.put(event.min, segWithAccAhead); // HU
					run.time.minOfLastUpdateOfAccidentAhead = event.min;
				} else {
					segWithAccAhead = (run.accidentsAhead.containsKey(event.min)) ? run.accidentsAhead.get(event.min) : -1;
				}	
				
				// Context update
				runLookUp(event); // RL 14
				boolean accident = segWithAccAhead != -1; // FI
				run.context = "accident";
				
				runLookUp(event); // RL 15
				boolean congestion = !accident && run.congested(event.min);
				run.context = "congestion";
				
				runLookUp(event); // RL 16
				boolean clear = !accident && !run.congested(event.min);
				run.context = "clear";
				
				// Toll notification derivation
				TollNotification tollNotification;			
						
				if 	(congestion) { // FI
					
					runLookUp(event); // RL 12		
					double vehCount = run.lookUpVehCount(event.min);
					tollNotification = new TollNotification(event, run.avgSpd, vehCount, startOfSimulation, tollNotificationsFailed, distrProgr); // ED	
					run.output.tollNotifications.add(tollNotification);
				} 
				
				if (clear) {
					
					runLookUp(event); // RL 6					
					tollNotification = new TollNotification(event, run.avgSpd, startOfSimulation, tollNotificationsFailed, distrProgr);	// ED
					run.output.tollNotifications.add(tollNotification);
				}
				
				if (accident) {
					
					runLookUp(event); // RL 7				
					tollNotification = new TollNotification(event, run.avgSpd, startOfSimulation, tollNotificationsFailed, distrProgr);	// ED	
					run.output.tollNotifications.add(tollNotification);
				}		
				
				// Accident warning derivation
				if (accident) { // FI		
					
					runLookUp(event); // RL 8					
					AccidentWarning accidentWarning = new AccidentWarning(event, segWithAccAhead, startOfSimulation, accidentWarningsFailed, distrProgr); // ED
					run.output.accidentWarnings.add(accidentWarning);				
				}			
			}
		/*********************************************** If the vehicle was in the segment before ***********************************************/
		} else {	
			
			// Old car detection
			runLookUp(event); // RL 9
			
			if (run.vehicles.get(event.vid) != null) { // FI
				OldCar oldCar = new OldCar(event); // ED
			}	
			
			Vehicle vehicle = run.vehicles.get(event.vid);
				
			// Update of existingVehicle: time
			runLookUp(event); // RL 18		
			vehicle.sec = event.sec; // TU
			
			runLookUp(event); // RL 17
			if (event.min > vehicle.min) { // FI				
				vehicle.min = event.min; // TU
			}
			
			// Update of vehCounts
			runLookUp(event); // RL 11
			if (event.min > vehicle.min) { // FI
				
				double new_count = run.vehCounts.containsKey(next_min) ? run.vehCounts.get(next_min)+1 : 1;
				run.vehCounts.put(next_min, new_count);	// HU
			}
			
			// Update of existingVehicle: spd, spds
			runLookUp(event); // RL 10
			
			vehicle.spd = event.spd; // HU
			if (vehicle.spds.containsKey(event.min)) {    

				vehicle.spds.get(event.min).add(event.spd); // HU		
					
			} else {             					
			
				Vector<Double> new_speeds_per_min = new Vector<Double>();
				new_speeds_per_min.add(event.spd);
				vehicle.spds.put(event.min, new_speeds_per_min); // HU		
			}		
		} 			
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
				double spd = vehicle.default_getAvgSpd(min);
				if (spd>-1) {
					sum += spd;	
					count++;
		}}}
		return (sum==0 && count==0) ? -1 : sum/count;		
	}

	/**
	 * Get the run with the same identifier as the input event
	 * @param position report
	 * @return run
	 */	
	public Run runLookUp (PositionReport event) {
		RunID runid = new RunID(event.xway,event.dir,event.seg); 
		return runs.get(runid);
	}
}
