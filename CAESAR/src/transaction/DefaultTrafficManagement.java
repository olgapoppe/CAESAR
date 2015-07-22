package transaction;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Set;
import java.util.Vector;
import java.util.concurrent.atomic.AtomicBoolean;

import accident.AccidentLocation;
import run.*;
import event.*;

/** 
 * A traffic managing transaction processes all events in the event sequence by their respective run
 * using a non-optimized or partially optimized query plan containing all queries.  
 * Optimization techniques can be turned on and off.
 * @author Olga Poppe
 */
public class DefaultTrafficManagement extends Transaction {	
	
	AtomicBoolean accidentWarningsFailed;
	AtomicBoolean tollNotificationsFailed;
	
	boolean event_derivation_omission;
	boolean early_mandatory_projections;
	boolean early_condensed_filtering;
	boolean reduced_stream_history_traversal;
		
	public DefaultTrafficManagement (ArrayList<PositionReport> eventList, HashMap<RunID,Run> rs, long start, 
			AtomicBoolean awf, AtomicBoolean tnf, HashMap<Double,Long> distrProgrPerSec,
			boolean ed, boolean pr, boolean fi, boolean sh) {
		
		super(eventList,rs,start,distrProgrPerSec);	
		
		accidentWarningsFailed = awf;
		tollNotificationsFailed = tnf;
		
		event_derivation_omission = ed;
		early_mandatory_projections = pr;
		early_condensed_filtering = fi;
		reduced_stream_history_traversal = sh;		
	}
		
	/**
	 * Execute these events by their respective run.
	 */	
	public void run() {	
			
		for (PositionReport event : events) {
			
			// WRITE: Update the respective run and remove old data
			long distrProgr = distributorProgressPerSec.get(event.sec);			
			defaultTrafficManagement(event, startOfSimulation, accidentWarningsFailed, tollNotificationsFailed, distrProgr); // includes garbage collection							
		}		
		// Count down the number of transactions
		transaction_number.countDown();
	}
	
	/************************************************* Traffic management *************************************************/
	/**
	 * defaultTrafficManagement maintains stopped vehicles, detects accidents and their clearance and derives accident warnings (like accidentManagement) and 
	 * 					 		maintains average speeds and vehicle counts and derives toll notifications (like congestionManagement).   
	 * @param event					incoming position report
	 * @param startOfSimulation 	start of simulation
	 * @param accidentWarningsFailed whether accident warnings failed the constraints already
	 * @param tollNotificationsFailed whether toll notifications failed the constraints already
	 * @param distrProgr			distributor progress in event.sec
	 */
	public void defaultTrafficManagement (PositionReport event, long startOfSimulation, AtomicBoolean accidentWarningsFailed, AtomicBoolean tollNotificationsFailed, long distrProgr) {
		
		// Set auxiliary variables
		double next_min = event.min+1;
		   
		// Update of avgSpd and min
		Run run = runLookUp(event); // RL 0	
		
		if (!early_condensed_filtering) {
			if (run.time.min < event.min) { // FI 
				run.avgSpd = default_getAvgSpdFor5Min(run, event, event.min); // HU	
			}			
			runLookUp(event); // RL 1	
			if (run.time.min < event.min) { // FI 
				run.time.min = event.min; // TU
			}
		} else {
			if (run.time.min < event.min) { // FI 
				run.avgSpd = default_getAvgSpdFor5Min(run, event, event.min); // HU		
				run.time.min = event.min; // TU
			} 
		}
		// Update of sec
		if (!early_condensed_filtering) runLookUp(event); // RL 2		
		run.time.sec = event.sec; // TU
						
		/************************************************* If the vehicle is new in the segment *************************************************/
		// New car detection
		if (!early_condensed_filtering) runLookUp(event); // RL 3
		
		if (run.vehicles.get(event.vid) == null) { // FI
			
			if (!event_derivation_omission) {
				NewCar newCar = new NewCar(event); // ED
			}
					
			// Update of vehicles
			if (!early_condensed_filtering) runLookUp(event); // RL 4
			
			Vehicle newVehicle = new Vehicle (event);
			Vector<Double> new_speeds_per_min = new Vector<Double>();
			new_speeds_per_min.add(event.spd); 
			
			newVehicle.spds.put(event.min,new_speeds_per_min);
			run.vehicles.put(event.vid,newVehicle); // HU  	   

			// Update of vehCounts
			if (!early_condensed_filtering) runLookUp(event); // RL 5
			
			double new_count = run.vehCounts.containsKey(next_min) ? run.vehCounts.get(next_min)+1 : 1;
			run.vehCounts.put(next_min, new_count); // HU
					    
			// New traveling car detection
			if (event.lane < 4) { // FI  
				
				if (!event_derivation_omission) {
					NewCar newTravelingCar = new NewTravelingCar(event); // ED
				}
				
				// Accident ahead detection
				if (!early_condensed_filtering) runLookUp(event); // RL 13
				
				double segWithAccAhead;
				if (!reduced_stream_history_traversal) {
					
					if (event.min > run.time.minOfLastUpdateOfAccidentAhead) { // FI
						
						segWithAccAhead = run.default_getSegWithAccidentAhead(runs, event); // RL, FI
						if (segWithAccAhead!=-1) run.accidentsAhead.put(event.min, segWithAccAhead); // HU
						run.time.minOfLastUpdateOfAccidentAhead = event.min;
					} else {
						segWithAccAhead = (run.accidentsAhead.containsKey(event.min)) ? run.accidentsAhead.get(event.min) : -1; // PR
					}					
				} else {
					if (event.min > run.time.minOfLastUpdateOfAccidentAhead) { // FI
					
						segWithAccAhead = run.getSegWithAccidentAhead(runs, event); // RL, FI
						if (segWithAccAhead!=-1) run.accidentsAhead.put(event.min, segWithAccAhead); // HU
						run.time.minOfLastUpdateOfAccidentAhead = event.min;
					} else {
						segWithAccAhead = (run.accidentsAhead.containsKey(event.min)) ? run.accidentsAhead.get(event.min) : -1; // PR
				}}	
				
				// Context update
				if (!early_condensed_filtering) runLookUp(event); // RL 14
				boolean accident = segWithAccAhead != -1; // FI
				run.context = "accident";
				
				if (!early_condensed_filtering) runLookUp(event); // RL 15
				boolean congestion = !accident && run.congested(event.min);
				run.context = "congestion";
				
				if (!early_condensed_filtering) runLookUp(event); // RL 16
				boolean clear = !accident && !run.congested(event.min);
				run.context = "clear";
				
				// Toll notification derivation
				TollNotification tollNotification;			
						
				if 	(congestion) { // CW
					
					if (!early_condensed_filtering) runLookUp(event); // RL 12		
					double vehCount = run.lookUpVehCount(event.min);
					tollNotification = new TollNotification(event, run.avgSpd, vehCount, startOfSimulation, tollNotificationsFailed, distrProgr); // ED	
					run.output.tollNotifications.add(tollNotification);
				} 
				
				if (clear) { // CW
					
					if (!early_condensed_filtering) runLookUp(event); // RL 6					
					tollNotification = new TollNotification(event, run.avgSpd, startOfSimulation, tollNotificationsFailed, distrProgr);	// ED
					run.output.tollNotifications.add(tollNotification);
				}
				
				if (accident) { // CW
					
					if (!early_condensed_filtering) runLookUp(event); // RL 7				
					tollNotification = new TollNotification(event, run.avgSpd, startOfSimulation, tollNotificationsFailed, distrProgr);	// ED	
					run.output.tollNotifications.add(tollNotification);
				}		
				
				// Accident warning derivation
				if (accident) { // CW
					
					if (!early_condensed_filtering) runLookUp(event); // RL 8					
					AccidentWarning accidentWarning = new AccidentWarning(event, segWithAccAhead, startOfSimulation, accidentWarningsFailed, distrProgr); // ED
					run.output.accidentWarnings.add(accidentWarning);				
				}			
			}
		/*********************************************** If the vehicle was in the segment before ***********************************************/
		} else {	
			
			// Old car detection
			if (!early_condensed_filtering) {
				
				runLookUp(event); // RL 9
				
				if (run.vehicles.get(event.vid) != null) { // FI 
					if (!event_derivation_omission) {						
						OldCar oldCar = new OldCar(event); // ED
			}}} else {
				if (!event_derivation_omission) {						
					OldCar oldCar = new OldCar(event); // ED
				}
			}
			
			// Get previous info about the vehicle
			Vehicle existingVehicle = run.vehicles.get(event.vid);
				
			// Update of existingVehicle: time
			if (!early_condensed_filtering) runLookUp(event); // RL 18		
			existingVehicle.sec = event.sec; // TU
			
			// Update of vehCounts
			if (!early_condensed_filtering) {
				
				runLookUp(event); // RL 11
			
				if (event.min > existingVehicle.min) { // FI 				
					double new_count = run.vehCounts.containsKey(next_min) ? run.vehCounts.get(next_min)+1 : 1;
					run.vehCounts.put(next_min, new_count);	// HU
				}
				runLookUp(event); // RL 17
				
				if (event.min > existingVehicle.min) { // FI					
					existingVehicle.min = event.min; // TU
				}
			} else {
				
				if (event.min > existingVehicle.min) { // FI					
					double new_count = run.vehCounts.containsKey(next_min) ? run.vehCounts.get(next_min)+1 : 1;
					run.vehCounts.put(next_min, new_count);	// HU
					
					existingVehicle.min = event.min; // TU
			}}
			
			// Update of existingVehicle: spd, spds
			if (!early_condensed_filtering) runLookUp(event); // RL 10
			
			existingVehicle.spd = event.spd; // HU
			if (existingVehicle.spds.containsKey(event.min)) {    

				existingVehicle.spds.get(event.min).add(event.spd); // HU		
					
			} else {             					
			
				Vector<Double> new_speeds_per_min = new Vector<Double>();
				new_speeds_per_min.add(event.spd);
				existingVehicle.spds.put(event.min, new_speeds_per_min); // HU		
			}	
			
			// Accident detection and clearance	
			// Update stoppedVehicles
			// Update existingVehicle: count, lane, pos
			if (existingVehicle.pos == event.pos && existingVehicle.lane == event.lane) { // Same position is reported, FI    
				
				// Same position derivation
				if (!early_condensed_filtering) runLookUp(event); // RL 19
				if (!event_derivation_omission) {
					SamePos samePos = new SamePos(event); // ED
				}

				// Update count of the existing vehicle
				if (!early_condensed_filtering) runLookUp(event); // RL 21
				existingVehicle.count++; // HU
				
				// Update stopped vehicles
				if (!early_condensed_filtering) runLookUp(event); // RL 22

				// Add new stopped vehicle
				AccidentLocation accidentLocation = new AccidentLocation (event.lane, event.pos);
				if (existingVehicle.count == 4 && existingVehicle.lane > 0 && existingVehicle.lane < 4)  { // FI

					StoppedVehicle stopped_vehicle = new StoppedVehicle(event.vid, event.sec);

					if (run.stoppedVehicles.containsKey(accidentLocation)) {
				
						Vector<StoppedVehicle> stopped_vehicles = run.stoppedVehicles.get(accidentLocation);
						if (stopped_vehicles.size() < 2) { 
							stopped_vehicles.add(stopped_vehicle); // HU
							
							// Accident detection
							if (!early_condensed_filtering) runLookUp(event); // RL 23
							run.toAccident(event, startOfSimulation, false); // FI, HU
						}    					
					} else {
						Vector<StoppedVehicle> stopped_vehicles = new Vector<StoppedVehicle>();
						stopped_vehicles.add(stopped_vehicle);
						run.stoppedVehicles.put(accidentLocation,stopped_vehicles); // HU
				}}
				// Update second of previously detected stopped vehicle
				if (existingVehicle.count > 4 && existingVehicle.lane > 0 && existingVehicle.lane < 4)  { // FI

					if (!early_condensed_filtering) runLookUp(event); // RL 24
					StoppedVehicle stopped_vehicle = run.getStoppedVehicle(event.lane, event.pos, event.vid);
					if (stopped_vehicle != null) stopped_vehicle.sec = event.sec; // HU
				}
			} else { // Other position is reported
				
				// Other position derivation
				if (!early_condensed_filtering) { 
					
					runLookUp(event); // RL 20
				
					if (existingVehicle.pos != event.pos && existingVehicle.lane != event.lane) { // FI 
						
						if (!event_derivation_omission) {
							OtherPos samePos = new OtherPos(event); // ED
						}
				}} else {
					if (!event_derivation_omission) {
						OtherPos samePos = new OtherPos(event); // ED
					}
				}
				      					
				if (existingVehicle.count >= 4 && existingVehicle.lane > 0 && existingVehicle.lane < 4) { // FI    	
					
					if (!early_condensed_filtering) runLookUp(event); // RL 25
					run.setRemovalTime(event.vid, event.sec); // HU	
					
					// Accident clearance detection
					if (!early_condensed_filtering) runLookUp(event); // RL 27
					run.fromAccident(event, startOfSimulation, false); // FI, HU
				}  
				if (!early_condensed_filtering) runLookUp(event); // RL 26
				existingVehicle.count = 1; // HU   			
				existingVehicle.lane = event.lane;
				existingVehicle.pos = event.pos;
		}}
		run.collectGarbage(event.min);
	}
	
	/**
	 * Compute the rolling average speed of all vehicles for the last 5 minutes
	 * @param min
	 * @return rolling average speed
	 */
	public double default_getAvgSpdFor5Min (Run run, PositionReport event, double min) {
		
		double sum = 0;
		double count = 0;
		// Look-up or compute average speeds for 5 minutes before
		for (int i=1; i<=5 && min-i>0; i++) {	
			double avgSpdPerMin = default_lookUpOrComputeAvgSpd(run, event, min-i);			
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
	public double default_lookUpOrComputeAvgSpd (Run run, PositionReport event, double min) {
		
		runLookUp(event); // RL
		
		double result = 0;
		
		if (!early_condensed_filtering) {
			if (run.time.min < event.min) { // FI 
				if (run.avgSpds.containsKey(min)) {
					result = run.avgSpds.get(min); // PR
				} else {
					result = default_getAvgSpd(run, event, min);
					run.avgSpds.put(min,result); // HU
		}}} else {
			if (run.avgSpds.containsKey(min)) {
				result = run.avgSpds.get(min); // PR
			} else {
				result = default_getAvgSpd(run, event, min);
				run.avgSpds.put(min,result); // HU
		}}
		return result;
	}
	
	/**
	 * Compute average speed of all vehicles for a given minute
	 * @param min
	 * @return average speed
	 */
	public double default_getAvgSpd (Run run, PositionReport event, double min) {
		
		runLookUp(event); // RL
		
		double sum = 0;
		double count = 0;		
		Set<Double> vids = run.vehicles.keySet();	
		
		if (!early_condensed_filtering) {
			if (run.time.min < event.min) { // FI
				for (Double vid : vids) {				
					Vehicle vehicle = run.vehicles.get(vid);
					double spd = vehicle.default_getAvgSpd(min);
					if (spd>-1) {
						sum += spd;	
						count++;
		}}}} else {
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
