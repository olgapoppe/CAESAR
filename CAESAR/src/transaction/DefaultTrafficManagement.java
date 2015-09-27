package transaction;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Set;
import java.util.Vector;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.LinkedBlockingQueue;
import accident.*;
import run.*;
import event.*;

/** 
 * A traffic managing transaction processes all events in the event sequence by their respective run
 * using a non-optimized or partially optimized query plan containing all queries.  
 * Optimization techniques can be turned on and off.
 * @author Olga Poppe
 */
public class DefaultTrafficManagement extends Transaction {	
	
	HashMap<Double,Double> distrFinishTimes;
	HashMap<Double,Double> schedStartTimes;
	
	AtomicBoolean accidentWarningsFailed;
	AtomicBoolean tollNotificationsFailed;
		
	public DefaultTrafficManagement (ArrayList<PositionReport> eventList, 
			HashMap<RunID,Run> rs, long start,
			HashMap<Double,Double> distrFinishT, HashMap<Double,Double> schedStartT,
			AtomicInteger tet, AtomicBoolean awf, AtomicBoolean tnf) {
		
		super(eventList,rs,start,tet);
		
		distrFinishTimes = distrFinishT;
		schedStartTimes = schedStartT;
		
		accidentWarningsFailed = awf;
		tollNotificationsFailed = tnf;		
	}
		
	/**
	 * Execute these events by their respective run.
	 */	
	public void run() {	
		
		double segWithAccAhead;
		double max_exe_time_in_this_transaction = 0;
			
		for (PositionReport event : events) {
			
			if (event == null) System.out.println("NULL EVENT!!!");
			
			RunID runid = new RunID(event.xway, event.dir, event.seg);
			Run run = runs.get(runid); // RL
			if (run == null) System.out.println("NULL RUN!!!" + event.toString());
			
			// READ: If a new vehicle on a travel lane arrives, lookup accidents ahead
			if (run.vehicles.get(event.id) == null && event.lane < 4) { // FI
								
				if (event.min > run.time.minOfLastUpdateOfAccidentAhead) { // FI
									
					segWithAccAhead = run.getSegWithAccidentAhead(runs, event); // RL 13, FI
					if (segWithAccAhead!=-1) run.accidentsAhead.put(event.min, segWithAccAhead); // HU
					run.time.minOfLastUpdateOfAccidentAhead = event.min;
				} else {
					segWithAccAhead = (run.accidentsAhead.containsKey(event.min)) ? run.accidentsAhead.get(event.min) : -1; // PR
				}			
			} else {
				segWithAccAhead = -1;
			}	
			HashMap<Double,Double> accAhead = run.accidentsAhead; // optional PR
			double min1 = event.min;
			
			// WRITE: Update the respective run and remove old data
			double app_time_start = (System.currentTimeMillis() - startOfSimulation)/new Double(1000);			
			
			defaultTrafficManagement (event, segWithAccAhead, startOfSimulation, accidentWarningsFailed, tollNotificationsFailed);			
			
			double app_time_end = (System.currentTimeMillis() - startOfSimulation)/new Double(1000);			
			double exe_time = app_time_end - app_time_start;
			if (max_exe_time_in_this_transaction < exe_time) max_exe_time_in_this_transaction = exe_time;
		}	
		// Increase maximal execution time
		//if (total_exe_time.get() < max_exe_time_in_this_transaction) total_exe_time.set(max_exe_time_in_this_transaction);
				
		// Count down the number of transactions
		transaction_number.countDown();
	}	
	
	/*** 
	 * This method is same as run.trafficManagement but without operator omission and reordering (FI, PR, RL, ED).
	 * It is incremental and has optimal stream history access. 
	 ***/
	public void defaultTrafficManagement (PositionReport event, double segWithAccAhead, long startOfSimulation, 
			AtomicBoolean accidentWarningsFailed, AtomicBoolean tollNotificationsFailed) {
		
		// Set auxiliary variables
		double next_min = event.min+1;
				   
		// Update of avgSpd
		Run run = runLookUp(event); // RL 0			
		if (run.time.min < event.min) { // FI 
					
			double avgSpd = run.avgSpd; // optional PR
			run.avgSpd = default_getAvgSpdFor5Min(run, event, event.min); // HU	
		}	
		// Update of minute
		runLookUp(event); // RL 1	
		if (run.time.min < event.min) { // replicated FI 
					
			double emin = event.min; // optional PR
			double rmin = run.time.min; 
			run.time.min = event.min; // TU
		}		 
		// Update of second
		runLookUp(event); // RL 2
		double esec = event.sec; // optional PR
		double rsec = run.time.sec; 
		run.time.sec = event.sec; // TU
								
		/************************************************* If the vehicle is new in the segment *************************************************/
		// New car detection
		runLookUp(event); // RL 3
						
		if (run.vehicles.get(event.id) == null) { // FI
					
			projection(event); // optional PR
			event_derivation("NewCar",event); // optional ED
					
			// Update of vehicles
			runLookUp(event); // RL 4
			ConcurrentHashMap<Double,Vehicle> vehicles = run.vehicles; // optional PR
			projection(event);
					
			Vehicle newVehicle = new Vehicle (event);
			Vector<Double> new_speeds_per_min = new Vector<Double>();
			new_speeds_per_min.add(event.spd); 			
					
			newVehicle.spds.put(event.min,new_speeds_per_min);
			run.vehicles.put(event.id,newVehicle); // HU  	   

			// Update of vehCounts
			runLookUp(event); // RL 5
			HashMap<Double,Double> counts = run.vehCounts; // optional PR
			double min = run.time.min;
					
			double new_count = run.vehCounts.containsKey(next_min) ? run.vehCounts.get(next_min)+1 : 1;
			run.vehCounts.put(next_min, new_count); // HU
						
			// New traveling car detection
			runLookUp(event); // RL 28
					
			if (event.lane < 4) { // FI  
					
				projection(event); // optional PR	
				event_derivation("NewTravelingCar",event); // optional ED
								
				// Context update
				runLookUp(event); // RL 14
				boolean accident = segWithAccAhead != -1; // FI
				String context = run.context; // optional PR
				run.context = "accident";
						
				runLookUp(event); // RL 15
				boolean congestion = !accident && run.congested(event.min);
				String context1 = run.context; // optional PR
				run.context = "congestion";
						
				runLookUp(event); // RL 16
				boolean clear = !accident && !run.congested(event.min);
				String context2 = run.context; // optional PR
				run.context = "clear";
						
				// Toll notification derivation
				TollNotification tollNotification;			
								
				if 	(congestion) { // CW
						
					runLookUp(event); // RL 12		
					double vehCount = run.lookUpVehCount(event.min);
					tollNotification = new TollNotification(event, run.avgSpd, vehCount, distrFinishTimes, schedStartTimes, startOfSimulation, tollNotificationsFailed); // ED	
					run.output.tollNotifications.add(tollNotification);
				} 
				if (clear) { // CW
							
					runLookUp(event); // RL 6					
					tollNotification = new TollNotification(event, run.avgSpd, distrFinishTimes, schedStartTimes, startOfSimulation, tollNotificationsFailed);	// ED
					run.output.tollNotifications.add(tollNotification);
				}
				if (accident) { // CW
							
					runLookUp(event); // RL 7				
					tollNotification = new TollNotification(event, run.avgSpd, distrFinishTimes, schedStartTimes, startOfSimulation, tollNotificationsFailed);	// ED	
					run.output.tollNotifications.add(tollNotification);
				}		
						
				// Accident warning derivation
				if (accident) { // CW
							
					runLookUp(event); // RL 8					
					AccidentWarning accidentWarning = new AccidentWarning(event, segWithAccAhead, distrFinishTimes, schedStartTimes, startOfSimulation, accidentWarningsFailed); // ED
					run.output.accidentWarnings.add(accidentWarning);				
				}			
			}
			/*********************************************** If the vehicle was in the segment before ***********************************************/
		} else {	
			// Old car detection
			runLookUp(event); // RL 9
			if (run.vehicles.get(event.id) != null) { // FI				
				projection(event); // optional PR
				event_derivation("OldCar",event); // optional ED
			}  
				
			// Update of existingVehicle: sec
			runLookUp(event); // RL 18
			Vehicle existingVehicle = run.vehicles.get(event.id);
			double sec2 = event.sec; // optional PR
			existingVehicle.sec = event.sec; // TU
					
			// Update of vehCounts
			runLookUp(event); // RL 11
			if (event.min > existingVehicle.min) { // FI 	
				HashMap<Double,Double> vehCounts = run.vehCounts; // optional PR
				double min3 = event.min;
				double new_count = run.vehCounts.containsKey(next_min) ? run.vehCounts.get(next_min)+1 : 1;
				run.vehCounts.put(next_min, new_count);	// HU
			}
			// Update of existingVehicle: sec
			runLookUp(event); // RL 17
			Vehicle vehicle = run.vehicles.get(event.id); // optional PR
			double min2 = event.min; 
			if (event.min > existingVehicle.min) { // FI					
				existingVehicle.min = event.min; // TU
			}
							
			// Update of existingVehicle: spd, spds
			runLookUp(event); // RL 10
			Vehicle vehicle1 = run.vehicles.get(event.id); // optional PR
			double min3 = event.min;
			double spd = event.spd;
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
				runLookUp(event); // RL 19
				projection(event); // optional PR
				event_derivation("SamePos",event); // optional ED
						
				// Update count of the existing vehicle
				runLookUp(event); // RL 21
				Vehicle vehicle2 = run.vehicles.get(event.id); // optional PR
				double new_count = existingVehicle.count;
				existingVehicle.count++; // HU
						
				// Update stopped vehicles
				runLookUp(event); // RL 22
				HashMap<AccidentLocation, Vector<StoppedVehicle>> stoppedVeh = run.stoppedVehicles; // optional PR
				Vehicle vehicle3 = run.vehicles.get(event.id); 

				// Add new stopped vehicle
				AccidentLocation accidentLocation = new AccidentLocation (event.lane, event.pos);
				if (existingVehicle.count == 4 && existingVehicle.lane > 0 && existingVehicle.lane < 4)  { // FI

					StoppedVehicle stopped_vehicle = new StoppedVehicle(event.id, event.sec);

					if (run.stoppedVehicles.containsKey(accidentLocation)) {
						
						Vector<StoppedVehicle> stopped_vehicles = run.stoppedVehicles.get(accidentLocation);
						if (stopped_vehicles.size() < 2) { 
							stopped_vehicles.add(stopped_vehicle); // HU
									
							// Accident detection
							runLookUp(event); // RL 23
							LinkedBlockingQueue<Accident> accidents = run.accidents; // optional PR
							AccidentLocation al = run.currentAccidentLocation;
							double pos = event.pos;
							double lane = event.lane;
							double min = event.min;
							run.toAccident(event, startOfSimulation, false); // FI, HU
						}    					
					} else {
						Vector<StoppedVehicle> stopped_vehicles = new Vector<StoppedVehicle>();
						stopped_vehicles.add(stopped_vehicle);
						run.stoppedVehicles.put(accidentLocation,stopped_vehicles); // HU
				}}
				// Update second of previously detected stopped vehicle
				runLookUp(event); // RL 24
				if (existingVehicle.count > 4 && existingVehicle.lane > 0 && existingVehicle.lane < 4)  { // FI
								
					Vehicle vehicle4 = run.vehicles.get(event.id); // optional PR
					double sec = event.sec;
					StoppedVehicle stopped_vehicle = run.getStoppedVehicle(event.lane, event.pos, event.id);
					if (stopped_vehicle != null) stopped_vehicle.sec = event.sec; // HU
				}
			} else { // Other position is reported
						
				// Other position derivation
				runLookUp(event); // RL 20					
				if (existingVehicle.pos != event.pos && existingVehicle.lane != event.lane) { // FI
					projection(event); // optional PR
					event_derivation("OtherPos",event); // optional ED
				} 
						
				if (existingVehicle.count >= 4 && existingVehicle.lane > 0 && existingVehicle.lane < 4) { // FI    	
						
					// Set vehicle removal time
					runLookUp(event); // RL 25
					Vehicle vehicle2 = run.vehicles.get(event.id); // optional PR
					double sec = event.sec;
					run.setRemovalTime(event.id, event.sec); // HU	
							
					// Accident clearance detection
					runLookUp(event); // RL 27
					LinkedBlockingQueue<Accident> accidents = run.accidents; // optional PR
					AccidentLocation cal = run.currentAccidentLocation;
					double min = event.min;
					run.fromAccident(event, startOfSimulation, false); // FI, HU
				}  
				runLookUp(event); // RL 26
				Vehicle vehicle2 = run.vehicles.get(event.id); // optional PR
				double lane = event.lane;
				double pos = event.pos;
				existingVehicle.count = 1; // HU   			
				existingVehicle.lane = event.lane;
				existingVehicle.pos = event.pos;
		}}
		run.collectGarbage(event.min,false);		
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
		
		runLookUp(event); // RL 0
		
		double result = 0;
				
		if (run.time.min < event.min) { // FI 
			if (run.avgSpds.containsKey(min)) {
				result = run.avgSpds.get(min); // PR
			} else {
				result = default_getAvgSpd(run, event, min);
				run.avgSpds.put(min,result); // HU
			}
		}
		return result;
	}
	
	/**
	 * Compute average speed of all vehicles for a given minute
	 * @param min
	 * @return average speed
	 */
	public double default_getAvgSpd (Run run, PositionReport event, double min) {
		
		runLookUp(event); // RL 0
		
		double sum = 0;
		double count = 0;		
		Set<Double> vids = run.vehicles.keySet();	
		
		if (run.time.min < event.min) { // FI				
			for (Double vid : vids) {				
				Vehicle vehicle = run.vehicles.get(vid);
				double spd = vehicle.default_getAvgSpd(min);
				if (spd>-1) {
					sum += spd;	
					count++;
		}}}
		return (sum==0 && count==0) ? -1 : sum/count;		
	}
	
	/***
	 * Projection of the attributes of the given event.
	 * @param event
	 */
	public void projection (PositionReport event) {
		
		double type = event.type; 
		double sec = event.sec;
		double min = event.min;
		double vid = event.id;
		double spd = event.spd;
		double xway = event.xway;
		double lane = event.lane;
		double dir = event.dir;
		double seg = event.seg;
		double pos = event.pos;	
	}
	
	/*** 
	 * Derive events of the given event type.
	 * @param derived_event_type
	 * @param event
	 */
	public void event_derivation (String derived_event_type, PositionReport event) {
					
		switch (derived_event_type) {
			
			case "NewCar" : NewCar newCar = new NewCar(event);
							break;
			case "NewTravelingCar" : NewTravelingCar newTravelingCar = new NewTravelingCar(event);
							break;
			case "OldCar" : OldCar oldCar = new OldCar(event);
							break;
			case "SamePos" : SamePos samePos = new SamePos(event);
							break;
			case "OtherPos" : OtherPos otherPos = new OtherPos(event);
							break;
			default : System.err.println("No valid derived event type!");
							break;
		}		
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
