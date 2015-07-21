package run;

import java.util.HashMap;
import java.util.Set;
import java.util.Vector;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import accident.*;
import event.*;

/**
 * A run captures the processing of a unidirectional road segment.
 * @author Olga Poppe
 */
public class Run {
	
	public RunID runID;
	public Time time;
	
	// Mapping of vehicle identifiers to vehicle data 
	public ConcurrentHashMap<Double,Vehicle> vehicles;	
	// Mapping of minutes to vehicle counts
	public HashMap<Double,Double> vehCounts;
	
	// Mapping of minutes to average speeds
	public HashMap<Double,Double> avgSpds;
	public double avgSpd;
	
	// Mapping of accident locations to stopped vehicles 
	public HashMap<AccidentLocation,Vector<StoppedVehicle>> stoppedVehicles;		
	boolean accident;
	public AccidentLocation currentAccidentLocation;
	LinkedBlockingQueue<Accident> accidents;
	
	// Mapping of minutes to segments ahead with accidents
	public HashMap<Double,Double> accidentsAhead;
	AtomicInteger firstHPseg;	
	
	public Output output;	
	
	boolean count_and_rate;
	public String context;
	
	public Run (RunID id, double s, double m, AtomicInteger f, boolean cr) {
		
		runID = id;		
		time = new Time(s,m);
		
		vehicles = new ConcurrentHashMap<Double,Vehicle>();
		vehCounts = new HashMap<Double,Double>();
		
		avgSpds = new HashMap<Double,Double>();
		avgSpd = -1;
		
		stoppedVehicles = new HashMap<AccidentLocation,Vector<StoppedVehicle>>();		
		accident = false;
		currentAccidentLocation = new AccidentLocation(-1,-1);
		accidents = new LinkedBlockingQueue<Accident>();
		
		accidentsAhead = new HashMap<Double,Double>();
		firstHPseg = f;
			
		output = new Output();	
		
		count_and_rate = cr;
		context = "clear";
	}
	
	/************************************************* Vehicle count *************************************************/
	/**
	 * Look up vehicle count for a given minute
	 * @param min
	 * @return vehicle count
	 */
	public double lookUpVehCount (double min) {
		return (vehCounts.containsKey(min)) ? vehCounts.get(min) : 0;
	}
	
	/************************************************* Average speed *************************************************/
	/**
	 * Compute average speed of all vehicles for a given minute
	 * @param min
	 * @return average speed
	 */
	public double getAvgSpd (double min) {
		
		double sum = 0;
		double count = 0;		
		Set<Double> vids = vehicles.keySet();	
		
		for (Double vid : vids) {				
			Vehicle vehicle = vehicles.get(vid);
			double spd = vehicle.getAvgSpd(min);
			if (spd>-1) {
				sum += spd;	
				count++;
		}}
		return (sum==0 && count==0) ? -1 : sum/count;		
	}	
	
	/**
	 * Look up or compute average speed of all vehicles for a given minute 
	 * @param min
	 * @return average speed
	 */
	public double lookUpOrComputeAvgSpd (double min) {
		double result;
		if (avgSpds.containsKey(min)) {
			result = avgSpds.get(min);
		} else {
			result = getAvgSpd(min);
			avgSpds.put(min,result);
		}
		return result;
	}
	
	/**
	 * Compute the rolling average speed of all vehicles for the last 5 minutes
	 * @param min
	 * @return rolling average speed
	 */
	public double getAvgSpdFor5Min (double min) {
		
		double sum = 0;
		double count = 0;
		// Look-up or compute average speeds for 5 minutes before
		for (int i=1; i<=5 && min-i>0; i++) {	
			double avgSpdPerMin = lookUpOrComputeAvgSpd(min-i);			
			if (avgSpdPerMin>-1) {
				sum += avgSpdPerMin;	
				count++;			
		}}
		return (min==1 || (sum==0 && count==0)) ? -1 : sum/count;	
	}
	
	/************************************************* Stopped vehicles *************************************************/
	/**
	 * Get the number of stopped vehicles in the current accident location during the given second 
	 * @param sec
	 * @return number of stopped vehicles in the current accident location
	 */
	public int getStopVehNumber (double sec) {
		
		if (stoppedVehicles.containsKey(currentAccidentLocation)) {
			
			Vector<StoppedVehicle> stopped_vehicles = stoppedVehicles.get(currentAccidentLocation);	
			
			if (stopped_vehicles.size() == 2) {
				
				StoppedVehicle first_stopped_car = stopped_vehicles.get(0); 
				StoppedVehicle second_stopped_car = stopped_vehicles.get(1);
				
				if (first_stopped_car.removalSec == -1 && first_stopped_car.sec + 30 < sec) first_stopped_car.removalSec = first_stopped_car.sec + 30;				
				if (second_stopped_car.removalSec == -1 && second_stopped_car.sec + 30 < sec) second_stopped_car.removalSec = second_stopped_car.sec + 30;				
				
				if (first_stopped_car.removalSec == -1 && second_stopped_car.removalSec == -1) return 2;
				if (first_stopped_car.removalSec == -1 || second_stopped_car.removalSec == -1) return 1;
			}
		}
		return 0;
	}
	
	/**
	 * Get maximal number of stopped vehicles during the given second
	 * @param sec
	 * @return number of stopped vehicles
	 */
	public double getMaxStopVehNumber (double sec) {
		
		double max = 0;		
		Set<AccidentLocation> locations = stoppedVehicles.keySet();
		for (AccidentLocation location : locations) {
			
			int size = 0;
			Vector<StoppedVehicle> stopped_vehicles = stoppedVehicles.get(location);
			for (StoppedVehicle v : stopped_vehicles) {
				
				if (v.removalSec == -1) {
					if (v.sec + 30 < sec) {
						v.removalSec = v.sec + 30;					
					} else {
						size++;
			}}}		
			max = max < size ? size : max; 			
		}	
		return max;
	}
	
	/** 
	 * Return a vehicle if it is stopped at a given location and its identifier is equal to the given identifier
	 * @param lane	lane
	 * @param pos	position
	 * @param vid	vehicle identifier
	 * @return vehicle
	 */
	public StoppedVehicle getStoppedVehicle (double lane, double pos, double vid) {
		
		AccidentLocation key = new AccidentLocation (lane, pos);
		if (stoppedVehicles.containsKey(key)) {
			Vector<StoppedVehicle> stopped_vehicles = stoppedVehicles.get(key);				
			for (StoppedVehicle v : stopped_vehicles) {
				if (v.vid == vid) {
					return v;
		}}}
		return null;
	}
	
	/**
	 * Set removal time of a vehicle with a given vehicle identifier at the current accident location
	 * @param vid			vehicle identifier
	 * @param removalTime	removal time stamp
	 */
	public void setRemovalTime (double vid, double removalTime) {	
		
		if (stoppedVehicles.containsKey(currentAccidentLocation)) {
			Vector<StoppedVehicle> stopped_vehicles = stoppedVehicles.get(currentAccidentLocation);			
			for (StoppedVehicle v : stopped_vehicles) {
				if (v.vid == vid) {
					v.removalSec = removalTime;		
		}}}		
	}	
	
	/**
	 * Return the minute during which the accident at the current accident location was cleared
	 * @return minute
	 */
	public double getAccClearMin () {
		
		double accClearMin;
		
		Vector<StoppedVehicle> stopped_vehicles = stoppedVehicles.get(currentAccidentLocation);			
						
		double first_removal_time = stopped_vehicles.get(0).removalSec;
		double second_removal_time = stopped_vehicles.get(1).removalSec;
				
		double first_min = Math.floor(first_removal_time/60); 
		double second_min = Math.floor(second_removal_time/60);
				
		if (first_min != second_min) {
			accClearMin = Math.max(first_min,second_min);
		} else {
			double first_rest = first_removal_time/60 - first_min;
			double second_rest = second_removal_time/60 - second_min;			
			accClearMin = (first_rest+second_rest < 0.2) ? first_min : first_min+1;					
		}
		stopped_vehicles.remove(currentAccidentLocation);
				
		return accClearMin;
	}
	
	/************************************************* State and priority *************************************************/
	/**
	 * Returns true if this road segment is congested during the given minute, returns false otherwise.
	 * @param min
	 * @return boolean
	 */
	public boolean congested (double min) {
		return lookUpVehCount(min) > 50 && 0 < avgSpd && avgSpd < 40;			
	}
	
	/**
	 * If there is an accident on this road segment, 
	 * update accidents, current accident location and first HP segment.
	 * @param event
	 * @param startOfSimulation
	 * @param run_priorization
	 */
	public void toAccident (PositionReport event, long startOfSimulation, boolean run_priorization) { 
		
		if (!accident && getMaxStopVehNumber(event.sec) == 2) {	
			
			// Update accidents
			accident = true;
			long currentSystemTime = System.currentTimeMillis() - startOfSimulation;
			writeAccidents(event, runID, event.min, currentSystemTime, -1, -1);		
			
			// Update current accident location
			currentAccidentLocation.reset(event.lane,event.pos);
			
			System.out.println(	event.sec + ": Run " + runID + " changes its state to ACCIDENT in lane " + 
								event.lane + " at position " + event.pos + " during minute " + event.min + ".");
			
			// Update first HP segment on this road in this direction
			if (run_priorization) {
				
				//long beginOfPriorityMaintenance = System.currentTimeMillis();
				
				Double firstSeg;
				double lastSeg;
				if (runID.dir == 0) {
					double x = runID.seg-4;
					firstSeg = Math.max(0, x); // if x<0
					lastSeg = 99;
				} else {
					double x = runID.seg+4;
					firstSeg = Math.min(x, 99); // if x>99
					lastSeg = 0;
				}
				firstHPseg.set(firstSeg.intValue());
							
				//long durationOfPriorityMaintenance = System.currentTimeMillis() - beginOfPriorityMaintenance;
				//time.priorityMaintenanceTime += durationOfPriorityMaintenance;
			
				System.out.println(	time.sec + ": New HPruns: xway " + runID.xway + " dir " + runID.dir + 
									" from seg " + firstSeg + " to seg " + lastSeg);
		}}
	}
	
	/**
	 * If there is no accident on this road segment, 
	 * update accidents, current accident location and first HP segment.
	 * @param event
	 * @param startOfSimulation
	 * @param run_priorization
	 */
	public void fromAccident (PositionReport event, long startOfSimulation, boolean run_priorization) {
		
		if (accident && getStopVehNumber(event.sec) == 0) {		
		
			// Update accidents
			accident = false;
			double clearAppMin = getAccClearMin();
			long currentSystemTime = System.currentTimeMillis() - startOfSimulation;
			writeAccidents(event, runID, -1, -1, clearAppMin, currentSystemTime);	
		
			// Update current accident location
			currentAccidentLocation.reset(-1,-1);	
		
			System.out.println(event.sec + ": Run " + runID + " changes its state from ACCIDENT during minute " + clearAppMin + ".");
		
			// Update first HP segment on this road in this direction
			if (run_priorization) {
			
				//long beginOfPriorityMaintenance = System.currentTimeMillis();
			
				firstHPseg.set(-1); // assuming there is only one accident per road in one direction
						
				//long durationOfPriorityMaintenance = System.currentTimeMillis() - beginOfPriorityMaintenance;
				//time.priorityMaintenanceTime += durationOfPriorityMaintenance;
			
				Double firstSeg;
				double lastSeg;
				if (runID.dir == 0) {
					double x = runID.seg-4;
					firstSeg = Math.max(0, x); // if x<0
					lastSeg = 99;
				} else {
					double x = runID.seg+4;
					firstSeg = Math.min(x, 99); // if x>99
					lastSeg = 0;
				}
				System.out.println(	time.sec + ": No HPruns anymore: xway " + runID.xway + " dir " + runID.dir + 
									" from seg " + firstSeg + " to seg " + lastSeg);
		}}
	}	
	
	/************************************************* Accident ahead *************************************************/
	/**
	 * Return true if the given event arrives during an accident in this road segment.
	 * @param event
	 * @return boolean
	 */
	boolean readAccidents (PositionReport event) {
		  for (Accident a : accidents) {	    		
		   		if (a.detAppMin<event.min && (a.clearAppMin==-1 || event.min<=a.clearAppMin+1)) {
		   			//System.out.println(runid + " reads accidents at time " + event.min + " : true");
		   			return true;
		  }}
		  //System.out.println(runid + " reads accidents at time " + event.min + " : false");
		  return false;		  
	}
	
	/**
	 * Create a new accident and store it or 
	 * set the time when the accident was cleared.
	 * @param event	incoming position report
	 * @param runid	run identifier
	 * @param dam	accident detection application minute 
	 * @param dpt	accident detection processing second
	 * @param cam	accident clearance application minute 
	 * @param cpt	accident clearance processing second
	 */
	public void writeAccidents (PositionReport event, RunID runid, double dam, double dpt, double cam, double cpt) {		
	    if (dam!=-1) {
	    	Accident a = new Accident(runid, event.lane, event.pos, dam, dpt);
	    	if (!accidents.contains(a)) accidents.add(a);	   	    		
	    } else {
	    	Accident a = getNotFinishedAccident();
	    	a.clearAppMin = cam;	   
	    	a.clearProcTime = cpt;	    		
	    }    
	}
	
	/**
	 * Return the current accident in this road segment.
	 * @return accident
	 */
	public Accident getNotFinishedAccident () {
		for (Accident a : accidents) {
			if (a.clearAppMin==-1) return a;
		}
		return null;
	}	
	
	/**
	 * Return segment with accident ahead or -1 if there is no accident ahead.
	 * @param runs	hash saving all runs
	 * @param event	incoming position report
	 * @return segment with accident ahead or -1
	 */
	public double getSegWithAccidentAhead (HashMap<RunID,Run> runs, PositionReport event) {
		
		/*** Get 4 runs ahead and read their accidents ***/
		for (int i=0; i<5; i++) {
			
			double segAhead = (runID.dir==0) ? (runID.seg+i) : (runID.seg-i);
			RunID runid = new RunID(runID.xway, runID.dir, segAhead);			
				
			if (segAhead>=0 && segAhead<=99 && runs.containsKey(runid)) {				 
					
				Run runAhead = runs.get(runid);		
				// if there is an accident ahead, segment with accident ahead is returned
				
				/*if (event == null) System.out.println("NULL EVENT!!!");
				if (runAhead == null) System.out.println("NULL RUN!!!");		*/		
				
				if (runAhead.readAccidents(event)) return segAhead;
		}}		
		return -1;
	}	
	
	/************************************************* Garbage collection *************************************************/
	/**
	 * Vehicle counts for the current minute are kept.
	 * All earlier vehicle counts are deleted.
	 * @param new_min
	 */
	public void deleteVehCounts (double new_min) {
		Set<Double> mins = vehCounts.keySet();
		/*** Get minutes to delete ***/
		Vector<Double> mins2delete = new Vector<Double>();
		for (Double m : mins) {
			if (m < new_min) mins2delete.add(m);				
		}
		/*** Delete these minutes ***/
		for (Double m : mins2delete) {
			vehCounts.remove(m);
		} 	 
	}
	
	/**
	 * Average speeds for the last 5 minutes are kept.
	 * All earlier average speeds are deleted.
	 * @param new_min	current processing minute of this run
	 */
	public void deleteAvgSpds (double new_min) {
		Set<Double> mins = avgSpds.keySet();
		/*** Get minutes to delete ***/
		Vector<Double> mins2delete = new Vector<Double>();
		for (Double m : mins) {
			if (m < new_min-5) mins2delete.add(m);				
		}
		/*** Delete these minutes ***/
		for (Double m : mins2delete) {
			avgSpds.remove(m);
		}
	}
	
	/**
	 *  Vehicles that sent a report during the last 5 minutes are kept.
	 *  All other vehicles are deleted.
	 * @param new_min	current processing minute of this run
	 */
	public void deleteVehicles (double new_min) {
		Set<Double> vids = vehicles.keySet();            			
		/*** Get vehicles to delete ***/
		Vector<Double> vehicles2delete = new Vector<Double>();
		for (Double vid : vids) {
			Vehicle vehicle = vehicles.get(vid);				
			if (vehicle.min < new_min-5) vehicles2delete.add(vid);				 
		}
		/*** Delete these vehicles ***/
		for (Double vid : vehicles2delete) {
			vehicles.remove(vid);				
		}			
	}
	
	/**
	 * Delete expired objects and values
	 * @param new_min	current processing minute of this run 
	 */
	public void collectGarbage (double new_min) {
		if (time.minOfLastGarbageCollection < new_min-2) {
	 			
	 		//long beginOfDeletion = System.currentTimeMillis();
	 			
 			deleteVehCounts(new_min);
 			deleteAvgSpds(new_min);
 			deleteVehicles(new_min);
 			time.minOfLastGarbageCollection = new_min;	
	 			
 			//long durationOfDeletion = System.currentTimeMillis() - beginOfDeletion;
 			//time.garbageCollectionTime += durationOfDeletion;		 			
		}
	}
	
	/************************************************* Traffic management *************************************************/
	/**
	 * trafficManagement maintains stopped vehicles, detects accidents and their clearance and derives accident warnings (like accidentManagement) and 
	 * 					 maintains average speeds and vehicle counts and derives toll notifications (like congestionManagement).   
	 * @param event					incoming position report
	 * @param startOfSimulation 	start of simulation
	 * @param segWithAccAhead		segment with accident ahead
	 * @param accidentWarningsFailed whether accident warnings failed the constraints already
	 * @param tollNotificationsFailed whether toll notifications failed the constraints already
	 * @param distrProgr			distributor progress in event.sec
	 */
	public void trafficManagement (PositionReport event, long startOfSimulation, double segWithAccAhead, 
			AtomicBoolean accidentWarningsFailed, AtomicBoolean tollNotificationsFailed, long distrProgr) {
		
		// Set auxiliary variables
		double next_min = event.min+1;
		boolean isAccident = segWithAccAhead != -1;   
		
		// Set event processing time
		/*if (time.minOfLastStorageOfEventProcessingTime < event.min) {
					
			event.processingTime = (System.currentTimeMillis() - startOfSimulation)/1000;
			output.positionReports.add(event);
			time.minOfLastStorageOfEventProcessingTime = event.min;  		   			 	
		}*/	

		// Update run data: avgSpd, time, numberOfProcessedEvents
		if (time.min < event.min) {  

			avgSpd = getAvgSpdFor5Min(event.min);   		
			time.min = event.min;
		}   
		time.sec = event.sec;
		if (count_and_rate) output.update_positionreport_rates(runID, event.min);
		
		/************************************************* If the vehicle is new in the segment *************************************************/
		if (vehicles.get(event.vid) == null) {
			
			// Update vehicles, vehCounts
			Vehicle newVehicle = new Vehicle (event);
			Vector<Double> new_speeds_per_min = new Vector<Double>();
			new_speeds_per_min.add(event.spd);
			newVehicle.spds.put(event.min,new_speeds_per_min);
			vehicles.put(event.vid,newVehicle);    	   

			double new_count = vehCounts.containsKey(next_min) ? vehCounts.get(next_min)+1 : 1;
			vehCounts.put(next_min, new_count);
			    
			// Derive complex events
			if (event.lane < 4) {    
				
				TollNotification tollNotification;
				if 	(!isAccident && congested(event.min)) { 
	
					double vehCount = lookUpVehCount(event.min);
					tollNotification = new TollNotification(event, avgSpd, vehCount, startOfSimulation, tollNotificationsFailed, distrProgr); 	
					if (count_and_rate) {
						output.real_toll_count++;
						output.update_real_tollnotification_rates(runID, event.min);
					}
				} else {
					tollNotification = new TollNotification(event, avgSpd, startOfSimulation, tollNotificationsFailed, distrProgr);	
					if (count_and_rate) { 
						output.zero_toll_count++;
						output.update_zero_tollnotification_rates(runID, event.min);
					}
				}
				output.tollNotifications.add(tollNotification);
				
				if (isAccident) {		
					
					AccidentWarning accidentWarning = new AccidentWarning(event, segWithAccAhead, startOfSimulation, accidentWarningsFailed, distrProgr);
					if (count_and_rate) output.update_accidentwarning_rates(runID, event.min);
					output.accidentWarnings.add(accidentWarning);				
			}}
			/************************************************* If the vehicle was in the segment before *************************************************/
		} else {
			// Get previous info about the vehicle
			Vehicle existingVehicle = vehicles.get(event.vid);
		
			// Update vehCounts
			// Update existingVehicle: time
			existingVehicle.sec = event.sec;  		
			
			if (event.min > existingVehicle.min) {
				
				existingVehicle.min = event.min;
				
				double new_count = vehCounts.containsKey(next_min) ? vehCounts.get(next_min)+1 : 1;
				vehCounts.put(next_min, new_count);			
			}
			// Update existingVehicle: spd, spds
			existingVehicle.spd = event.spd;
			if (existingVehicle.spds.containsKey(event.min)) {    

				existingVehicle.spds.get(event.min).add(event.spd);    			
 				
			} else {             					
	
				Vector<Double> new_speeds_per_min = new Vector<Double>();
				new_speeds_per_min.add(event.spd);
				existingVehicle.spds.put(event.min, new_speeds_per_min);			
			}			
			
			// Accident detection and clearance	
			// Update stoppedVehicles
			// Update existingVehicle: count, lane, pos
			if (existingVehicle.pos == event.pos && existingVehicle.lane == event.lane) { // Same position is reported      

				existingVehicle.count++;

				AccidentLocation accidentLocation = new AccidentLocation (event.lane, event.pos);
				if (existingVehicle.count == 4 && existingVehicle.lane > 0 && existingVehicle.lane < 4)  {

					StoppedVehicle stopped_vehicle = new StoppedVehicle(event.vid, event.sec);

					if (stoppedVehicles.containsKey(accidentLocation)) {
	
						Vector<StoppedVehicle> stopped_vehicles = stoppedVehicles.get(accidentLocation);
						if (stopped_vehicles.size() < 2) { 
							stopped_vehicles.add(stopped_vehicle);
							toAccident(event, startOfSimulation, false);
						}    					
					} else {
						Vector<StoppedVehicle> stopped_vehicles = new Vector<StoppedVehicle>();
						stopped_vehicles.add(stopped_vehicle);
						stoppedVehicles.put(accidentLocation,stopped_vehicles);
					}}
				if (existingVehicle.count > 4 && existingVehicle.lane > 0 && existingVehicle.lane < 4)  {

					StoppedVehicle stopped_vehicle = getStoppedVehicle(event.lane, event.pos, event.vid);
					if (stopped_vehicle != null) stopped_vehicle.sec = event.sec;
				}
			} else { // Other position is reported
	      					
				if (existingVehicle.count >= 4 && existingVehicle.lane > 0 && existingVehicle.lane < 4) {     				
					setRemovalTime(event.vid, event.sec); 	
					fromAccident(event, startOfSimulation, false);
				}  
				existingVehicle.count = 1;    			
				existingVehicle.lane = event.lane;
				existingVehicle.pos = event.pos;
		}}		
	}
	/**
	 * accidentManagement maintains stopped vehicles, detects accidents and their clearance and derives accident warnings.   
	 * @param event					incoming position report
	 * @param startOfSimulation 	start of simulation
	 * @param segWithAccAhead		segment with accident ahead
	 * @param run_priorization		whether run priorities are maintained
	 * @param accidentWarningsFailed whether accident warnings failed the constraints already
	 * @param distrProgr			distributor progress in event.sec
	 */
	public void accidentManagement (PositionReport event, long startOfSimulation, double segWithAccAhead, boolean run_priorization, AtomicBoolean accidentWarningsFailed, long distrProgr) {
		
		//System.out.println(event.timesToString());
		
		// Set auxiliary variables
		boolean isAccident = segWithAccAhead != -1;   		
		
		/************************************************* If the vehicle is new in the segment *************************************************/
		if (vehicles.get(event.vid) == null) {
			
			// Update vehicles
			Vehicle newVehicle = new Vehicle (event);
			vehicles.put(event.vid,newVehicle);    
			
			// Derive accident warnings
			if (event.lane < 4 && isAccident) {		
					
				AccidentWarning accidentWarning = new AccidentWarning (event, segWithAccAhead, startOfSimulation, accidentWarningsFailed, distrProgr);
				output.accidentWarnings.add(accidentWarning);				
			}
		/********************************************** If the vehicle was in the segment before ***********************************************/
		} else {
			// Get previous info about the vehicle
			Vehicle existingVehicle = vehicles.get(event.vid);
		
			// Same position is reported: ACCIDENT DETECTION
			// Update stoppedVehicles
			// Update existingVehicle: count, lane, pos			
			if (existingVehicle.pos == event.pos && existingVehicle.lane == event.lane) {       

				existingVehicle.count++;
				AccidentLocation accidentLocation = new AccidentLocation (event.lane, event.pos);
				
				// Add new stopped vehicle
				if (existingVehicle.count == 4 && existingVehicle.lane > 0 && existingVehicle.lane < 4)  {

					StoppedVehicle stopped_vehicle = new StoppedVehicle(event.vid, event.sec);
					if (stoppedVehicles.containsKey(accidentLocation)) {
	
						Vector<StoppedVehicle> stopped_vehicles = stoppedVehicles.get(accidentLocation);						
						if (stopped_vehicles.size() < 2) { 
							
							stopped_vehicles.add(stopped_vehicle); 
							toAccident(event, startOfSimulation, run_priorization);
						}    					
					} else {
						Vector<StoppedVehicle> stopped_vehicles = new Vector<StoppedVehicle>();
						stopped_vehicles.add(stopped_vehicle);
						stoppedVehicles.put(accidentLocation,stopped_vehicles);
				}}
				// Update the time stamp of the stopped vehicle
				if (existingVehicle.count > 4 && existingVehicle.lane > 0 && existingVehicle.lane < 4)  {

					StoppedVehicle stopped_vehicle = getStoppedVehicle(event.lane, event.pos, event.vid);
					if (stopped_vehicle != null) stopped_vehicle.sec = event.sec;
				}
			} else { 
			// Other position is reported: ACCIDENT CLEARANCE
	      					
				if (existingVehicle.count >= 4 && existingVehicle.lane > 0 && existingVehicle.lane < 4) {   
					
					setRemovalTime(event.vid, event.sec);
					fromAccident(event, startOfSimulation, run_priorization);
				}  
				existingVehicle.count = 1;    			
				existingVehicle.lane = event.lane;
				existingVehicle.pos = event.pos;
		}} 
		//event.executorTime = (System.currentTimeMillis() - startOfSimulation)/1000;
	}	
	
	/**
	 * congestionManagement maintains average speeds and vehicle counts and derives toll notifications. 
	 * @param event					incoming position report
	 * @param startOfSimulation 	start of simulation
	 * @param segWithAccAhead		segment with accident ahead 
	 * @param tollNotificationsFailed whether toll notifications failed the constraints already
	 * @param distrProgr			distributor progress in event.sec
	 */
	public void congestionManagement (PositionReport event, long startOfSimulation, double segWithAccAhead, AtomicBoolean tollNotificationsFailed, long distrProgr) { 
		
		// Set auxiliary variables
		double next_min = event.min+1;
		boolean isAccident = segWithAccAhead != -1;   

		// Set event processing time
		/*if (time.minOfLastStorageOfEventProcessingTime < event.min) {
			
			event.processingTime = (System.currentTimeMillis() - startOfSimulation)/1000;
			output.positionReports.add(event);
			time.minOfLastStorageOfEventProcessingTime = event.min;  		   			 	
		}*/	

		// Update run data: avgSpd, time
		if (time.min < event.min) {  

			avgSpd = getAvgSpdFor5Min(event.min);   		
			time.min = event.min;
		}   	
		time.sec = event.sec;
				
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
	
	/************************************************* Output *************************************************/
	/**
	 * Write the number of processed and stored events to the given file. 
	 * @param file
	 */
	/*public void write2FileEventStorage (BufferedWriter file) {		
		String line = runID.toString() + " " + output.numberOfProcessedEvents + " " + output.maxNumberOfStoredEvents + "\n";	
		try { file.write(line); } catch (IOException e) { e.printStackTrace(); }
	}*/
	
	/**
	 * Write accident processing times to the given file. 
	 * @param file
	 */
	/*public void write2FileAccidentProcessingTimes (BufferedWriter file) {
		for (Accident a : accidents) {
			int dpt = new Double(a.detProcTime).intValue();
			int cpt = new Double(a.clearProcTime).intValue();
			String line = runID.toString() + " " + dpt + " " + cpt + "\n";		
			try { file.write(line); } catch (IOException e) { e.printStackTrace(); }
		}
	}*/
	
	/** 
	 * Print this run.
	 */
	public String toString() {
		String s =	"\nRun " + runID.toString() + " with " +										
					"sec: " + time.sec + 
					"\nvehCounts: ";
		Set<Double> mins = vehCounts.keySet(); 
		for (Double min : mins) { 
			s += min + ": " + vehCounts.get(min) + ", ";
		}				
		s +=		"\navgSpd: " + avgSpd  + ", ";									
		return s;
	}	
}