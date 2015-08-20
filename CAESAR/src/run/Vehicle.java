package run;

import java.util.HashMap;
import java.util.Set;
import java.util.Vector;
import event.PositionReport;

/**
 * A vehicle is described by a vehicle identifier, time stamp, minute, speed, expressway, lane, direction, segment, position,
 * number of times same position was reported in a raw and hash mapping minutes to speeds. 
 * @author Olga Poppe
 */
public class Vehicle {
	
	public double vid;	
	
	public double appearance_sec;
	public double sec;
	public double min;
	
	public double spd;
	public double xway;
	public double lane;
	public double dir;
	public double seg;
	public double pos;
	
	// Number of times this position was reported
	public double count;	
	// Mapping of minutes to speeds
	public HashMap<Double,Vector<Double>> spds;
	// Mapping of minutes to average speeds
	public HashMap<Double,Double> avgSpds;
					
	public Vehicle (PositionReport e) {
		
		vid = e.vid;
		
		appearance_sec = e.sec;
		sec = e.sec;
		min = e.min;
		
		spd = e.spd;
		xway = e.xway;
		lane = e.lane;
		dir = e.dir;
		seg = e.seg;
		pos = e.pos;
		
		count = 1;		
		spds = new HashMap<Double,Vector<Double>>();		
		avgSpds = new HashMap<Double,Double>();
	}
	
	/**
	 * Compute the average speed of this vehicle during the given minute 
	 * @param min	minute
	 * @return 		average speed 
	 */	
	public double getAvgSpd (double min) {
		if (spds.containsKey(min)) {			
			double sum = 0;
			Vector<Double> spdsPerMin = spds.get(min);	
			for (double spd : spdsPerMin) {
				sum += spd;
			}
			return sum/new Double(spdsPerMin.size());			
		} else {
			return -1;
		}	
	}
	
	/**
	 * Compute the average speed of this vehicle during the given minute 
	 * @param min	minute
	 * @return 		average speed 
	 */	
	public double default_getAvgSpd (double min) {
		if (spds.containsKey(min)) {			
			double sum = 0;
			Vector<Double> spdsPerMin = spds.get(min);	
			for (double spd : spdsPerMin) {
				sum += spd;
			}
			double result = sum/new Double(spdsPerMin.size());
			avgSpds.put(min, result); // HU
			return result;			
		} else {
			return -1;
		}	
	}	
	
	/** 
	 * Print this position report.
	 */
	public String toString() {
		
		String s = 	"\nvid=" + vid + ", " +
					"sec=" + sec + ", " +
					"appearance sec=" + appearance_sec + ", " +
					"spd=" + spd + ", " +
					"xway=" + xway + ", " +
					"lane=" + lane + ", " +
					"dir=" + dir + ", " +
					"seg=" + seg + ", " +
					"pos=" + pos + ", " +
					"count=" + count + "\n" +
					"Speeds per minute:\n";
		
		Set<Double> mins = spds.keySet(); 
		for (Double min : mins) { 
			s += min + ": ";
			Vector<Double> speeds = spds.get(min); 
			for (Double speed : speeds) {
				s += speed + ", "; 
			}
			s += "\n";
		}		
		return s;
	}
}