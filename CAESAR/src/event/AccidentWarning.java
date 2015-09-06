package event;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * In addition to type, time stamp and vehicle identifier, 
 * an accident warning has an emission time stamp and a segment. 
 * @author Olga Poppe
 */
public class AccidentWarning extends Event {
	
	public double emit;
	double seg;
	
	/**
	 * Construct accident warning when there is an accident on a road.
	 * @param p					position report
	 * @param s					segment with accident ahead
	 * @param delay				scheduler wake-up time after waiting for distributor
	 * @param startOfSimulation	start of simulation to generate the emission time
	 * @param awf				indicates whether accident warning failed already
	 */
	public AccidentWarning (PositionReport p, double s, 
			HashMap<Double,Double> distrFinishTimes, HashMap<Double,Double> schedStartTimes, long startOfSimulation, AtomicBoolean awf) {
		
		super(1,p.sec,p.vid);	
		
		emit = (System.currentTimeMillis() - startOfSimulation)/new Double(1000);
		seg = s;
		
		printError (p, emit, distrFinishTimes, schedStartTimes, awf, "ACCIDENT WARNINGS");			
	}	
	
	/**
	 * Return true if the given list of accident warnings contains this accident warning
	 * @param list
	 * @return
	 */
	public boolean isContained (ArrayList<AccidentWarning> list) {
		for (AccidentWarning other : list) {
			if (this.equals(other)) return true;
		}
		return false;
	}
	
	/**
	 * Return true if this accident warning is equals to other accident warning
	 * @param other
	 * @return
	 */
	public boolean equals (AccidentWarning other) {
		return 	type == other.type &&
				sec == other.sec &&
				vid == other.vid &&
				emit == other.emit &&
				seg == other.seg;
	}

	/** 
	 * Print this accident warning.
	 */
	public String toString () {
		return new Double(type).intValue() + ","
				+ new Double(sec).intValue() + ","
				+ emit + "," 
				+ new Double(vid).intValue() + ","
				+ new Double(seg).intValue() + "\n";			
	}
}
