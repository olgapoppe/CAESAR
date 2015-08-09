package event;

import java.util.concurrent.atomic.AtomicBoolean;

/**
 * In addition to type, time stamp and vehicle identifier, 
 * an accident warning has an emission time stamp and a segment. 
 * @author Olga Poppe
 */
public class AccidentWarning extends Event {
	
	public double distributorTime;
	public double emit;
	double seg;
			
	public AccidentWarning (PositionReport p, double s, double scheduling_time, long startOfSimulation, AtomicBoolean awf) {
		
		super(1,p.sec,p.vid);	
		
		distributorTime = p.distributorTime;
		emit = (System.currentTimeMillis() - startOfSimulation)/new Double(1000);
		seg = s;
				
		printError (p, emit, awf, "ACCIDENT WARNINGS", scheduling_time);			
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
