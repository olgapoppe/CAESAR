package event;

import java.util.concurrent.atomic.AtomicBoolean;

/**
 * In addition to type, time stamp and vehicle identifier, 
 * an accident warning has an emission time stamp and a segment. 
 * @author Olga Poppe
 */
public class AccidentWarning extends Event {
	
	public double emit;
	double seg;
			
	public AccidentWarning (PositionReport p, double s, long startOfSimulation, AtomicBoolean awf, long distrProgr) {
		
		super(1,p.sec,p.vid);		
		emit = (System.currentTimeMillis() - startOfSimulation)/new Double(1000);
		seg = s;
				
		printError (p, emit, awf, "ACCIDENT WARNINGS", distrProgr);			
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
