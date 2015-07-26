package event;

import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

/**
 * In addition to type, time stamp and vehicle identifier, 
 * an accident warning has an emission time stamp and a segment. 
 * @author Olga Poppe
 */
public class AccidentWarning extends Event {
	
	double emit;
	double seg;
			
	public AccidentWarning (PositionReport p, double s, long startOfSimulation, AtomicBoolean awf, AtomicLong max_latency, long distrProgr) {
		
		super(1,p.sec,p.vid);		
		emit = (System.currentTimeMillis() - startOfSimulation)/1000;
		seg = s;
				
		printError (p, emit, awf, max_latency, "ACCIDENT WARNINGS", distrProgr);			
	}	

	/** 
	 * Print this accident warning.
	 */
	public String toString () {
		return new Double(type).intValue() + ","
				+ new Double(sec).intValue() + ","
				+ new Double(emit).intValue() + "," 
				+ new Double(vid).intValue() + ","
				+ new Double(seg).intValue() + "\n";			
	}
}
