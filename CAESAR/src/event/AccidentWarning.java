package event;

/**
 * In addition to type, time stamp and vehicle identifier, 
 * an accident warning has an emission time stamp and a segment.
 * 
 * @author Olga Poppe
 */
public class AccidentWarning extends Event {
	
	double emit;
	double seg;
	
	public AccidentWarning (double v, double t, double s, long startOfSimulation, double arrivalTime) {
		
		super(1,t,v);
		
		emit = (System.currentTimeMillis() - startOfSimulation)/1000;
		if (emit - arrivalTime > 5) System.err.println(this.toString() + " violates response time constraint!");
		
		seg = s;		
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
