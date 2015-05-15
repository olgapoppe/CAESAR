package accident;

/** 
 * Accident location is uniquely identified by a lane and a position in a road segment. 
 * @author Olga Poppe
 */
public class AccidentLocation {
	
	public double accLane;
	public double accPos;
	
	public AccidentLocation (double l, double p) {
		accLane = l;
		accPos = p;
	}
	
	/**
	 * Reset current accident location to the given lane and position.
	 * @param l	lane
	 * @param p	position
	 */
	public void reset (double l, double p) {
		accLane = l;
		accPos = p;
	}
	
	/**
	 * Compute the hash code of this accident location.
	 */
	public int hashCode(){
		if (accLane == -1 && accPos == -1) {
			return -1;
		} else {
			String s = "" 
	        			+ new Double(accLane).intValue() 
	        			+ new Double(accPos).intValue();	        		
			return Integer.parseInt(s);
		}
	}
	
	/**
	 * Determine whether two accident locations coincide. 
	 */
	public boolean equals (Object o) {
		if (!(o instanceof AccidentLocation)) {
			return false;
		} else {
			AccidentLocation l = (AccidentLocation) o;
			return accLane == l.accLane && accPos == l.accPos;
		}
	}
	
	/** 
	 * Print this accident location.
	 */
	public String toString() {
		return accLane + ";" + accPos;
	}
}
