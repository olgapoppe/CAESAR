package run;

/** 
 * A run processes a road segment which is uniquely identified by an expressway, direction and segment.
 * @author Olga Poppe
 */
public class RunID {
	
	public double xway;
	public double dir;
	public double seg;
	
	public RunID (double x, double d, double s) {		
		xway = x;
		dir = d;
		seg = s;
	}
	
	public RunID (double i) {		
		xway = i;
		dir = i;
		seg = i;
	}
	
	/**
	 * Compute the hash code of this run identifier
	 * 
	 * @return hash code 
	 */
	public int hashCode(){
		String s = "" 
	        		+ new Double(xway).intValue() 
	        		+ new Double(dir).intValue() 
	        		+ new Double(seg).intValue();
		return Integer.parseInt(s);
	}
	
	/**
	 * Determines whether this run identifier equals to the given run identifier 
	 * 
	 * @param o 
	 */	 
	public boolean equals (Object o) {
		if (!(o instanceof RunID)) {
			return false;
		} else {
			RunID r = (RunID) o;
			return xway == r.xway && dir == r.dir && seg == r.seg;
		}
	}
	
	/** 
	 * Print this position report.
	 */
	public String toString() {
		
		int x = new Double(xway).intValue();
		int d = new Double(dir).intValue();
		int s = new Double(seg).intValue();
		
		return x + " " + d + " " + s;
	}
}
