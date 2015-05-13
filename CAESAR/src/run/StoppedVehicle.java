package run;

/**
 * A stopped vehicle is described by a vehicle identifier, 
 * the time point at which it was stopped and 
 * the time point at which it was removed.
 * 
 * @author olga
 */
public class StoppedVehicle {
	
	public double vid;
	public double sec;
	public double removalSec;
	
	public StoppedVehicle (double v, double t) {
		vid = v;
		sec = t;
		removalSec = -1;
	}
}
