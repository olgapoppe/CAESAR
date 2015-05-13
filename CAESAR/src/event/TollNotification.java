package event;

/**
 * In addition to type, time stamp and vehicle identifier, 
 * an toll notification has an emission time stamp, average speed and a toll value.
 * 
 * @author olga
 */
public class TollNotification extends Event {
	
	double emit;
	double avgSpd;
	double toll;
	
	/**
	 * Construct real toll notification when the road segment is congested.
	 * @param vid				vehicle identifier
	 * @param time				time stamp
	 * @param a					average speed for the last 5 minutes in the road segment
	 * @param vehCount			vehicle count in the segment
	 * @param startOfSimulation	start of simulation to generate the emission time
	 */
	public TollNotification (double vid, double time, double a, double vehCount, long startOfSimulation, double arrivalTime) {
		
		super(0,time,vid);
		
		emit = (System.currentTimeMillis() - startOfSimulation)/1000;
		if (emit - arrivalTime > 5) System.err.println(this.toString() + " violates response time constraint!");
		
		avgSpd = a;
		toll = 2*(vehCount-50)*(vehCount-50);		 	
	}
	
	/**
	 * Construct real toll notification when there is an accident on the road or the road segment is not congested.
	 * @param vid				vehicle identifier
	 * @param time				time stamp
	 * @param a					average speed for the last 5 minutes in the road segment
	 * @param startOfSimulation	start of simulation to generate the emission time
	 */
	public TollNotification (double vid, double time, double a, long startOfSimulation, double arrivalTime) {
		
		super(0,time,vid);
		
		emit = (System.currentTimeMillis() - startOfSimulation)/1000;
		if (emit - arrivalTime > 5) System.err.println(this.toString() + " violates response time constraint!");
		
		avgSpd = a;
		toll = 0;			
	}
	
	/** 
	 * Print this position report.
	 */
	public String toString () {
		return new Double(type).intValue() + ","
				+ new Double(vid).intValue() + ","
				+ new Double(sec).intValue() + ","
				+ new Double(emit).intValue() + "," 
				+ new Double(avgSpd).intValue() + ","
				+ new Double(toll).intValue() + "\n";			
	}
}
