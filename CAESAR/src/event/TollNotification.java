package event;

import java.util.HashMap;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * In addition to type, time stamp and vehicle identifier, 
 * an toll notification has an emission time stamp, average speed and a toll value.
 * @author Olga Poppe
 */
public class TollNotification extends Event {
	
	public double emit;
	double avgSpd;
	double toll;
	
	/**
	 * Construct real toll notification when the road segment is congested.
	 * @param p					position report
	 * @param a					average speed for the last 5 minutes in the road segment
	 * @param vehCount			vehicle count in the segment
	 * @param delay				scheduler wake-up time after waiting for distributor
	 * @param startOfSimulation	start of simulation to generate the emission time
	 * @param tnf				indicates whether toll notification failed already
	 */
	public TollNotification (PositionReport p, double a, double vehCount, 
			HashMap<Double,Double> distrFinishTimes, HashMap<Double,Double> schedStartTimes, long startOfSimulation, AtomicBoolean tnf) {
		
		super(0,p.sec,p.vid);	
		
		emit = (System.currentTimeMillis() - startOfSimulation)/new Double(1000);
		avgSpd = a;
		toll = 2*(vehCount-50)*(vehCount-50);
		
		printError (p, emit, distrFinishTimes, schedStartTimes, tnf, "TOLL NOTIFICATIONS");		 	
	}
	
	/**
	 * Construct zero toll notification when there is not accident on the road and the road segment is congested.
	 * @param p					position report
	 * @param a					average speed for the last 5 minutes in the road segment
	 * @param delay				scheduler wake-up time after waiting for distributor
	 * @param startOfSimulation	start of simulation to generate the emission time
	 * @param tnf				indicates whether toll notification failed already
	 */
	public TollNotification (PositionReport p, double a, 
			HashMap<Double,Double> distrFinishTimes, HashMap<Double,Double> schedStartTimes, long startOfSimulation, AtomicBoolean tnf) {
		
		super(0,p.sec,p.vid);
		
		emit = (System.currentTimeMillis() - startOfSimulation)/new Double(1000);
		avgSpd = a;
		toll = 0;	
		
		printError (p, emit, distrFinishTimes, schedStartTimes, tnf, "TOLL NOTIFICATIONS");			
	}
	
	/** 
	 * Print this position report.
	 */
	public String toString () {
		return new Double(type).intValue() + ","
				+ new Double(vid).intValue() + ","
				+ new Double(sec).intValue() + ","
				+ emit + "," 
				+ new Double(avgSpd).intValue() + ","
				+ new Double(toll).intValue() + "\n";			
	}
}
