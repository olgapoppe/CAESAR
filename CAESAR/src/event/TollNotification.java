package event;

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
	 * @param startOfSimulation	start of simulation to generate the emission time
	 * @param tnf				indicates whether toll notification failed already
	 */
	public TollNotification (PositionReport p, double a, double vehCount, long startOfSimulation, AtomicBoolean tnf, long distrProgr, double scheduler_wakeup_time) {
		
		super(0,p.sec,p.vid);	
		
		emit = (System.currentTimeMillis() - startOfSimulation)/new Double(1000) - scheduler_wakeup_time;
		avgSpd = a;
		toll = 2*(vehCount-50)*(vehCount-50);
		
		printError (p, emit, scheduler_wakeup_time, tnf, "TOLL NOTIFICATIONS", distrProgr);		 	
	}
	
	/**
	 * Construct real toll notification when there is an accident on the road or the road segment is not congested.
	 * @param p					position report
	 * @param a					average speed for the last 5 minutes in the road segment
	 * @param startOfSimulation	start of simulation to generate the emission time
	 * @param tnf				indicates whether toll notification failed already
	 */
	public TollNotification (PositionReport p, double a, long startOfSimulation, AtomicBoolean tnf, long distrProgr, double scheduler_wakeup_time) {
		
		super(0,p.sec,p.vid);
		
		emit = (System.currentTimeMillis() - startOfSimulation)/new Double(1000) - scheduler_wakeup_time;
		avgSpd = a;
		toll = 0;
		
		printError (p, emit, scheduler_wakeup_time, tnf, "TOLL NOTIFICATIONS", distrProgr);			
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
