package event;

import java.util.HashMap;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * An event has a type, time stamp and carries a vehicle identifier. 
 * @author Olga Poppe
 */
public abstract class Event {
	
	public double type;
	public double sec;
	public double vid;
	
	public double totalProcessingTime;
	
	public Event (double t, double s, double v) {
		
		type = t;
		sec = s;
		vid = v;
	}
	/***
	 * Print an error message if the latency constraint of 5 seconds is violated.
	 * Update accident failed and maximal latency.
	 * @param p				input position report
	 * @param totalProcessingTime total processing time of the input event
	 * @param failed		true if latency constraint was violated before
	 * @param s				type of complex event 
	 */
	public void printError (PositionReport p, double emit, HashMap<Double,Double> distrFinishTimes, HashMap<Double,Double> schedStartTimes, AtomicBoolean failed, String s) {
		
		/*System.out.println("Delay 1: " + (schedStartTimes.get(p.sec) - distrFinishTimes.get(p.sec)) + 
				" Delay 2: " + (p.schedulerTime - p.distributorTime));*/
		
		if (schedStartTimes.containsKey(p.sec) && distrFinishTimes.containsKey(p.sec)) {
			
			double delay = schedStartTimes.get(p.sec) - distrFinishTimes.get(p.sec);
			totalProcessingTime =  emit - p.distributorTime - delay;
			//if (delay>1) System.out.println("Scheduler wait time for distributor at second " + p.sec + " is " + delay + " seconds too long.");
			
		} else {
			totalProcessingTime =  emit - p.schedulerTime;
		}		
		
		if (!failed.get() && totalProcessingTime > 5) {
			
			/*System.err.println(	s + " FAILED!!!\n" + 
								p.timesToString() + "triggered " + this.toString());*/
			failed.compareAndSet(false, true);
		}	
	}
	
	public abstract String toString();
}
