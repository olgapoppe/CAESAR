package event;

import java.util.concurrent.atomic.AtomicBoolean;

/**
 * An event has a type, time stamp and carries a vehicle identifier. 
 * @author Olga Poppe
 */
public abstract class Event {
	
	public double type;
	public double sec;
	public double vid;
	
	public Event (double t, double s, double v) {
		
		type = t;
		sec = s;
		vid = v;
	}
	/***
	 * Print an error message if the latency constraint of 5 seconds is violated.
	 * Update accident failed and maximal latency.
	 * @param p				input position report
	 * @param emit			emission time of complex event
	 * @param failed		true if latency constraint was violated before
	 * @param max_latency	maximal latency so far
	 * @param s				type of complex event 
	 */
	public void printError (PositionReport p, double emit, AtomicBoolean failed, String s, double scheduling_time) {
		
		// Print an error message and update the accident warning failed variable
		double diff = emit - p.distributorTime;
		
		if (!failed.get() && diff > 5) {
			
			System.err.println(	s + " FAILED!!!\n" + 
								p.timesToString() + 
								"triggered " + this.toString() +
								"\nTransaction scheduling time is " + scheduling_time);
			failed.compareAndSet(false, true);
		}	
	}
	
	public abstract String toString();
}
