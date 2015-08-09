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
	 * @param totalProcessingTime total processing time of the input event
	 * @param failed		true if latency constraint was violated before
	 * @param s				type of complex event 
	 */
	public void printError (PositionReport p, double totalProcessingTime, AtomicBoolean failed, String s) {
		
		if (!failed.get() && totalProcessingTime > 5) {
			
			System.err.println(	s + " FAILED!!!\n" + 
								p.timesToString() + 
								"triggered " + this.toString());
			failed.compareAndSet(false, true);
		}	
	}
	
	public abstract String toString();
}
