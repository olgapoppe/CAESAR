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
	
	public void printError (double emit, double arrivalTime, AtomicBoolean failed, String s) {
		
		if (!failed.get() && emit - arrivalTime > 5) { 
			System.err.println(s + " FAILED!!! " + this.toString() + " attived at " + arrivalTime);
			failed.compareAndSet(false, true);
		}
	}
	
	public abstract String toString();
}
