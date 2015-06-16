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
	
	public void printError (PositionReport p, double emit, AtomicBoolean failed, String s, long distrProgr) {
		
		int diff = new Double(emit).intValue() - new Double(p.sec).intValue();
		
		if (!failed.get() && diff > 5) {
			
			System.err.println(	s + " FAILED!!!\n" + 
								p.timesToString() + 
								"triggered " + this.toString() +
								"Distributer progress in " + p.sec + " is " + distrProgr);
			failed.compareAndSet(false, true);
		}
	}
	
	public abstract String toString();
}
