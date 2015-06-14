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
	
	public void printError (PositionReport p, double emit, AtomicBoolean failed, double distrTimeStamp, String s) {
		
		int diff = new Double(emit).intValue() - new Double(distrTimeStamp/1000).intValue();
		
		if (!failed.get() && diff > 5) {
			
			System.err.println(	s + " FAILED!!!\n" + 
								p.timesToString() + 
								"triggered " + this.toString() + 
								"Distibutor progress at " + p.sec + " is " + distrTimeStamp/1000);
			failed.compareAndSet(false, true);
		}
	}
	
	public abstract String toString();
}
