package event;

/**
 * An event has a type, time stamp and carries a vehicle identifier.
 * 
 * @author olga
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
	
	public abstract String toString();
}
