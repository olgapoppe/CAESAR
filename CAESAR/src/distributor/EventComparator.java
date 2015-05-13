package distributor;

import java.util.Comparator;

import event.Event;

/**
 * Event comparator determines the position of an event in an ordered data structure.
 * Events are ordered by their time stamps.  
 * @author Olga Poppe
 */
public class EventComparator implements Comparator<Event> {
		
	public int compare (Event e1, Event e2) {
	    return (e1.sec >= e2.sec)? 1 : -1;        
	}
}
