package operator;

public class EventDerivation implements Operator {
	
	String event_type;
	
	public EventDerivation (String et) {
		event_type = et;
	}
	
	public boolean omittable (Operator neighbor) {
		return !(event_type.equals("tn") || event_type.equals("aw"));
	}
	
	public int getCost() {
		return 1;
	}	
	
	public boolean equals(Operator operator) {
		
		if (!(operator instanceof EventDerivation)) return false;				
		EventDerivation other = (EventDerivation) operator;
		return event_type.equals(other.event_type);
	}
	
	public String toString() {
		return "ED " + event_type;
	}
}
