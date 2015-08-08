package operator;

public class EventDerivation implements Operator {
	
	String event_type;
	
	EventDerivation (String et) {
		event_type = et;
	}
	
	public boolean omittable (Operator neighbor) {
		return event_type.equals("TollNotification") || event_type.equals("AccidentWarning");
	}
	
	public int getCost() {
		return 1;
	}
}
