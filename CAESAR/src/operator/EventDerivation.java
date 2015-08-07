package operator;

public class EventDerivation extends Operator {
	
	String event_type;
	
	EventDerivation (double c, String et) {
		super(c);
		event_type = et;
	}
	
	public boolean omittable (Operator neighbor) {
		return event_type.equals("TollNotification") || event_type.equals("AccidentWarning");
	}

}
