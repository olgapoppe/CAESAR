package operator;

public class EventDerivation extends Operator {
	
	String event_type;
	
	EventDerivation (double c, String et) {
		super(c);
		event_type = et;
	}

}
