package operator;

public class Tuple {
	
	String attribute;
	String value;
	
	Tuple (String a, String v) {
		attribute = a;
		value = v;
	}
	
	public String toString () {
		return attribute + " " + value;
	}

}
