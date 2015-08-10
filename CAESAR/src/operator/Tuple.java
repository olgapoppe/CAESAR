package operator;

public class Tuple {
	
	String attribute;
	String value;
	
	Tuple (String a, String v) {
		attribute = a;
		value = v;
	}
	
	public static Tuple parse(String s) {
		String components[] = s.split("=");
		String attribute = components[0];
		String value = components[1];
		return new Tuple(attribute, value);
	}
	
	public String toString () {
		return attribute + "=" + value;
	}

}
