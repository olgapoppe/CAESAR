package operator;

import java.util.ArrayList;

public class RunUpdate extends Operator {
	
	ArrayList<Tuple> tuples;
	
	RunUpdate (ArrayList<Tuple> t) {
		tuples = t;
	}
	
	public static RunUpdate parse(String s) {
		
		ArrayList<Tuple> tuples = new ArrayList<Tuple>();
		String tuples_string = s.substring(3); // Skip "RU "	
		String allTuples[] = tuples_string.split(", ");
		for (String tuple_string : allTuples) {
			Tuple tuple = Tuple.parse(tuple_string);
			tuples.add(tuple);
		}				
		return new RunUpdate(tuples);
	}
	
	public boolean omittable (Operator neighbor) {
		return this.equals(neighbor);		
	}
	
	public int getCost() {
		return tuples.size();
	}
	
	public boolean equals (Operator operator) {
		if (!(operator instanceof RunUpdate)) return false;
		RunUpdate other = (RunUpdate) operator;
		return tuples.containsAll(other.tuples) && other.tuples.containsAll(tuples);		
	}
	
	public String toString() {
		String s = "RU";
		for (Tuple t : tuples) {
			s += t.toString() + ", ";
		}
		return s;
	}
}
