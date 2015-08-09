package operator;

import java.util.ArrayList;

public class RunUpdate implements Operator {
	
	ArrayList<Tuple> tuples;
	
	RunUpdate (ArrayList<Tuple> t) {
		tuples = t;
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
