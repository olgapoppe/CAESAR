package optimizer;

import java.util.ArrayList;
import java.util.LinkedList;
import operator.*;

public class QueryPlan {
	
	LinkedList<Operator> operators;
	
	public QueryPlan(LinkedList<Operator> ops) {
		operators = ops;
	}
	
	public static QueryPlan parse(String s) {
		
		LinkedList<Operator> operators = new LinkedList<Operator>();			
		String allOperators[] = s.split("; ");
		
		for (String operator_string : allOperators) {
			
			Operator operator;
			if (operator_string.startsWith("RC")) { operator = RunCreation.parse(operator_string); } else {
			if (operator_string.startsWith("RU")) { operator = RunUpdate.parse(operator_string); } else {
			if (operator_string.startsWith("RD")) { operator = RunDeletion.parse(operator_string); } else {
			
			if (operator_string.startsWith("CI")) { operator = ContextInitiation.parse(operator_string); } else {
			if (operator_string.startsWith("CS")) { operator = ContextSwitch.parse(operator_string); } else {
			if (operator_string.startsWith("CT")) { operator = ContextTermination.parse(operator_string); } else {
			if (operator_string.startsWith("CW")) { operator = ContextWindow.parse(operator_string); } else {
			
			if (operator_string.startsWith("ED")) { operator = EventDerivation.parse(operator_string); } else {
			if (operator_string.startsWith("PR")) { operator = Projection.parse(operator_string); } else {
													operator = Filter.parse(operator_string); }}}}}}}}}			
			operators.add(operator);
		}			
		return new QueryPlan(operators);
	}
	
	public int getCost() {
		int cost = 0;
		for (Operator op : operators) {
			cost += op.getCost();
		}		
		return cost;
	}
	
	public boolean contained (ArrayList<QueryPlan> list) {
		for (QueryPlan qp : list) {
			if (this.equals(qp)) return true;
		}
		return false;
	}
	
	public boolean equals (QueryPlan other) {
		
		if (operators.size()!=other.operators.size()) return false;
		for (int i=0; i<operators.size(); i++) {
			if (!operators.get(i).equals(other.operators.get(i))) 
				return false;	
		}
		return true;
	}
	
	public String toString() {
		String s = "";
		for (Operator op : operators) {
			s += op.toString() + " ";
		}
		return s;
	}
}
