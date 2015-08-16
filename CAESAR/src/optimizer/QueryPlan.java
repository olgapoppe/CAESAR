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
			
			operator_string = operator_string.trim();
			
			System.out.println(operator_string);
			
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
	
	ArrayList<OperatorsToMerge> getOperators2merge () {
		
		ArrayList<OperatorsToMerge> ops2merge = new ArrayList<OperatorsToMerge>(); 		
		int i = 0;
		int start = -1;
		int end = -1;
		
		while (i+1<operators.size()) {
			
			while (i+1<operators.size() && operators.get(i).mergable(operators.get(i+1))) {
				
				if (start==-1) start=i;
				i++;
			}
			if (start>-1) {
				
				end = i;
				OperatorsToMerge ops = new OperatorsToMerge(start,end);
				ops2merge.add(ops);
			}
			start = -1;
			i++;
		}
		return ops2merge;
	}
	
	public double getCost() {
		double cost = operators.get(0).getCost();
		for (int i=1; i<operators.size(); i++) {
			cost += operators.get(i-1).getSelectivity()*operators.get(i).getCost();
		}		
		return cost;
	}
	
	public boolean contained (ArrayList<QueryPlan> list, boolean optimized) {
		for (QueryPlan qp : list) {
			if (optimized) {
				if (this.equivalent(qp)) return true;
			} else {
				if (this.equals(qp)) return true;
			}		
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
	
	public boolean equivalent (QueryPlan other) {		
		if (operators.size()!=other.operators.size()) return false;
		for (int i=0; i<operators.size(); i++) {
			if (!operators.get(i).equivalent(other.operators.get(i))) 
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
