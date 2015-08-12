package operator;

public class AtomicPredicate {
	
	String attribute;
	String comparisonOperator;
	int value;
	
	public AtomicPredicate (String a, String o, int v) {
		attribute = a;
		comparisonOperator = o;
		value = v;
	}
	
	public static AtomicPredicate parse(String s) {
		String attr;
		String op;
		int val;
		String c;
		
		String[] parts1 = s.split(">");
		if (parts1.length>1) {
			attr = parts1[0].trim();
			op = ">";
			c = parts1[1].trim();
		} else {
			
		String[] parts2 = s.split("<");
		if (parts2.length>1) {
			attr = parts2[0].trim();
			op = "<";
			c = parts2[1].trim();
		} else {
			
		String[] parts3 = s.split(">=");
		if (parts3.length>1) {
			attr = parts3[0].trim();
			op = ">=";
			c = parts3[1].trim();
		} else {
			
		String[] parts4 = s.split("<=");
		if (parts4.length>1) {
			attr = parts4[0].trim();
			op = "<=";
			c = parts4[1].trim();
		} else {

		String[] parts5 = s.split("=");
		if (parts5.length>1) {
			attr = parts5[0].trim();
			op = "=";
			c = parts5[1].trim();
		} else {

			String[] parts6 = s.split("!=");
			attr = parts6[0].trim();
			op = "!=";
			c = parts6[1].trim();
		}}}}}
		
		val = Integer.parseInt(c);
		
		return new AtomicPredicate(attr, op, val);
	}
	
	
	
	public AtomicPredicate getNegated() {
		
		String new_comparison_operator;
		
		if (this.comparisonOperator == "=") { 	new_comparison_operator = "!="; } else {
		if (this.comparisonOperator == "!=") { 	new_comparison_operator = "="; } else {
		if (this.comparisonOperator == ">") { 	new_comparison_operator = "<="; } else {
		if (this.comparisonOperator == ">=") { 	new_comparison_operator = "<"; } else {
		if (this.comparisonOperator == "<") { 	new_comparison_operator = ">="; } else {
												new_comparison_operator = ">"; }}}}}
		
		return new AtomicPredicate(this.attribute, new_comparison_operator, this.value);
	}
	
	public boolean implies (AtomicPredicate p) {
		
		int c1 = value;
		int c2 = p.value;
		
		return 	this.attribute.equals(p.attribute) &&
				
				((this.comparisonOperator == "=" && p.comparisonOperator == "=" && c1 == c2) ||
						
				(this.comparisonOperator == "!=" && p.comparisonOperator == "!=" && c1 == c2) ||
				
				(this.comparisonOperator == "<" && p.comparisonOperator == "<" && c1 <= c2) ||
				(this.comparisonOperator == "<" && p.comparisonOperator == "<=" && c1 < c2) ||
				
				(this.comparisonOperator == "<=" && p.comparisonOperator == "<" && c1 < c2) ||
				(this.comparisonOperator == "<=" && p.comparisonOperator == "<=" && c1 <= c2) ||
				
				(this.comparisonOperator == ">" && p.comparisonOperator == ">" && c1 >= c2) ||
				(this.comparisonOperator == ">" && p.comparisonOperator == ">=" && c1 > c2) ||
				
				(this.comparisonOperator == ">=" && p.comparisonOperator == ">" && c1 > c2) ||
				(this.comparisonOperator == ">=" && p.comparisonOperator == ">=" && c1 >= c2));				
	}
	
	public boolean impliedBy (Conjunction c) {
		for (AtomicPredicate p : c.atomicPredicates) {
			if (p.implies(this)) { return true; }
		}
		return false;
	}
	
	public boolean contradicts (AtomicPredicate p) {	
		
		int c1 = value;
		int c2 = p.value;
		
		boolean result = 	this.attribute.equals(p.attribute) &&
				
				((this.comparisonOperator == "=" && p.comparisonOperator == "=" && c1 != c2) ||
				
				(this.comparisonOperator == "=" && p.comparisonOperator == "<" && c1 >= c2) ||
				(this.comparisonOperator == "=" && p.comparisonOperator == "<=" && c1 > c2) ||
				
				(this.comparisonOperator == "!=" && p.comparisonOperator == "=" && c1 == c2) ||
				
				(this.comparisonOperator == ">" && p.comparisonOperator == "=" && c1 >= c2) ||
				(this.comparisonOperator == ">" && p.comparisonOperator == "<" && c1 >= c2) ||
				(this.comparisonOperator == ">" && p.comparisonOperator == "<=" && c1 >= c2) ||
				
				(this.comparisonOperator == ">=" && p.comparisonOperator == "=" && c1 > c2) ||
				(this.comparisonOperator == ">=" && p.comparisonOperator == "<" && c1 >= c2)) ||
				(this.comparisonOperator == ">=" && p.comparisonOperator == "<=" && c1 > c2); 
		
		//System.out.println("\n\n" + this.toString() + " vs " + p.toString() + " "+ result + "\n\n");
		
		return result;
	}
	
	public boolean contradicts (Conjunction c) {
		for (AtomicPredicate p : c.atomicPredicates) {
			if (this.contradicts(p) || p.contradicts(this)) { return true; }
		}
		return false;
	}
	
	public AtomicPredicate deepCopy() {
		return new AtomicPredicate(this.attribute, this.comparisonOperator, this.value);
	}
	
	public String toString() {
		return attribute + comparisonOperator + value;
	}

}
