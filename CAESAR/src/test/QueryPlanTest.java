package test;

import java.util.ArrayList;
import java.util.LinkedList;
import operator.*;
import optimizer.*;
import org.junit.Assert;
import org.junit.Test;

public class QueryPlanTest {

	@Test
	public void test() {
		
		// First query: FI1 x>3 CW1 c PR1 x,y,z ED1 a
		
		AtomicPredicate p1 = new AtomicPredicate("x", ">", 10);
    	ArrayList<AtomicPredicate> ps1 = new ArrayList<AtomicPredicate>();
    	ps1.add(p1);
    	Conjunction c1 = new Conjunction(ps1);
    	ArrayList<Conjunction> cs1 = new ArrayList<Conjunction>();
    	cs1.add(c1);
    	Disjunction d1 = new Disjunction(cs1);
    	Filter fi1 = new Filter(d1);
    	
    	ContextWindow cw1 = new ContextWindow("c");
    	
    	ArrayList<String> attr1 = new ArrayList<String>();
    	attr1.add("x");
    	attr1.add("y");
    	Projection pr1 = new Projection(attr1);
    	
    	EventDerivation ed1 = new EventDerivation("b");
    	
    	LinkedList<Operator> ops = new LinkedList<Operator>();
    	ops.add(fi1);
    	ops.add(cw1);
    	ops.add(pr1);
    	ops.add(ed1);
    	QueryPlan qp = new QueryPlan(ops);
    	
    	// Second query: FI2 x>10 CW2 c PR2 x,y ED2 b
    	
    	AtomicPredicate p2 = new AtomicPredicate("x", ">", 10);
    	ArrayList<AtomicPredicate> ps2 = new ArrayList<AtomicPredicate>();
    	ps2.add(p2);
    	Conjunction c2 = new Conjunction(ps2);
    	ArrayList<Conjunction> cs2 = new ArrayList<Conjunction>();
    	cs2.add(c2);
    	Disjunction d2 = new Disjunction(cs2);
    	Filter fi2 = new Filter(d2);
    	
    	ContextWindow cw2 = new ContextWindow("c");
    	
    	ArrayList<String> attr2 = new ArrayList<String>();
    	attr2.add("x");
    	attr2.add("y");
    	Projection pr2 = new Projection(attr2);
    	
    	EventDerivation ed2 = new EventDerivation("b");    	
    	
    	LinkedList<Operator> ops2 = new LinkedList<Operator>();
    	ops2.add(fi2);
    	ops2.add(cw2);
    	ops2.add(pr2);
    	ops2.add(ed2);
    	QueryPlan qp2 = new QueryPlan(ops2);
    	
    	ArrayList<QueryPlan> query_plans = new ArrayList<QueryPlan>();
    	query_plans.add(qp2);
    	
    	Assert.assertTrue(fi1.equals(fi2));
    	Assert.assertTrue(cw1.equals(cw2));
    	Assert.assertTrue(pr1.equals(pr2));
    	Assert.assertTrue(ed1.equals(ed2));
    	Assert.assertTrue(qp.contained(query_plans,false));
	}

}
