package operator;

import org.junit.Assert;
import java.util.ArrayList;
import org.junit.Test;

public class FilterTest {

	@Test
	public void test() {
		
		AtomicPredicate p1 = new AtomicPredicate ("x", ">", 3);		
		ArrayList<AtomicPredicate> conj = new ArrayList<AtomicPredicate>();
		conj.add(p1);
		
		Conjunction c1 = new Conjunction(conj);		
		ArrayList<Conjunction> conjuncts = new ArrayList<Conjunction>();
		conjuncts.add(c1);
		
		Disjunction d1 = new Disjunction(conjuncts);
		
		Filter f1 = new Filter(d1);
		
		AtomicPredicate p2 = new AtomicPredicate ("x", ">", 10);		
		ArrayList<AtomicPredicate> conj2 = new ArrayList<AtomicPredicate>();
		conj2.add(p2);
		
		Conjunction c2 = new Conjunction(conj2);		
		ArrayList<Conjunction> conjuncts2 = new ArrayList<Conjunction>();
		conjuncts2.add(c2);
		
		Disjunction d2 = new Disjunction(conjuncts2);
		
		Filter f2 = new Filter(d2);
		
		Assert.assertTrue(f1.omittable(f1));
		Assert.assertTrue(f1.omittable(f2));
		Assert.assertFalse(f2.omittable(f1));	
	}
}
