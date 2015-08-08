package operator;

import java.util.ArrayList;
import org.junit.Assert;
import org.junit.Test;

public class ProjectionTest {

	@Test
	public void test() {
		
		// Same attributes
		ArrayList<String> attributes1 = new ArrayList<String>();
		attributes1.add("a");
		attributes1.add("b");
		attributes1.add("c");
		Projection p1 = new Projection(attributes1);		
		
		Projection p2 = new Projection(attributes1);	
		
		Assert.assertTrue(p1.omittable(p2));
		
		// Less attributes
		ArrayList<String> attributes3 = new ArrayList<String>();
		attributes3.add("a");
		attributes3.add("b");		
		Projection p3 = new Projection(attributes3);
		
		Assert.assertTrue(p1.omittable(p3));
		Assert.assertFalse(p3.omittable(p1));
		
		// Other attributes
		ArrayList<String> attributes4 = new ArrayList<String>();
		attributes4.add("x");
		attributes4.add("b");
		Projection p4 = new Projection(attributes4);
		
		Assert.assertFalse(p1.omittable(p4));
		Assert.assertFalse(p4.omittable(p1));
	}

}
