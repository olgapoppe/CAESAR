package iogenerator;

import java.util.concurrent.atomic.AtomicLong;
import static java.lang.Double.*;

public class AtomicDouble extends Number {

	    private AtomicLong bits;

	    public AtomicDouble() {
	        this(0f);
	    }

	    public AtomicDouble (double initialValue) {
	        bits = new AtomicLong(doubleToLongBits(initialValue));
	    }

	   public final void set(double newValue) {
	        bits.set(doubleToLongBits(newValue));
	    }

	    public final double get() {
	        return longBitsToDouble(bits.get());
	    }

	    // Inherited methods
	    public double doubleValue() { return get(); }
	    public float floatValue() 	{ return (float) get(); }	    
	    public int intValue()       { return (int) get(); }
	    public long longValue()     { return (long) get(); }
}
