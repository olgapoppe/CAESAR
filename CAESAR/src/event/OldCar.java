package event;

public class OldCar extends PositionReport {
	
	public OldCar (PositionReport event) {
		super(event.type, event.sec, event.min, event.id, event.spd, event.xway, event.lane, event.dir, event.seg, event.pos);
	}
}
