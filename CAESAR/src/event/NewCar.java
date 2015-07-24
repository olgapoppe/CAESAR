package event;

public class NewCar extends PositionReport {
	
	public NewCar (PositionReport event) {
		super(event.type, event.sec, event.min, event.vid, event.spd, event.xway, event.lane, event.dir, event.seg, event.pos);
	}
}
