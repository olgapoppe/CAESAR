package event;

public class NewCar extends PositionReport {
	
	public NewCar (PositionReport e) {
		super(e.type, e.sec, e.min, e.vid, e.spd, e.xway, e.lane, e.dir, e.seg, e.pos);
	}
}
