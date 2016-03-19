package structures;

/**
 * Class of parameters
 * 
 * @author aox
 *
 */
public class OnlineParas {
	private int min_sup;
	private int delta;
	private int DELTA;
	private int updateFrequency;

	public OnlineParas(int support, int delta, int window, int frequency) {
		this.min_sup = support;
		this.delta = delta;
		this.DELTA = window;
		this.updateFrequency = frequency;
	}

	public int getMin_sup() {
		return min_sup;
	}

	public void setMin_sup(int min_sup) {
		this.min_sup = min_sup;
	}

	public int getDelta() {
		return delta;
	}

	public void setDelta(int delta) {
		this.delta = delta;
	}

	public int getDELTA() {
		return DELTA;
	}

	public void setDELTA(int dELTA) {
		DELTA = dELTA;
	}

	public int getUpdateFrequency() {
		return updateFrequency;
	}

	public void setUpdateFrequency(int updateFrequency) {
		this.updateFrequency = updateFrequency;
	}

}
