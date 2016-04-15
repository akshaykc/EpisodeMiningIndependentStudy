package structures;

import java.io.Serializable;
import java.util.HashMap;

/**
 * The implementation of node of an episode trie.
 * 
 * @author aox
 *
 */
public class TrieNode implements Serializable {

	private static final long serialVersionUID = 1994071553995248284L;

	private String event;
	private Integer Te;
	private boolean lastMO;
	private boolean[] children = null;
	private String episode;
	private float eventProbability;
	
	public TrieNode(String evt, Integer Te, Integer childSize, float evtPrb) {
		this.event = evt;
		this.Te = Te;
		this.eventProbability = evtPrb;
		this.children = new boolean[childSize];
		for (int i = 0; i < childSize; i++) {
			children[i] = false;
		}
		this.lastMO = true;
	}
	
	public float getEventProbability() {
		return eventProbability;
	}

	public void setEventProbability(float evtPrb) {
		this.eventProbability = evtPrb;
	}
	
	public String getEvent() {
		return event;
	}

	public void setEvent(String event) {
		this.event = event;
	}

	public Integer getTe() {
		return Te;
	}

	public void setTe(Integer te) {
		Te = te;
	}

	public boolean isLastMO() {
		return lastMO;
	}

	public void setLastMO(boolean lastMO) {
		this.lastMO = lastMO;
	}

	public boolean[] getChildren() {
		return children;
	}

	public void setChildren(boolean[] children) {
		this.children = children;
	}

	public String getEpisode() {
		return episode;
	}

	public void setEpisode(String episode) {
		this.episode = episode;
	}

	public static long getSerialversionuid() {
		return serialVersionUID;
	}

}
