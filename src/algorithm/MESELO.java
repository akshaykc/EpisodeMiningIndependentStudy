package algorithm;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.io.UnsupportedEncodingException;

import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import org.apache.commons.lang.StringUtils;

import structures.OnlineParas;
import structures.TrieNode;

import utils.SerializeUtil;

/**
 * @author aox version 1.0 MESELO algorithm
 *
 */
public class MESELO {

	private String inputFile;
	private String externalWeightsFile;
	private String outputFile;
	private Map<String, Integer> frequentEpisodeSet = new HashMap<String, Integer>();
	/*private Map<String, Integer> topKEpisodeSet = new HashMap<String, Integer>();
	private int K = 10;
	private int dynamicMinSup = 100000;
	private String dynamicMinEpisode;*/
	private Map<String, Integer> candidateEpisodeSet = new ConcurrentHashMap<String, Integer>();
	private long startTime;
	private long endTime;
	private List<ArrayList<TrieNode>> TrieList = new ArrayList<ArrayList<TrieNode>>();
	private HashSet<String> Q = new HashSet<String>();

	private java.sql.Connection con = null;
	private OnlineParas paras = null;
	private List<ArrayList<TrieNode>> MexBuffer = new ArrayList<ArrayList<TrieNode>>();

	private Integer round;
	private double averageTime;

	private Long deltaTime = 0L;

	private int begin = 0;
	private int end = 0;

	private int ESetSize = 0;
	
	private Map<String, Integer> externalWeightDict = new HashMap<String, Integer>();

	private Map<String, Integer> eventDic = new HashMap<String, Integer>();

	private int timestamp = 1;

	private static String INSERT = "insert into node values (?,?)";
	private static String PERFORMANCEFILE = "./data/experiments/performance-online.csv";

	static ExecutorService es = Executors.newFixedThreadPool(5);

	/**
	 * @param inputFile
	 *            - input event sequence dataset.
	 * @param outputFile
	 *            - output frequent episode by each time slot.
	 * @param min_sup
	 *            - minimal support threshold.
	 * @param delta
	 *            - maximal occurrence window threshold.
	 * @param window_size
	 *            - window size for valid sequence.
	 * @param updateFrequency
	 *            - when time slot is bigger than DELTA, the algorithm will
	 *            output frequent episodes every updateFrequency.
	 * @param begin
	 *            - begin time slot on the sequence.
	 * @param end
	 *            - end time slot on the sequence. for whole sequence to test,
	 *            begin = 1, end = |event sequence|.
	 */
	public MESELO(String inputFile, String outputFile, String externalWeightsFile, int min_sup, int delta,
			int window_size, int updateFrequency, int begin, int end) {
		this.inputFile = inputFile;
		this.outputFile = outputFile;
		this.externalWeightsFile=externalWeightsFile;
		this.paras = new OnlineParas(min_sup, delta, window_size,
				updateFrequency);
		String driver = "com.mysql.jdbc.Driver";
		String url = "jdbc:mysql://127.0.0.1:3306/onlinefem"; // ��ݿ�URL
		String user = "root"; // ��ݿ��½��
		String password = "311";
		this.begin = begin;
		this.end = end;

		try {
			this.con = DriverManager.getConnection(url, user, password);
			System.out.println("Connection: " + con);
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

		this.ESetSize = this.getEventDictionary(this.inputFile, this.externalWeightsFile);
	}

	/**
	 * @param input
	 *            - input file of event sequence.
	 * @return the number of events in the sequence.
	 */
	private int getEventDictionary(String input, String weightsFile) {
		// TODO Auto-generated method stub
		Set<String> eventSet = new HashSet<String>();
		try {
			BufferedReader reader = new BufferedReader(new InputStreamReader(
					new FileInputStream(input), "UTF-8"));
			String line = null;
			int offset = 0;
			while ((line = reader.readLine()) != null) {
				String[] array = StringUtils.split(line.trim(), ' ');
				for (String event : array) {
					eventSet.add(stripInternalWeight(event));
					if (!this.eventDic.containsKey(stripInternalWeight(event))) {
						eventDic.put(stripInternalWeight(event), offset++);
					}
				}
			}
			reader.close();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		try{
			BufferedReader reader = new BufferedReader(new InputStreamReader(
					new FileInputStream(weightsFile), "UTF-8"));
			String line = null;
			int externalWeight = 0;
			String event;
			while ((line = reader.readLine()) != null) {
				String[] array = StringUtils.split(line.trim(), ' ');
				event = array[0];
				externalWeight = Integer.parseInt(array[1]);
				if (!this.externalWeightDict.containsKey(event)) {
					externalWeightDict.put(event, externalWeight);
				}
			}
			reader.close();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return eventSet.size();
	}
	
	public String stripInternalWeight(String event){
		//return event.replaceAll("[^A-Za-z]", "");
		return event;
	}
	
	/**
	 * The core of algorithm.
	 */
	public void algCore() {
		try {
			BufferedReader reader = new BufferedReader(new InputStreamReader(
					new FileInputStream(this.inputFile), "UTF-8"));
			String line = null;
			round = 0;
			int lineNum = 1;
			BufferedWriter writer = new BufferedWriter(new OutputStreamWriter(
					new FileOutputStream(this.outputFile), "UTF-8"));
			while ((line = reader.readLine()) != null) {
				// if (timestamp % 100 == 0) {
				// System.out.println("Curren ti = " + timestamp);
				// }
				if (lineNum < this.begin) {
					lineNum++;
					timestamp++;
				} else {
					final Set<String> tmpS = this.Q;
					this.Q = new HashSet<String>();
					es.submit(new Runnable() {
						@Override
						public void run() {
							tmpS.clear();
						}
					});
					String[] EkplusOne = StringUtils.split(line, " ");
					boolean isVaildEkplusOne = false;
					// if (EkplusOne.length <= eventUpperBound) {
					isVaildEkplusOne = true;
					// if ((timestamp >= this.paras.getMax_win())
					// && ((timestamp - this.paras.getMax_win())
					// % this.paras.getUpdateFrequency() == 0)) {
					this.startTime = System.currentTimeMillis();
					// }
					ArrayList<TrieNode> trie = BuildTrie(EkplusOne, timestamp);
					if (trie != null)
						this.TrieList.add(trie);
					if (this.TrieList.size() >= 1) {
						this.Q = UpdateTries(this.TrieList, EkplusOne,
								timestamp);
					}
					if ((timestamp >= this.paras.getDELTA())
							&& ((timestamp - this.paras.getDELTA())
									% this.paras.getUpdateFrequency() == 0)) {
						Integer Tend = timestamp - this.paras.getDELTA();
						Integer Tbegin = timestamp - this.paras.getDELTA()
								- this.paras.getUpdateFrequency() + 1;
						this.frequentEpisodeSet = ExtrudeF(
								this.candidateEpisodeSet,
								this.frequentEpisodeSet, this.Q, Tbegin, Tend);
					} else {
						frequentEpisodeSet = UpdateHighUtilityEpisode(
								candidateEpisodeSet, frequentEpisodeSet/*, topKEpisodeSet*/,
								this.Q, this.paras);
					}

					if (TrieList.size() > 0) {
						ArrayList<TrieNode> trie4Cut = TrieList.get(0);
						Integer trie4CutTime = trie4Cut.get(0).getTe();
						if (timestamp - trie4CutTime >= paras.getDelta() - 1) {
							trie4Cut = TrieList.remove(0);
							if (timestamp <= paras.getDELTA()) {
								try {
									PreparedStatement insertstatement = con
											.prepareStatement(MESELO.INSERT);
									Insert2DB indb = new Insert2DB(
											insertstatement, trie4Cut);
									es.submit(indb);
								} catch (SQLException e) {
									// TODO Auto-generated catch block
									e.printStackTrace();
								}

							} else {
								MexBuffer.add(trie4Cut);
							}
						}
					}
					this.endTime = System.currentTimeMillis();
					deltaTime += (long) (this.endTime - this.startTime);
					if ((timestamp >= this.paras.getDELTA())
							&& ((timestamp - this.paras.getDELTA())
									% this.paras.getUpdateFrequency() == 0)) {
						this.printFrequentEpisode(writer, frequentEpisodeSet,
								timestamp);
					}
					if (timestamp >= this.end) {
						break;
					}
					round++;
					timestamp++;
				}
			}
			reader.close();
			writer.close();
			this.averageTime = deltaTime.doubleValue() / round.doubleValue();
			System.err.println("Average run time = " + averageTime + "(ms)"
					+ " Round number = " + round);

		} catch (IOException e) {
			System.err.println(e.getMessage());
		} catch (Exception e) {
			this.averageTime = deltaTime.doubleValue() / round.doubleValue();
			System.err.println("Exception caused Average run time = " + averageTime + "(ms)"
					+ " Round number = " + round);
		} finally {
			try {
				es.shutdown();
				es.awaitTermination(Integer.MAX_VALUE, TimeUnit.DAYS);
				if (this.con != null) {
					clearDB();
					this.con.close();
				}
			} catch (SQLException | InterruptedException e) {
				// TODO Auto-generated catch block
				System.err.println(e.getMessage());
			}
			this.printStats(this.PERFORMANCEFILE);
		}
	}

	/**
	 * clear database after processing.
	 */
	private void clearDB() {
		// TODO Auto-generated method stub
		String deleteSQL = "truncate table node";
		try {
			Statement st = con.createStatement();
			st.execute(deleteSQL);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	/**
	 * print frequent episode set at given time slot
	 * 
	 * @param writer
	 *            - frequent episode file writer
	 * @param frequentEpisodeSet
	 *            - set of frequent episode.
	 * @param timestamp
	 *            - current time slot
	 */
	private void printFrequentEpisode(BufferedWriter writer,
			Map<String, Integer> frequentEpisodeSet, int timestamp) {
		// TODO Auto-generated method stub

		String outLine = "";
		for (Entry<String, Integer> entry : frequentEpisodeSet.entrySet()) {
			String episode = entry.getKey();
			outLine += episode + ",";
		}
		if (outLine.length() > 0) {
			outLine = outLine.substring(0, outLine.length() - 1);
			try {
				writer.write(timestamp + "," + outLine + "\n");
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
	}

	/**
	 * update support in candidate set as well as frequent set
	 * 
	 * @param candidateEpisodeSet
	 * @param frequentEpisodeSet
	 * @param occList
	 * @param paras
	 * @return updated frequent episode set and candidate episode set
	 */
	private Map<String, Integer> UpdateFrequentEpisode(
			Map<String, Integer> candidateEpisodeSet,
			Map<String, Integer> frequentEpisodeSet, HashSet<String> occList,
			OnlineParas paras) {
		// TODO Auto-generated method stub
		if (occList.size() > 0) {
			Iterator<String> iter = occList.iterator();
			while (iter.hasNext()) {
				String episode = iter.next();
				if (episode.indexOf("->") > -1) {
					if (frequentEpisodeSet.containsKey(episode)) {
						frequentEpisodeSet.put(episode,
								frequentEpisodeSet.get(episode) + 1);
					} else {
						if (candidateEpisodeSet.containsKey(episode)) {
							candidateEpisodeSet.put(episode,
									candidateEpisodeSet.get(episode) + 1);
						} else {
							candidateEpisodeSet.put(episode, 1);
						}
						if (candidateEpisodeSet.get(episode) >= paras
								.getMin_sup()) {
							frequentEpisodeSet.put(episode,
									candidateEpisodeSet.get(episode));
							candidateEpisodeSet.remove(episode);
						}
					}
				}
			}
		}
		return frequentEpisodeSet;
	}
	private Map<String, Integer> UpdateHighUtilityEpisode(
			Map<String, Integer> candidateEpisodeSet,
			Map<String, Integer> highUtilityEpisodeSet, /*Map<String, Integer> topKSet,*/
			HashSet<String> occList,OnlineParas paras){
		int utility;
		if (occList.size() > 0) {
			Iterator<String> iter = occList.iterator();
			while (iter.hasNext()) {
				String episode = iter.next();
				utility = 0;
				if (episode.indexOf("->") > -1) {
					String[] array = episode.split("->");
					for(String event:array){
						utility += Integer.parseInt(event.replaceAll("[A-Za-z]",""))*
								externalWeightDict.get(event.replaceAll("[^A-Za-z]", ""));
					}
					if (highUtilityEpisodeSet.containsKey(episode)) {
						highUtilityEpisodeSet.put(episode,
								highUtilityEpisodeSet.get(episode) + utility);
					} else {
						if (candidateEpisodeSet.containsKey(episode)) {
							candidateEpisodeSet.put(episode,
									candidateEpisodeSet.get(episode) + utility);
						} else {
							candidateEpisodeSet.put(episode, utility);
						}
						if (candidateEpisodeSet.get(episode) >= paras
								.getMin_sup()) {
							highUtilityEpisodeSet.put(episode,
									candidateEpisodeSet.get(episode));
							candidateEpisodeSet.remove(episode);
						}
					}
					/*if(topKSet.size() < K){
						if(topKSet.containsKey(episode)){
							topKSet.put(episode,topKSet.get(episode) + utility);
							if(topKSet.get(episode) < dynamicMinSup){
								dynamicMinSup = topKSet.get(episode);
								dynamicMinEpisode = episode;
							}
						}
						else{
							topKSet.put(episode, utility);
							if(utility < dynamicMinSup){
								dynamicMinSup = utility;
								dynamicMinEpisode = episode;
							}
						}
					}else
					{
						if(topKSet.containsKey(episode))
					}*/
				}
			}
		}
		return highUtilityEpisodeSet;
	}

	/**
	 * When prepare to output frequent episode, load expired tries from DB and
	 * update their support
	 * 
	 * @param candidateEpisodeSet
	 * @param frequentEpisodeSet
	 * @param occList
	 * @param tbegin
	 * @param tend
	 * @return extruded frequent and candidate episode set
	 */
	private Map<String, Integer> ExtrudeF(
			Map<String, Integer> candidateEpisodeSet,
			Map<String, Integer> frequentEpisodeSet, HashSet<String> occList,
			Integer tbegin, Integer tend) {
		int utility = 0;
		// TODO Auto-generated method stub
		if (tbegin <= tend && tbegin > 0) {
			HashMap<String, Integer> volatileSet = new HashMap<String, Integer>();
			String select = "select t.trie as trie from node as t where t.time >= "
					+ tbegin + " and t.time <= " + tend;
			try {
				Statement st = con.createStatement();
				ResultSet rs = st.executeQuery(select);
				while (rs.next()) {
					ArrayList<TrieNode> trie = (ArrayList<TrieNode>) SerializeUtil
							.unserialize(rs.getBytes(1));
					
					for (TrieNode node : trie) {
						utility = 0;
						String curEpisode = node.getEpisode();
						utility += Integer.parseInt(curEpisode.replaceAll("[A-Za-z]",""))*externalWeightDict.get(curEpisode.replaceAll("[^A-Za-z]", ""));
						
						if (volatileSet.containsKey(curEpisode)) {
							volatileSet.put(curEpisode,
									volatileSet.get(curEpisode) + utility);
						} else {
							volatileSet.put(curEpisode, utility);
						}
					}
				}// end while

				try {
					PreparedStatement insertstatement = con
							.prepareStatement(MESELO.INSERT);
					for (ArrayList<TrieNode> trie : MexBuffer) {
						Insert2DB indb = new Insert2DB(insertstatement, trie);
						es.submit(indb);
						trie = null;
					}
					// insertstatement.close();
				} catch (SQLException e) {
					// TODO Auto-generated catch block
					System.err.println(e.getMessage());
				}

				final List<ArrayList<TrieNode>> tmpS = MexBuffer;
				this.MexBuffer = new ArrayList<ArrayList<TrieNode>>();
				es.submit(new Runnable() {
					@Override
					public void run() {
						tmpS.clear();
					}
				});

			} catch (SQLException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			for (Entry<String, Integer> entry : volatileSet.entrySet()) {
				String episode = entry.getKey();
				if (frequentEpisodeSet.containsKey(episode)) {
					if (frequentEpisodeSet.get(episode) - entry.getValue() < this.paras
							.getMin_sup()) {
						candidateEpisodeSet.put(
								episode,
								frequentEpisodeSet.get(episode)
										- entry.getValue());
						frequentEpisodeSet.remove(episode);
					} else {
						frequentEpisodeSet.put(
								episode,
								frequentEpisodeSet.get(episode)
										- entry.getValue());
					}
				} else if (candidateEpisodeSet.containsKey(episode)) {
					if (candidateEpisodeSet.get(episode) == entry.getValue()) {
						candidateEpisodeSet.remove(episode);
					} else {
						candidateEpisodeSet.put(
								episode,
								candidateEpisodeSet.get(episode)
										- entry.getValue());
					}
				}
			}
			volatileSet.clear();
			volatileSet = null;
		}
		frequentEpisodeSet = UpdateHighUtilityEpisode(candidateEpisodeSet,
				frequentEpisodeSet/*, topKEpisodeSet*/,occList, this.paras);
		System.out.println(frequentEpisodeSet);
		return frequentEpisodeSet;
	}

	/**
	 * updated episode tries in \mathcal{M}_{in}.
	 * 
	 * @param trieList
	 * @param EkplusOne
	 * @param timestamp
	 * @return updated episode tries in \mathcal{M}_{in}.
	 */
	private HashSet<String> UpdateTries(List<ArrayList<TrieNode>> trieList,
			String[] EkplusOne, int timestamp) {
		// TODO Auto-generated method stub
		if (EkplusOne[0].length() > 0) {
			// Integer nodeInM = 0;
			// Integer realNodeInM = 0;
			for (int i = trieList.size() - 2; i >= 0; i--) {
				ArrayList<TrieNode> trie = trieList.get(i);
				ArrayList<TrieNode> tmp = new ArrayList<TrieNode>();
				for (TrieNode nodeOnTrie : trie) {
					String prefix = nodeOnTrie.getEpisode();
					for (String event : EkplusOne) {
						if (nodeOnTrie.isLastMO()) {
							int index = this.eventDic.get(event);
							if (nodeOnTrie.getChildren()[index] != true) {// add
																			// a
																			// child
								nodeOnTrie.getChildren()[index] = true;
								TrieNode newLeafNode = new TrieNode(event,
										timestamp, this.ESetSize);
								Object[] array = { prefix, event };
								String newEpisode = StringUtils.join(array,
										"->");
								newLeafNode.setEpisode(newEpisode);
								tmp.add(newLeafNode);
								this.Q.add(newEpisode);
							}
						}
					}
					if (this.Q.contains(prefix)) {
						nodeOnTrie.setLastMO(false);
					}
				}
				tmp.trimToSize();
				trie.addAll(tmp);
			}
		}
		return this.Q;
	}

	/**
	 * build an episode trie to \mathcal{M}_{in}.
	 * 
	 * @param EkplusOne
	 * @param timestamp
	 * @return new episode trie
	 */
	private ArrayList<TrieNode> BuildTrie(String[] EkplusOne, int timestamp) {
		// TODO Auto-generated method stub
		if (EkplusOne[0].length() == 0)
			return null;
		ArrayList<TrieNode> ret = new ArrayList<TrieNode>();
		for (String event : EkplusOne) {
			TrieNode node = new TrieNode(event, timestamp, this.ESetSize);
			node.setEpisode(event);
			ret.add(node);
			this.Q.add(event);
		}
		ret.trimToSize();
		return ret;
	}

	/**
	 * print performance information to a file
	 * 
	 * @param filename
	 */
	public void printStats(String filename) {
		// TODO Auto-generated method stub
		try {
			BufferedWriter writer = new BufferedWriter(new OutputStreamWriter(
					new FileOutputStream(filename, true), "UTF-8"));
			String outString = String.valueOf(this.averageTime);
			writer.write(outString + "\n");
			writer.close();
		} catch (UnsupportedEncodingException | FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
}
