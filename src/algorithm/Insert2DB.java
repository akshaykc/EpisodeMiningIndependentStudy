package algorithm;

import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.ArrayList;

import structures.TrieNode;
import utils.SerializeUtil;

/**
 * The class insert episode trie to database.
 * 
 * @author aox
 *
 */
public class Insert2DB extends Thread implements Runnable {
	private PreparedStatement insert;
	private ArrayList<TrieNode> trie4Cut;

	public Insert2DB(PreparedStatement insert, ArrayList<TrieNode> trie4Cut) {
		this.insert = insert;
		this.trie4Cut = trie4Cut;
	}

	public void run() {
		try {
			insertToDB(insert, trie4Cut);
			insert.close();
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	private void insertToDB(PreparedStatement insertstatement,
			ArrayList<TrieNode> trie4Cut) {
		// TODO Auto-generated method stub
		try {
			Integer time = trie4Cut.get(0).getTe();
			insertstatement.setInt(1, time);
			insertstatement.setBytes(2, SerializeUtil.serialize(trie4Cut));
			insertstatement.execute();
			// this.con.commit();
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();

		}
	}

}
