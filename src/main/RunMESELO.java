package main;

import java.io.File;

import algorithm.MESELO;

/**
 * main class for MESELO algorithm.
 * 
 * @author aox
 *
 */
public class RunMESELO {

	public static void main(String[] args) {
		// TODO Auto-generated method stub
		if (args.length == 0) {
			// min_sup = 2; delta = 3; DELTA=6; when time slot >=6, every 2 time
			// slot output frequent episodes.
			// test period from time slot 1 to time slot 8.
			MESELO meselo = new MESELO(
					"./data/alginput/synthetic/toy2sequence.dat",
					"./data/freqepisodes/synthetic/toy2sequence.csv","./data/alginput/synthetic/toy2sequenceExternalWeights.dat", 20, 3, 6,
					2, 1, 14);
			// MESELO meselo = new MESELO(
			// "./data/alginput/real/chinaStock/stock-1.data",
			// "./data/freqepisodes/real/chinaStock/stock-1.csv", 10, 3, 2470,
			// 1, 1, 2478);
			meselo.algCore();
		} else {
			String filename = args[0];
			String output = args[1];
			System.out
					.println("*************************************\nRUN MESELO ALG.\n************************************");
			MESELO meselo = new MESELO(filename, output, "./data/alginput/synthetic/toy2sequenceExternalWeights.dat",
					Integer.parseInt(args[2]), Integer.parseInt(args[3]),
					Integer.parseInt(args[4]), Integer.parseInt(args[5]),
					Integer.parseInt(args[6]), Integer.parseInt(args[7]));
			meselo.algCore();
		}
	}
}
