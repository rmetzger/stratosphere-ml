package eu.stratosphere.library.clustering.DistributedOnePassKMeans;

import java.security.SecureRandom;
import java.util.ArrayList;
import java.util.Iterator;

import org.apache.mahout.math.Centroid;

import org.apache.mahout.math.Vector;
import org.apache.mahout.math.neighborhood.UpdatableSearcher;
import org.apache.mahout.math.random.WeightedThing;

/**
 * 
 * @author JOSE LUIS LOPEZ PINO (JLLOPEZPINO@GMAIL.COM)
 *
 */

public class SKMeans {

	// Used to generate random numbers
	private  SecureRandom random = new java.security.SecureRandom();
	public  UpdatableSearcher centroids = null;

	private  double maxClusterSize = 0;
	private  double facilityCostIncrement = 0;
	private  int kappa = 0;

	public  SKMeans(UpdatableSearcher searcher,
			int paramKappa,
			double paramMaxClusterSize,
			double paramFacilityCostIncrement){
		kappa = paramKappa;
		maxClusterSize = paramMaxClusterSize;
		facilityCostIncrement = paramFacilityCostIncrement;
		centroids = searcher;
		System.out.println("i am here");
	}


	public  void processPoint(Centroid p) {

		if( centroids.size() == 0 ){
			centroids.add(p);
		}
		else {

			centroids.searchFirst(p, false);

			WeightedThing<Vector > cn = centroids.searchFirst(p, false);


			//var cn = closestNeighbour(p, centers)
			//var probability = cn._2 / maxClusterSize

			double probability = p.getWeight() * cn.getWeight() / maxClusterSize;

			//	System.out.println("Point [" + p + "]: closest to [" + cn + " Distance: " + cn.getWeight() + " Probability: " + probability);

			if(random.nextDouble() < probability){
				centroids.add( p );

				//			System.out.println(" Append: " + p + " (" + cn.toString() + ")");
			} else {
				Centroid cnCentroid = (Centroid) cn.getValue();

				centroids.remove(cnCentroid, 1.0E-6);

				cnCentroid.update(p);

				centroids.add(cnCentroid);
			}

			dismissPoints();
		}

	}


	private  void dismissPoints(){


		ArrayList<Vector> oldCenters = new ArrayList<Vector>();

		Iterator< Vector> ite = centroids.iterator();

		while( ite.hasNext() ){
			Vector next = ite.next();
			oldCenters.add(next);
		}




		// Meanwhile the number of centers is bigger than the maximum
		while( oldCenters.size() > kappa ){
			maxClusterSize += facilityCostIncrement;

			//	System.out.println("We are trying to dismiss points. Using max cluster size: " + maxClusterSize);

			// Empty the array for the new centers
			// TO-DO: currently we always keep the last point
			centroids.clear(); // new selectedCenters is centroids
			centroids.add( oldCenters.get(oldCenters.size() - 1) );


			//var centersWeight = weight(centers, maxClusterSize)
			int j = 0;

			// Select the most interesting centers (non-deterministic)
			while( j < oldCenters.size() - 1 ){

				Centroid p = (Centroid) oldCenters.get(j);

				WeightedThing<Vector> cn = centroids.searchFirst(p,  false);

				double probability = p.getWeight() * cn.getWeight() / maxClusterSize;

				//		System.out.println("Point [" + p + "]: closest to [" + cn + " Distance: " + cn.getWeight() + " Probability: " + probability);

				if(random.nextDouble() < probability){
					centroids.add(p);
					//			System.out.println(" Append: " + p + " (" + cn.toString() + ")");

				} else {
					Centroid cnCentroid = (Centroid) cn.getValue();

					centroids.remove(cnCentroid, 1.0E-6);

					cnCentroid.update(p);

					centroids.add(cnCentroid);
				}

				j++;

			}

			oldCenters.clear();

			ite = centroids.iterator();
			while( ite.hasNext() ){
				Vector next = ite.next();
				oldCenters.add(next);
			}
		}
	}


}
