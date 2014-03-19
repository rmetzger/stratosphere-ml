package eu.stratosphere.library.clustering.DistributedOnePassKMeans;


import java.util.ArrayList;
import java.util.Iterator;

import org.apache.mahout.common.distance.ChebyshevDistanceMeasure;
import org.apache.mahout.common.distance.CosineDistanceMeasure;
import org.apache.mahout.common.distance.DistanceMeasure;
import org.apache.mahout.common.distance.EuclideanDistanceMeasure;
import org.apache.mahout.common.distance.MahalanobisDistanceMeasure;
import org.apache.mahout.common.distance.ManhattanDistanceMeasure;
import org.apache.mahout.common.distance.MinkowskiDistanceMeasure;
import org.apache.mahout.common.distance.SquaredEuclideanDistanceMeasure;
import org.apache.mahout.common.distance.TanimotoDistanceMeasure;
import org.apache.mahout.common.distance.WeightedEuclideanDistanceMeasure;
import org.apache.mahout.common.distance.WeightedManhattanDistanceMeasure;
import org.apache.mahout.math.Centroid;
import org.apache.mahout.math.DenseVector;
import org.apache.mahout.math.Vector;
import org.apache.mahout.math.neighborhood.BruteSearch;
import org.apache.mahout.math.neighborhood.FastProjectionSearch;
import org.apache.mahout.math.neighborhood.LocalitySensitiveHashSearch;
import org.apache.mahout.math.neighborhood.ProjectionSearch;
import org.apache.mahout.math.neighborhood.UpdatableSearcher;

import eu.stratosphere.api.java.record.functions.FunctionAnnotation.ConstantFields;
import eu.stratosphere.api.java.record.functions.ReduceFunction;
import eu.stratosphere.configuration.Configuration;
import eu.stratosphere.types.IntValue;
import eu.stratosphere.types.StringValue;
import eu.stratosphere.types.Record;
import eu.stratosphere.util.Collector;

/**
 * 
 * @author PRATEEK GAUR (PGAUR19@GMAIL.COM)
 *
 */

@ConstantFields(0)
public class OnePassReducer extends ReduceFunction {

	// ----------------------------------------------------------------------------------------------------------------
	public static final String finalclusters = "parameter.FINAL_CLUSTERS";
	public static final String maxIterations="parameter.MAX_ITERATIONS";
	public static final String trimFraction="parameter.TRIM_FRACTION";
	public static final String kMeansPlusPlusInit="parameter.KMEANS_PLUS_PLUS_INIT";
	public static final String correctWeights="parameter.CORRECT_WEIGHTS";
	public static final String testProbability="parameter.TEST_PROBABILITY";
	public static final String numRuns="parameter.NUM_RUNS";
	public static final String searcherTechnique ="parameter.SEARCHER_TECHNIQUE";
	public static final String distanceTechnique ="parameter.DISTANCE_TECHNIQUE";
	public static final String numProjections = "parameter.NUM_PROJECTIONS";
	public static final String searchSize = "parameter.SEARCH_SIZE";

	int efinalclusters;
	int emaxIterations;
	float etrimFraction;
	boolean ekMeansPlusPlusInit;
	boolean ecorrectWeights;
	float etestProbability;
	int enumRuns;


	DistanceMeasure distanceMeasure = null;
	UpdatableSearcher searcher = null;

	@Override
	public void open(Configuration parameters) {

		efinalclusters = Integer.parseInt(parameters.getString(finalclusters, "3"));
		emaxIterations = Integer.parseInt(parameters.getString(maxIterations, "20"));
		etrimFraction = Float.parseFloat(parameters.getString( trimFraction, "0.9f"));
		ekMeansPlusPlusInit = Boolean.parseBoolean(parameters.getString(kMeansPlusPlusInit, "false"));
		ecorrectWeights= Boolean.parseBoolean(parameters.getString(correctWeights, "false"));
		etestProbability = Float.parseFloat(parameters.getString(testProbability, "0.0f"));
		enumRuns = Integer.parseInt(parameters.getString(numRuns, "10"));
		int esearcherTechnique =Integer.parseInt(parameters.getString(searcherTechnique, "0"));
		int edistanceTechnique =Integer.parseInt(parameters.getString(distanceTechnique, "0"));
		int enumProjections = Integer.parseInt(parameters.getString(numProjections, "0"));
		int esearchSize = Integer.parseInt(parameters.getString(searchSize , "0"));

		switch(edistanceTechnique){
		case 0:
			distanceMeasure =  new SquaredEuclideanDistanceMeasure();
			break;
		case 1:
			distanceMeasure =  new CosineDistanceMeasure();
			break;
		case 2:
			distanceMeasure =  new ChebyshevDistanceMeasure();
			break;
		case 3:
			distanceMeasure =  new EuclideanDistanceMeasure();
			break;
		case 4:
			distanceMeasure =  new ManhattanDistanceMeasure();
			break;
		case 5:
			distanceMeasure =  new MahalanobisDistanceMeasure();
			break;
		case 6:
			distanceMeasure =  new MinkowskiDistanceMeasure();
			break;
		case 7:
			distanceMeasure =  new TanimotoDistanceMeasure();
			break;
		case 8:
			distanceMeasure =  new WeightedEuclideanDistanceMeasure();
			break;
		case 9:
			distanceMeasure =  new WeightedManhattanDistanceMeasure();
			break;
		}


		switch(esearcherTechnique){
		case 0:
			searcher = new BruteSearch(distanceMeasure);
			break;
		case 1:
			searcher = new ProjectionSearch(distanceMeasure, enumProjections, esearchSize);
			break;
		case 2:
			searcher = new FastProjectionSearch(distanceMeasure, enumProjections, esearchSize);
			break;
		case 3:
			searcher = new LocalitySensitiveHashSearch(distanceMeasure, esearchSize);
			break;
		}
	}



	@Override
	public void reduce(Iterator<Record> records, Collector<Record> collector) throws Exception {
		// Implement your solution here


		ArrayList<Centroid> stream = new ArrayList<Centroid>();

		int count=0;
		Record pr=null;
		double[] z;
		while(records.hasNext()){
			pr=records.next();
			StringValue pcInt=pr.getField(1, StringValue.class);


			String[] x=   pcInt.toString().split("#");

			z=new double[x.length];

			for(int m=0;m<x.length;m++)
			{
				z[m]=Double.parseDouble(x[m]);
			}

			DenseVector randomDenseVector = new DenseVector(z);


			Centroid newCentroid = new Centroid(count, randomDenseVector);

			stream.add(newCentroid);
			count++;
		}

		System.out.println("Number of centers: " + searcher.getDistanceMeasure());

		BallKMeans clusterer = new BallKMeans(searcher,
				efinalclusters, emaxIterations, etrimFraction,ekMeansPlusPlusInit,ecorrectWeights, etestProbability , enumRuns);
	
		UpdatableSearcher centr = clusterer.cluster(stream);


		Iterator< Vector> ite = centr.iterator();
		StringBuffer outball;
		int counter=0;

		while( ite.hasNext() ){
			Vector next = ite.next();
			outball=new StringBuffer();

			for(int j=0;j<next.size()-1;j++)
			{
				outball.append(String.valueOf(next.getQuick(j))+",");
			}
			outball.append(next.getQuick(next.size()-1));


			Record outPactRec=new Record();
			StringValue outString=new StringValue(outball.toString());
			IntValue one=new IntValue(counter+1);
			counter++;
			outPactRec.setField(0, one);
			outPactRec.setField(1, outString);
			collector.collect(outPactRec);
		}
	}
}
