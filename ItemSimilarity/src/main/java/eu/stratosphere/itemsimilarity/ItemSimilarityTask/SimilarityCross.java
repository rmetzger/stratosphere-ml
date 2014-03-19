package eu.stratosphere.itemsimilarity.ItemSimilarityTask;

import java.io.Serializable;

import org.apache.mahout.common.ClassUtils;
import org.apache.mahout.math.Vector;
import org.apache.mahout.math.hadoop.similarity.cooccurrence.measures.VectorSimilarityMeasure;

import eu.stratosphere.api.java.record.functions.CrossFunction;
import eu.stratosphere.itemsimilarity.common.VectorW;
import eu.stratosphere.types.IntValue;
import eu.stratosphere.types.LongValue;
import eu.stratosphere.types.Record;
import eu.stratosphere.util.Collector;

public class SimilarityCross extends CrossFunction implements Serializable {
	private static final long serialVersionUID = 1L;
	double threshold = Double.MIN_VALUE;

	private String similarity_measure;
	private VectorSimilarityMeasure similarity;

	public SimilarityCross(String sim_measure, double threshold) {
		this.similarity_measure = sim_measure;
		this.threshold = threshold;
	}

	VectorW similarityV = new VectorW();
	Record output = new Record();
	Vector norms;
	int noOfusers;
	double similarityValue;

	@Override
	public void cross(Record record1, Record record2, Collector<Record> out)
			throws Exception {

		LongValue row = record1.getField(0, LongValue.class);
		Vector dots = record1.getField(1, VectorW.class).get();

		Vector similarities = dots.like();

		noOfusers = record2.getField(0, IntValue.class).getValue();
		norms = record2.getField(1, VectorW.class).get();
		similarity = ClassUtils.instantiateAs(
				ItemSimilarityPlan.getSimilarityClassName(similarity_measure),
				VectorSimilarityMeasure.class);

		double normA = norms.getQuick((int) row.getValue());

		for (Vector.Element b : dots.nonZeroes()) {
			double similarityValue = similarity.similarity(b.get(), normA,
					norms.getQuick(b.index()), noOfusers);
			// System.out.println("similarityValue-->"+similarityValue);
			if (similarityValue >= threshold) {
				similarities.set(b.index(), similarityValue);
			}
		}
		/*
		 * if (excludeSelfSimilarity) { similarities.setQuick(row.get(), 0); }
		 */
		similarities.setQuick((int) row.getValue(), 0);
		// System.out.println(row.getValue()+ "---inside Cross--"+similarities);

		output.setField(0, row);
		similarityV.set(similarities);
		output.setField(1, similarityV);
		out.collect(output);

	}

}
