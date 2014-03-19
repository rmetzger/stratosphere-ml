package eu.stratosphere.itemsimilarity.ItemSimilarityTask;

import java.io.Serializable;
import java.util.Collection;
import java.util.Iterator;

import org.apache.mahout.common.ClassUtils;
import org.apache.mahout.math.Vector;
import org.apache.mahout.math.hadoop.similarity.cooccurrence.measures.VectorSimilarityMeasure;

import eu.stratosphere.api.java.record.functions.ReduceFunction;
import eu.stratosphere.configuration.Configuration;
import eu.stratosphere.itemsimilarity.common.VectorW;
import eu.stratosphere.types.IntValue;
import eu.stratosphere.types.LongValue;
import eu.stratosphere.types.Record;
import eu.stratosphere.util.Collector;

public class SimilarityReducer extends ReduceFunction implements Serializable {

	/**
	 * 
	 */

	public SimilarityReducer(String sim_measure, double threshold) {
		this.similarity_measure = sim_measure;
		this.threshold = threshold;
	}

	private static final long serialVersionUID = 1L;
	double threshold = Double.MIN_VALUE;

	private String similarity_measure;
	private VectorSimilarityMeasure similarity;

	VectorW similarityV = new VectorW();
	Record output = new Record();
	Vector norms;
	int noOfusers;
	double similarityValue;

	// LongValue index = new LongValue();
	VectorW dotsV = new VectorW();
	LongValue itemId = new LongValue();

	public void open(Configuration parameters) throws Exception {
		Collection<Record> normsBC = this.getRuntimeContext()
				.getBroadcastVariable("myNormBC");

		for (Record r : normsBC) {
			noOfusers = r.getField(0, IntValue.class).getValue();

			norms = r.getField(1, VectorW.class).get();
		}
	}

	@Override
	public void reduce(Iterator<Record> records, Collector<Record> out)
			throws Exception {

		Record currentRecord = records.next();
		Vector dots = currentRecord.getField(1, VectorW.class).get();
		itemId.setValue(currentRecord.getField(0, LongValue.class).getValue());
		while (records.hasNext()) {
			currentRecord = records.next();
			Vector toAdd = currentRecord.getField(1, VectorW.class).get();
			
			 for (Vector.Element nonZeroElement : toAdd.nonZeroes()) {
		          dots.setQuick(nonZeroElement.index(), dots.getQuick(nonZeroElement.index()) + nonZeroElement.get());
		        }
		}

		Vector similarities = dots.like();

		similarity = ClassUtils.instantiateAs(
				ItemSimilarityPlan.getSimilarityClassName(similarity_measure),
				VectorSimilarityMeasure.class);

		double normA = norms.getQuick((int) itemId.getValue());
		Iterator<Vector.Element> dotsWith = dots.nonZeroes().iterator();

		while (dotsWith.hasNext()) {
			Vector.Element b = dotsWith.next();

			similarityValue = similarity.similarity(b.get(), normA,
					norms.getQuick(b.index()), noOfusers);
			if (similarityValue >= threshold) {
				similarities.set(b.index(), similarityValue);
			}
		}

		similarities.setQuick((int) itemId.getValue(), 0);

		output.setField(0, itemId);
		similarityV.set(similarities);
		output.setField(1, similarityV);
		out.collect(output);

	}

}
