package eu.stratosphere.itemsimilarity.ItemSimilarityTask;

import java.io.Serializable;
import java.util.Iterator;
import java.util.Random;

import org.apache.mahout.cf.taste.hadoop.TasteHadoopUtils;
import org.apache.mahout.common.ClassUtils;
import org.apache.mahout.common.RandomUtils;
import org.apache.mahout.math.RandomAccessSparseVector;
import org.apache.mahout.math.Vector;
import org.apache.mahout.math.hadoop.similarity.cooccurrence.measures.VectorSimilarityMeasure;

import eu.stratosphere.api.java.record.functions.ReduceFunction;
import eu.stratosphere.itemsimilarity.common.ItemPrefVector;
import eu.stratosphere.itemsimilarity.common.VarLongW;
import eu.stratosphere.itemsimilarity.common.VectorW;
import eu.stratosphere.types.DoubleValue;
import eu.stratosphere.types.IntValue;
import eu.stratosphere.types.LongValue;
import eu.stratosphere.types.Record;
import eu.stratosphere.util.Collector;

public class UserVectorReducer extends ReduceFunction implements Serializable {
	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	private int maxPrefPerUser;
	private String similarity_measure;
	private VectorSimilarityMeasure similarity;
	private long seed;
	private double threshold;
	private double NO_THRESHOLD = Double.MIN_VALUE;
	public static final long NO_FIXED_RANDOM_SEED = Long.MIN_VALUE;

	public UserVectorReducer(String sim_measure, int maxPrefPerUser,
			long randomSeed, double threshold) {
		this.similarity_measure = sim_measure;
		this.maxPrefPerUser = maxPrefPerUser;
		this.seed = randomSeed;
		this.threshold = threshold;
	}

	private int nonZeroEntries;
	private double maxValues;
	int numNonZeroEntries;
	double maxValue = Double.MIN_VALUE;

	Record outputRecord = new Record();
	LongValue pactLong;
	VarLongW itemPVector;
	Record currentRecord;
	VectorW normVec = new VectorW();
	VectorW sampledRow = new VectorW();
	Vector rowVector;

	IntValue nonZeroEntriesW = new IntValue();
	DoubleValue maxValuesW = new DoubleValue();

	@Override
	public void reduce(Iterator<Record> records, Collector<Record> collector)
			throws Exception {

		Vector norms = new RandomAccessSparseVector(
				ItemSimilarityPlan.MAX_INT_VALUE);
		Vector userVector = new RandomAccessSparseVector(
				ItemSimilarityPlan.MAX_INT_VALUE);
		similarity = ClassUtils.instantiateAs(
				ItemSimilarityPlan.getSimilarityClassName(similarity_measure),
				VectorSimilarityMeasure.class);
		boolean isItemIdSet = false;
		while (records.hasNext()) {
			currentRecord = records.next();
			if (!isItemIdSet) {
				pactLong = currentRecord.getField(0, LongValue.class);
				isItemIdSet = true;
			}
			itemPVector = currentRecord.getField(1, ItemPrefVector.class);
			int index = TasteHadoopUtils.idToIndex(itemPVector.get());
			double value = itemPVector instanceof ItemPrefVector ? ((ItemPrefVector) itemPVector)
					.getPrefVal() : 1.0d;
			userVector.setQuick(index, value);
		}

		// CountObservationsMapper
		Vector columnCounts = new RandomAccessSparseVector(
				ItemSimilarityPlan.MAX_INT_VALUE);

		for (Vector.Element elem : userVector.nonZeroes()) {
			int index = TasteHadoopUtils.idToIndex(elem.index());
			// System.out.println("-->"+elem.index());
			columnCounts.setQuick(index,
					columnCounts.getQuick(elem.index()) + 1);
		}
		// sampling down
		Vector sampledRowVector = sampleDown(userVector, columnCounts);

		/**
		 * pre-processing & computing norms
		 */
		rowVector = similarity.normalize(sampledRowVector);
		/**
		 * computing nonZero entries and maxValues
		 */
		for (Vector.Element element : rowVector.nonZeroes()) {
			numNonZeroEntries++;
			if (maxValue < element.get()) {
				maxValue = element.get();
			}
		}

		if (threshold != NO_THRESHOLD) {
			nonZeroEntries = numNonZeroEntries;
			maxValues = maxValue;
		}
		// System.out.println(pactLong.getValue()+" ---->"+rowVector);
		norms.setQuick((int) pactLong.getValue(), similarity.norm(rowVector));
		normVec.set(norms);
		outputRecord.setField(0, pactLong);
		outputRecord.setField(1, normVec);
		outputRecord.setField(2, sampledRow);
		nonZeroEntriesW.setValue(nonZeroEntries);
		maxValuesW.setValue(maxValues);
		// System.out.println("-->"+sampledRowVector);
		outputRecord.setField(3, nonZeroEntriesW);
		outputRecord.setField(4, maxValuesW);
		sampledRow.set(sampledRowVector);
		collector.collect(outputRecord);

	}

	private Vector sampleDown(Vector rowVector, Vector columnCounts) {

		// long randomSeed = this.seed;
		Random random = RandomUtils.getRandom();
		if (this.seed == NO_FIXED_RANDOM_SEED) {
			random = RandomUtils.getRandom();
		} else {
			random = RandomUtils.getRandom(this.seed);
		}

		int observationsPerRow = rowVector.getNumNondefaultElements();
		double rowSampleRate = (double) Math.min(this.maxPrefPerUser,
				observationsPerRow) / (double) observationsPerRow;

		Vector downsampledRow = rowVector.like();
		// long usedObservations = 0;
		// long neglectedObservations = 0;
		for (Vector.Element elem : rowVector.nonZeroes()) {
			int index = TasteHadoopUtils.idToIndex(elem.index());
			// System.out.println("sample down-->"+index);
			int columnCount = (int) columnCounts.get(index);
			double columnSampleRate = (double) Math.min(this.maxPrefPerUser,
					columnCount) / (double) columnCount;
			if (random.nextDouble() <= Math
					.min(rowSampleRate, columnSampleRate)) {
				downsampledRow.setQuick(elem.index(), elem.get());
				// usedObservations++;
			} /*
			 * else { neglectedObservations++; }
			 */

		}
		return downsampledRow;
	}

}
