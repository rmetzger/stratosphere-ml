package eu.stratosphere.itemsimilarity.ItemSimilarityTask;

import java.io.Serializable;
import java.util.Arrays;
import java.util.Collection;
import java.util.Comparator;

import org.apache.mahout.common.ClassUtils;
import org.apache.mahout.math.RandomAccessSparseVector;
import org.apache.mahout.math.Vector;
import org.apache.mahout.math.hadoop.similarity.cooccurrence.measures.VectorSimilarityMeasure;

import com.google.common.primitives.Ints;

import eu.stratosphere.api.java.record.functions.MapFunction;
import eu.stratosphere.configuration.Configuration;
import eu.stratosphere.itemsimilarity.common.VectorW;
import eu.stratosphere.types.LongValue;
import eu.stratosphere.types.Record;
import eu.stratosphere.util.Collector;


public class CooccurrencesMapper extends MapFunction implements Serializable{
	Vector numNonZeroEntries;
	Vector maxValues;
	
	public void open(Configuration parameters) throws Exception {
		Collection<Record> normsBC = this.getRuntimeContext()
				.getBroadcastVariable("nonzeroMaxBC");

		for (Record r : normsBC) {
			numNonZeroEntries = r.getField(2, VectorW.class).get();

			maxValues = r.getField(1, VectorW.class).get();
		}
	}

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	public static final double NO_THRESHOLD = Double.MIN_VALUE;
	private VectorSimilarityMeasure similarity;
	
	VectorW coocc = new VectorW();
	LongValue keyVar =  new LongValue();
	private static final Comparator<Vector.Element> BY_INDEX = new Comparator<Vector.Element>() {
		@Override
		public int compare(Vector.Element one, Vector.Element two) {
			return Ints.compare(one.index(), two.index());
		}
	};
	private String similarity_measure;
	private double threshold= Double.MIN_VALUE;
	public CooccurrencesMapper(String sim_measure, double threshold) {
		this.similarity_measure = sim_measure;
		this.threshold = threshold;
	}
	 Record outputRecord = new Record();
	 private VectorW occurrenceVector;
	 @Override
		public void map(Record record, Collector<Record> out) throws Exception {
			similarity = ClassUtils.instantiateAs(ItemSimilarityPlan.getSimilarityClassName(similarity_measure), VectorSimilarityMeasure.class);
			occurrenceVector = record.getField(1, VectorW.class);
			
			  Vector.Element[] occurrences = VectorW.toArray(occurrenceVector);
		      Arrays.sort(occurrences, BY_INDEX);
		      Vector dots =null;
		      Vector.Element occurrenceA = null;
		      for (int n = 0; n < occurrences.length; n++) {
		         occurrenceA = occurrences[n];
		        dots = new RandomAccessSparseVector(ItemSimilarityPlan.MAX_INT_VALUE);
		        for (int m = n; m < occurrences.length; m++) {
		          Vector.Element occurrenceB = occurrences[m];
		          if (threshold == NO_THRESHOLD || consider(occurrenceA, occurrenceB)) {
		            dots.setQuick(occurrenceB.index(), similarity.aggregate(occurrenceA.get(), occurrenceB.get()));
		          } else {
		          }
		        }
		       // System.out.println("co occurence--"+occurrenceA.index()+" "+dots);	
		        coocc.set(dots);
				keyVar.setValue(occurrenceA.index());
				outputRecord.setField(0, keyVar);
				outputRecord.setField(1, coocc);
				out.collect(outputRecord);
		      }
			
			
		}
	
	 
	 private boolean consider(Vector.Element occurrenceA, Vector.Element occurrenceB) {
	      int numNonZeroEntriesA = (int)numNonZeroEntries.get(occurrenceA.index());
	      int numNonZeroEntriesB = (int)numNonZeroEntries.get(occurrenceB.index());

	      double maxValueA = maxValues.get(occurrenceA.index());
	      double maxValueB = maxValues.get(occurrenceB.index());

	      return similarity.consider(numNonZeroEntriesA, numNonZeroEntriesB, maxValueA, maxValueB, threshold);
	    }
}
