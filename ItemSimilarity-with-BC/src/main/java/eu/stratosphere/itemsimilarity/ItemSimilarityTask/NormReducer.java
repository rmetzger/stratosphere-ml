package eu.stratosphere.itemsimilarity.ItemSimilarityTask;

import java.io.Serializable;
import java.util.Iterator;

import org.apache.mahout.math.RandomAccessSparseVector;
import org.apache.mahout.math.Vector;

import eu.stratosphere.api.java.record.functions.ReduceFunction;
import eu.stratosphere.itemsimilarity.common.VectorW;
import eu.stratosphere.types.DoubleValue;
import eu.stratosphere.types.IntValue;
import eu.stratosphere.types.LongValue;
import eu.stratosphere.types.Record;
import eu.stratosphere.util.Collector;


public class NormReducer extends ReduceFunction implements Serializable{
	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	Record currentRecord=null;
	LongValue index;
	VectorW nonZero = new VectorW();
	VectorW maxVal = new VectorW();
	Record outRecord = new Record();
	IntValue type = new IntValue();
	VectorW normW = new VectorW();
	
	Vector nonZeroEntries;
	Vector maxValues;
	@Override
	public void reduce(Iterator<Record> records, Collector<Record> out)
			throws Exception {
		Vector norms = new RandomAccessSparseVector(ItemSimilarityPlan.MAX_INT_VALUE);
		nonZeroEntries = new RandomAccessSparseVector(ItemSimilarityPlan.MAX_INT_VALUE);
		maxValues   = new RandomAccessSparseVector(ItemSimilarityPlan.MAX_INT_VALUE);
		
		while (records.hasNext()) {
			currentRecord = records.next();
			index= currentRecord.getField(0,
					LongValue.class);
			
			Vector parnormVec = currentRecord.getField(1, VectorW.class).get();
			norms.setQuick((int)index.getValue(), parnormVec.get((int)index.getValue()));
			
			
			int nonZeroNum = currentRecord.getField(3, IntValue.class).getValue();
			double maxValue = currentRecord.getField(4, DoubleValue.class).getValue();
			nonZeroEntries.setQuick((int)index.getValue(), nonZeroNum);
			maxValues.setQuick((int)index.getValue(), maxValue);
			
			
		}
		nonZero.set(nonZeroEntries);
		maxVal.set(maxValues);
		normW.set(norms);
		outRecord.setField(0, normW);	
		outRecord.setField(1, maxVal);
		outRecord.setField(2, nonZero);
		out.collect(outRecord);
		

	}
	
	

}
