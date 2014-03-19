package eu.stratosphere.itemsimilarity.ItemSimilarityTask;

import java.io.Serializable;
import java.util.Iterator;

import org.apache.mahout.cf.taste.hadoop.TasteHadoopUtils;
import org.apache.mahout.math.RandomAccessSparseVector;
import org.apache.mahout.math.Vector;

import eu.stratosphere.api.java.record.functions.ReduceFunction;
import eu.stratosphere.itemsimilarity.common.ItemPrefVector;
import eu.stratosphere.itemsimilarity.common.VectorW;
import eu.stratosphere.types.LongValue;
import eu.stratosphere.types.Record;
import eu.stratosphere.util.Collector;

public class ItemVectorReducer extends ReduceFunction implements
		Serializable {

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	long userId;
	LongValue pactLong;
	Record outputRecord = new Record();
	Record currentRecord;
	LongValue lValue = new LongValue();
	VectorW userVectorW = new VectorW();
	ItemPrefVector itemPrefV; 
	Vector accumulator;
	
	@Override
	public void reduce(Iterator<Record> records, Collector<Record> out)
			throws Exception {
		int index;
		double value;
		boolean flag = false;
		accumulator = new RandomAccessSparseVector(
				ItemSimilarityPlan.MAX_INT_VALUE);
		while (records.hasNext()) {
			currentRecord = records.next();
			if (!flag) {
				userId = currentRecord.getField(0, LongValue.class).getValue();
				flag = true;
			}

			
			itemPrefV = currentRecord.getField(1, ItemPrefVector.class);
			index = TasteHadoopUtils.idToIndex(itemPrefV.get());
			value = itemPrefV instanceof ItemPrefVector ? ((ItemPrefVector) itemPrefV)
					.getPrefVal() : 1.0d;
			accumulator.setQuick(index, value);
		}
	
		lValue.setValue(userId);
		userVectorW.set(accumulator);
		outputRecord.setField(0, lValue);
		outputRecord.setField(1,userVectorW);
		out.collect(outputRecord);
	}


}
