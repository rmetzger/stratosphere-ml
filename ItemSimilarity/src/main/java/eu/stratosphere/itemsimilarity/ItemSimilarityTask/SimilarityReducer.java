package eu.stratosphere.itemsimilarity.ItemSimilarityTask;

import java.io.Serializable;
import java.util.Iterator;

import org.apache.mahout.math.Vector;

import eu.stratosphere.api.java.record.functions.ReduceFunction;
import eu.stratosphere.itemsimilarity.common.VectorW;
import eu.stratosphere.types.LongValue;
import eu.stratosphere.types.Record;
import eu.stratosphere.util.Collector;

public class SimilarityReducer extends ReduceFunction implements Serializable {

	/**
	 * 
	 */
	Record pactRec = new Record();
	private static final long serialVersionUID = 1L;

	public SimilarityReducer() {
	}

	VectorW dotsV = new VectorW();
	LongValue itemId = new LongValue();

	@Override
	public void reduce(Iterator<Record> records, Collector<Record> out)
			throws Exception {
		try {
			Record currentRecord = records.next();
			Vector dots = currentRecord.getField(1, VectorW.class).get();
			itemId.setValue(currentRecord.getField(0, LongValue.class)
					.getValue());
			while (records.hasNext()) {
				currentRecord = records.next();
				Vector toAdd = currentRecord.getField(1, VectorW.class).get();
				
				 for (Vector.Element nonZeroElement : toAdd.nonZeroes()) {
			          dots.setQuick(nonZeroElement.index(), dots.getQuick(nonZeroElement.index()) + nonZeroElement.get());
			        }
				
			}
			
			//System.out.println("after agg-->"+dots);
			pactRec.setField(0, itemId);
			dotsV.set(dots);
			pactRec.setField(1, dotsV);
			out.collect(pactRec);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

}
