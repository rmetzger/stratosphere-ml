package eu.stratosphere.itemsimilarity.ItemSimilarityTask;

import java.io.Serializable;

import org.apache.mahout.cf.taste.hadoop.similarity.item.TopSimilarItemsQueue;
import org.apache.mahout.cf.taste.similarity.precompute.SimilarItem;
import org.apache.mahout.math.RandomAccessSparseVector;
import org.apache.mahout.math.Vector;

import eu.stratosphere.api.java.record.functions.MapFunction;
import eu.stratosphere.itemsimilarity.common.VectorW;
import eu.stratosphere.types.LongValue;
import eu.stratosphere.types.Record;
import eu.stratosphere.util.Collector;

public class MostSimilarItemPairsMapper extends MapFunction implements Serializable {

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	int maxSimilarItemsPerItem;
	public MostSimilarItemPairsMapper(int maxSimilarItemsPerItem){
		this.maxSimilarItemsPerItem = maxSimilarItemsPerItem;
	}
	
	Record outputR = new Record();
	VectorW topKSimilar = new VectorW();
	
	
	@Override
	public void map(Record record, Collector<Record> out) throws Exception {

		Vector mostsimVec = new RandomAccessSparseVector(
				ItemSimilarityPlan.MAX_INT_VALUE);
		
		LongValue itemIDIndex = record.getField(0, LongValue.class);
		Vector similarityVector = record.getField(1, VectorW.class).get();
	      TopSimilarItemsQueue topKMostSimilarItems = new TopSimilarItemsQueue(maxSimilarItemsPerItem);

	      for (Vector.Element element : similarityVector.nonZeroes()) {
	        SimilarItem top = topKMostSimilarItems.top();
	        double candidateSimilarity = element.get();
	        if (candidateSimilarity > top.getSimilarity()) {
	          top.set(element.index(), candidateSimilarity);
	          topKMostSimilarItems.updateTop();
	        }
	      }
	      
	      for (SimilarItem similarItem : topKMostSimilarItems.getTopItems()) {
	    	  mostsimVec.setQuick((int)similarItem.getItemID(), similarItem.getSimilarity());
	      }
	      
	      topKSimilar.set(mostsimVec);
	      
	      outputR.setField(0,itemIDIndex);
	      outputR.setField(1, topKSimilar);
	      out.collect(outputR);
	}

}
