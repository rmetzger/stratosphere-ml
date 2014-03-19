package eu.stratosphere.itemsimilarity.common;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.EOFException;
import java.io.IOException;
import java.util.Iterator;

import org.apache.mahout.math.RandomAccessSparseVector;
import org.apache.mahout.math.Vector;

import eu.stratosphere.types.Key;
import eu.stratosphere.types.Value;


public class VectorW implements Value, Key {
	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;

	private Vector vector;
	
	public VectorW(){
		
	}
	public VectorW(Vector vector){
		this.vector =  vector;
	}
	
	public Vector get() {
		return vector;
	}

	public void set(Vector vector) {
		this.vector = vector;
	}
	

	@Override
	public void write(DataOutput out) throws IOException {
		
		 Iterator<Vector.Element> iter = ((RandomAccessSparseVector) vector).iterateNonZero();
		 out.writeInt(vector.size());
		 out.writeInt(vector.getNumNondefaultElements());
		 while (iter.hasNext()) {
	          Vector.Element element = iter.next();
	          int thisIndex = element.index();
	          out.writeInt(thisIndex);
	          out.writeDouble(element.get());
		 }
		
	}
	@Override
	public String toString(){
		
		 StringBuilder sb = new StringBuilder();
	        Iterator<Vector.Element> iter = ((RandomAccessSparseVector) vector).iterateNonZero();
	        while (iter.hasNext()) {
		          Vector.Element element = iter.next();
		          int thisIndex = element.index();
		          sb.append(String.format("%s:%f \n", thisIndex,element.get()));
			 }

	        return sb.toString();
	}
	@Override
	public void read(DataInput in) throws IOException {
		
		try{
		int size = in.readInt();
		int actualElements = in.readInt();
		vector = new RandomAccessSparseVector(size,actualElements);
		for(int i=0;i<size;i++){
			int val=in.readInt();
			vector.set(val, in.readDouble());
			}
			
		}catch(EOFException e){
			e.getMessage();
			
		}
		
		
		
	}
	
	
	public static Vector.Element[] toArray(VectorW vectorW) {
		    Vector.Element[] elements = new Vector.Element[vectorW.get().getNumNondefaultElements()];
		    int k = 0;
		    Iterator<Vector.Element> nonZeroElements = vectorW.get().nonZeroes().iterator();
		    while (nonZeroElements.hasNext()) {
		      Vector.Element nonZeroElement = nonZeroElements.next();
		      elements[k++] = new TemporaryElement(nonZeroElement.index(), nonZeroElement.get());
		    }
		    return elements;
		  }
	
	@Override
	public int compareTo(Key arg0) {
		if(this.hashCode()==arg0.hashCode())
			return 1;
		else return 0;
	}
	 static class TemporaryElement implements Vector.Element {

		    private final int index;
		    private double value;

		    TemporaryElement(int index, double value) {
		      this.index = index;
		      this.value = value;
		    }

		    TemporaryElement(Vector.Element toClone) {
		      this(toClone.index(), toClone.get());
		    }

		    @Override
		    public double get() {
		      return value;
		    }

		    @Override
		    public int index() {
		      return index;
		    }

		    @Override
		    public void set(double value) {
		      this.value = value;
		    }
		  }

}

