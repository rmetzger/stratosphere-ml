package de.tuberlin.dima.ml.pact.types;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Iterator;

import org.apache.mahout.math.DenseVector;
import org.apache.mahout.math.NamedVector;
import org.apache.mahout.math.RandomAccessSparseVector;
import org.apache.mahout.math.SequentialAccessSparseVector;
import org.apache.mahout.math.Varint;
import org.apache.mahout.math.Vector;
import org.apache.mahout.math.VectorWritable;
import org.apache.mahout.math.Vector.Element;

import com.google.common.base.Preconditions;

import eu.stratosphere.types.Value;

/**
 * PactVector encapsulates an mahout vector.
 * The vector can have different internal implementations
 * (dense, sparse random access, sparse sequential access)
 * 
 * Copy of the VectorWritable from mahout 0.8 snapshot (2013-07-18)
 * 
 * @author Andr�� Hacker
 */
public class PactVector implements Value {

  /**
	 * 
	 */
	private static final long serialVersionUID = 1L;
public static final int FLAG_DENSE = 0x01;
  public static final int FLAG_SEQUENTIAL = 0x02;
  public static final int FLAG_NAMED = 0x04;
  public static final int FLAG_LAX_PRECISION = 0x08;
  public static final int NUM_FLAGS = 4;

  private Vector vector;
  private boolean writesLaxPrecision;

  public PactVector() {}

  public PactVector(boolean writesLaxPrecision) {
    setWritesLaxPrecision(writesLaxPrecision);
  }

  public PactVector(Vector vector) {
    this.vector = vector;
  }

  public PactVector(Vector vector, boolean writesLaxPrecision) {
    this(vector);
    setWritesLaxPrecision(writesLaxPrecision);
  }

  /**
   * @return {@link Vector} that this is to write, or has
   *  just read
   */
  public Vector getValue() {
    return vector;
  }

  public void setValue(Vector vector) {
    this.vector = vector;
  }

  /**
   * @return true if this is allowed to encode {@link Vector}
   *  values using fewer bytes, possibly losing precision. In particular this means
   *  that floating point values will be encoded as floats, not doubles.
   */
  public boolean isWritesLaxPrecision() {
    return writesLaxPrecision;
  }

  public void setWritesLaxPrecision(boolean writesLaxPrecision) {
    this.writesLaxPrecision = writesLaxPrecision;
  }

  @Override
  public void write(DataOutput out) throws IOException {
    writeVector(out, this.vector, this.writesLaxPrecision);
  }

  @Override
  public void read(DataInput in) throws IOException {
    int flags = in.readByte();
    Preconditions.checkArgument(flags >> NUM_FLAGS == 0, "Unknown flags set: %d", Integer.toString(flags, 2));
    boolean dense = (flags & FLAG_DENSE) != 0;
    boolean sequential = (flags & FLAG_SEQUENTIAL) != 0;
    boolean named = (flags & FLAG_NAMED) != 0;
    boolean laxPrecision = (flags & FLAG_LAX_PRECISION) != 0;

    int size = Varint.readUnsignedVarInt(in);
    Vector v;
    if (dense) {
      double[] values = new double[size];
      for (int i = 0; i < size; i++) {
        values[i] = laxPrecision ? in.readFloat() : in.readDouble();
      }
      v = new DenseVector(values);
    } else {
      int numNonDefaultElements = Varint.readUnsignedVarInt(in);
      v = sequential
          ? new SequentialAccessSparseVector(size, numNonDefaultElements)
          : new RandomAccessSparseVector(size, numNonDefaultElements);
      if (sequential) {
        int lastIndex = 0;
        for (int i = 0; i < numNonDefaultElements; i++) {
          int delta = Varint.readUnsignedVarInt(in);
          int index = lastIndex + delta;
          lastIndex = index;
          double value = laxPrecision ? in.readFloat() : in.readDouble();
          v.setQuick(index, value);
        }
      } else {
        for (int i = 0; i < numNonDefaultElements; i++) {
          int index = Varint.readUnsignedVarInt(in);
          double value = laxPrecision ? in.readFloat() : in.readDouble();
          v.setQuick(index, value);
        }
      }
    }
    if (named) {
      String name = in.readUTF();
      v = new NamedVector(v, name);
    }
    vector = v;
  }

  /** Write the vector to the output */
  public static void writeVector(DataOutput out, Vector vector) throws IOException {
    writeVector(out, vector, false);
  }

  public static void writeVector(DataOutput out, Vector vector, boolean laxPrecision) throws IOException {
    boolean dense = vector.isDense();
    boolean sequential = vector.isSequentialAccess();
    boolean named = vector instanceof NamedVector;

    out.writeByte((dense ? FLAG_DENSE : 0)
        | (sequential ? FLAG_SEQUENTIAL : 0)
        | (named ? FLAG_NAMED : 0)
        | (laxPrecision ? FLAG_LAX_PRECISION : 0));

    Varint.writeUnsignedVarInt(vector.size(), out);
    if (dense) {
      for (Vector.Element element : vector.all()) {
        if (laxPrecision) {
          out.writeFloat((float) element.get());
        } else {
          out.writeDouble(element.get());
        }
      }
    } else {
      Varint.writeUnsignedVarInt(vector.getNumNonZeroElements(), out);
      Iterator<Element> iter = vector.nonZeroes().iterator();
      if (sequential) {
        int lastIndex = 0;
        while (iter.hasNext()) {
          Vector.Element element = iter.next();
          if (element.get() == 0) {
            continue;
          }
          int thisIndex = element.index();
          // Delta-code indices:
          Varint.writeUnsignedVarInt(thisIndex - lastIndex, out);
          lastIndex = thisIndex;
          if (laxPrecision) {
            out.writeFloat((float) element.get());
          } else {
            out.writeDouble(element.get());
          }
        }
      } else {
        while (iter.hasNext()) {
          Vector.Element element = iter.next();
          if (element.get() == 0) {
            // TODO(robinanil): Fix the damn iterator for the zero element.
            continue;
          }
          Varint.writeUnsignedVarInt(element.index(), out);
          if (laxPrecision) {
            out.writeFloat((float) element.get());
          } else {
            out.writeDouble(element.get());
          }
        }
      }
    }
    if (named) {
      String name = ((NamedVector) vector).getName();
      out.writeUTF(name == null ? "" : name);
    }
  }

  public static Vector readVector(DataInput in) throws IOException {
    VectorWritable v = new VectorWritable();
    v .readFields(in);
    return v.get();
  }

  public static VectorWritable merge(Iterator<VectorWritable> vectors) {
    Vector accumulator = vectors.next().get();
    while (vectors.hasNext()) {
      VectorWritable v = vectors.next();
      if (v != null) {
        for (Element nonZeroElement : v.get().nonZeroes()) {
          accumulator.setQuick(nonZeroElement.index(), nonZeroElement.get());
        }
      }
    }
    return new VectorWritable(accumulator);
  }

  @Override
  public boolean equals(Object o) {
    return o instanceof PactVector && vector.equals(((PactVector) o).getValue());
  }

  @Override
  public int hashCode() {
    return vector.hashCode();
  }

  @Override
  public String toString() {
    return vector.toString();
  }

}

