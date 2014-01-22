package de.tuberlin.dima.ml.pact.io;

import org.apache.mahout.math.SequentialAccessSparseVector;
import org.apache.mahout.math.Vector;

import de.tuberlin.dima.ml.pact.types.PactVector;
import eu.stratosphere.nephele.configuration.Configuration;
import eu.stratosphere.pact.common.io.DelimitedInputFormat;
import eu.stratosphere.pact.common.type.PactRecord;
import eu.stratosphere.pact.common.type.base.PactDouble;
import eu.stratosphere.pact.common.type.base.PactInteger;
import eu.stratosphere.pact.common.type.base.parser.DecimalTextDoubleParser;
import eu.stratosphere.pact.common.type.base.parser.DecimalTextIntParser;

/**
 * Read file in libsvm format.
 * The file may be have multiple labels per record or a single label (+1 or -1)
 * The first numbers are a comma-separated list of the labels, or the single label. After one or two spaces follows a sparse encoding of the feature
 * vector<br>
 * In multilabel mode: If the labels is equal to the positive class label, the label will be set to 1, otherwise it will be 0.
 * 
 * Multilabel example: '9,33,62,60,70  268:0.059 440:0.031 577:0.064'
 * Single label example: '+1 20:1.2 30:-2.1234'
 * 
 */
public class LibsvmInputFormat extends DelimitedInputFormat {

	// ------------------------------------- Config Keys ------------------------------------------

	public static final String CONF_KEY_NUM_FEATURES = "libsvm.num_features";
	
	public static final String CONF_KEY_MULTI_LABEL_INPUT = "libsvm.multi_label_input";

    public static final String CONF_KEY_POSITIVE_CLASS = "libsvm.positive_class";

	// --------------------------------------- Config ---------------------------------------------

	private static final char DELIMITER = ' ';

	private static final char DELIMITER_LABEL = ',';

	private static final char DELIMITER_FEATURE = ':';

	private static final int POSITIVE_CLASS_UNDEFINED = -1;

	private static final int NUM_FEATURES_UNDEFINED = -1;
	
	private static final boolean MULTI_LABEL_INPUT_UNDEFINED = true;

	// --------------------------------------- Output ---------------------------------------------

	private final PactInteger label = new PactInteger();

	private final PactVector features = new PactVector();

	// -------------------------------------- Internal --------------------------------------------

	private PactInteger index = new PactInteger();

	private PactDouble weight = new PactDouble();

	private DecimalTextIntParser intParser = new DecimalTextIntParser();

	private DecimalTextDoubleParser doubleParser = new DecimalTextDoubleParser();
	
	private boolean multiLabelInput;

	// Only relevant for multi label input
	private int positiveClass;
	
	private int numFeatures;

	// --------------------------------------------------------------------------------------------

	@Override
	public void configure(Configuration parameters) {
		super.configure(parameters);

		// num features
		this.numFeatures = parameters.getInteger(CONF_KEY_NUM_FEATURES, NUM_FEATURES_UNDEFINED);
		if (this.numFeatures == NUM_FEATURES_UNDEFINED) {
			throw new IllegalArgumentException("Please specify the number of features for the vector");
		}
		
		// multi label input?
		this.multiLabelInput = parameters.getBoolean(CONF_KEY_MULTI_LABEL_INPUT, MULTI_LABEL_INPUT_UNDEFINED);

		if (this.multiLabelInput) {
		  // target class
		  this.positiveClass = parameters.getInteger(CONF_KEY_POSITIVE_CLASS, POSITIVE_CLASS_UNDEFINED);
		  if (this.positiveClass == POSITIVE_CLASS_UNDEFINED) {
		    throw new IllegalArgumentException("Please specify the positive class id");
		  }
		}
	}

	@Override
	public boolean readRecord(PactRecord target, byte[] bytes, int offset, int numBytes) {

		final int limit = offset + numBytes;
		int readPos = offset;

		// ------------------------------------
		// 1. parse labels
		// ------------------------------------

		// move behind labels
		while (bytes[readPos++] != DELIMITER) { }

		boolean isPositive = false;

		int currentOffset = offset;
		while (currentOffset < readPos - 1) {
		  if (!multiLabelInput) {
		    // Positive class is encoded as '+'; skip over the '+'
		    if (bytes[currentOffset] == '+') {
		      ++currentOffset;
		    }
		  }
          currentOffset = this.intParser.parseField(bytes, currentOffset, readPos - 1, DELIMITER_LABEL, this.label);
          if (multiLabelInput) {
			if (this.label.getValue() == this.positiveClass) {
			  isPositive = true;
			  break;
			}
          } else {
            if (this.label.getValue() == 1) {
              isPositive = true;
              break;
            }
          }
		}

		this.label.setValue(isPositive ? 1 : 0);
		target.setField(0, this.label);

		// ------------------------------------
		// 2. parse features
		// ------------------------------------
		Vector vector = new SequentialAccessSparseVector(this.numFeatures);

		// Either a single or two spaces delimit the labels from the features
		currentOffset = readPos;
        if (bytes[readPos] == DELIMITER) {
          ++currentOffset;
        }

		while (currentOffset < limit) {

			currentOffset = this.intParser.parseField(bytes, currentOffset, limit, DELIMITER_FEATURE, this.index);

			currentOffset = this.doubleParser.parseField(bytes, currentOffset, limit, DELIMITER, this.weight);

			vector.setQuick(this.index.getValue(), this.weight.getValue());
		}

		this.features.setValue(vector);
		target.setField(1, this.features);

		return true;
	}
}
