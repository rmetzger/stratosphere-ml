package eu.stratosphere.itemsimilarity.ItemSimilarityTask;

import java.io.File;
import java.io.IOException;

import org.apache.commons.io.FileUtils;
import org.apache.mahout.math.hadoop.similarity.cooccurrence.measures.VectorSimilarityMeasures;

import eu.stratosphere.api.common.Plan;
import eu.stratosphere.api.common.Program;
import eu.stratosphere.api.common.ProgramDescription;
import eu.stratosphere.api.common.operators.FileDataSink;
import eu.stratosphere.api.common.operators.FileDataSource;
import eu.stratosphere.api.java.record.io.CsvOutputFormat;
import eu.stratosphere.api.java.record.io.TextInputFormat;
import eu.stratosphere.api.java.record.operators.CrossOperator;
import eu.stratosphere.api.java.record.operators.MapOperator;
import eu.stratosphere.api.java.record.operators.ReduceOperator;
import eu.stratosphere.client.LocalExecutor;
import eu.stratosphere.itemsimilarity.common.VectorW;
import eu.stratosphere.nephele.client.JobExecutionResult;
import eu.stratosphere.types.LongValue;

/**
 * 
 *<p>ItemBased Collaborative Filtering using BroadCast Variables</p>
 * 
 *<p>Command line arguments specific to this class are:</p>
 *
 ** <ol>
 * <li>Number of sub tasks</li>
 * <li>input path</li>
 * <li>output path</li>
 * <li>Similarity measure class to from </li>
 * <li>Maximum number of similarities considered per item (500)</li>
 * <li>Maximum number of preference per user to be considered for sampling (100) </li>
 * <li>Threshold of the similarity value to be considered</li>
 * </ol>
 * 
 * @author JANANI CHAKKARADHARI, SILVIA JULINDA, SURYAMITA HARINDRARI
 * 
 */
public class ItemSimilarityPlan implements Program, ProgramDescription{
	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	
	public static int MAX_INT_VALUE = Integer.MAX_VALUE;
	
	public String getDescription() {
		return "Usage:([numSubtasks]) [inputPath] [outputPath] [sim_measure] [mostSimilar] [maxPrefPerUser] [threshold]";
	}

	@Override
	public Plan getPlan(String... args) {
	
		int numSubtasks = args.length >= 1 ? Integer.parseInt(args[0]) : 1;
		System.out.println("Number of tasks--"+numSubtasks);
		String inputPath =  args.length >= 2 ? args[1] : "";
		System.out.println("inputpath--"+inputPath);
		String outputPath =  args.length >= 3 ? args[2]: "";
		System.out.println("outputpath--"+outputPath);
		String sim_measure =  args.length >= 4 ? args[3]: "SIMILARITY_EUCLIDEAN_DISTANCE";
		System.out.println("similarity measure--"+sim_measure);
		int maxSimilaritiesPerRow = args.length >= 5 ? Integer.parseInt(args[4]): 100;
		System.out.println("maxSimilaritiesPerRow---"+maxSimilaritiesPerRow);
		int maxPrefPerUser =  args.length >= 6 ?Integer.parseInt(args[5]):500;
		System.out.println("maxPrefPerUser---"+maxPrefPerUser);
		double threshold = args.length >= 7? Double.parseDouble(args[6]):Double.MIN_VALUE;
		System.out.println("threshold---"+threshold);
		long randomSeed = args.length >=8? Long.parseLong(args[7]):UserVectorReducer.NO_FIXED_RANDOM_SEED;
		System.out.println("randomSeed---"+randomSeed);
		
		
	/**
	 *Reads the input file 	
	 */
	FileDataSource source = new FileDataSource(new TextInputFormat(), inputPath, "Input Documents");
	FileDataSource source2 = new FileDataSource(new TextInputFormat(), inputPath, "Input Documents");
	/**.
	* Creates data source for input file and emits (item,(user,rating)) (key,value) pairs
	*/
	MapOperator userVecMapper = MapOperator.builder(UserVectorMapper.class)
			.input(source)
			.name("UserVectorMapper")
			.build();
	MapOperator userVecMapper2 = MapOperator.builder(new UserVectorMapper())
		    .input(source2)
		    .name("UserVector Mapper2")
		    .build();
	
	/**.
	 *  UserVectorReducer- groups by item key and forms a new pair (item,<user,rating>) 
	 * -samples down the user preference based on the input parameter "maxPrefPerUser"
	 * -pre-processing and computes norms for the corresponding item based on the supplied SIMILARITY_MEASURE
	 * -
	*/
	ReduceOperator userVecReducer =  ReduceOperator.builder(new UserVectorReducer(sim_measure,maxPrefPerUser,randomSeed,threshold), LongValue.class, 0)
			.input(userVecMapper)
			.name("UserVectorReducer")
			.build();
	
	ReduceOperator userVecReducer2 =  ReduceOperator.builder(new UserVectorReducer(sim_measure,maxPrefPerUser,randomSeed,threshold), LongValue.class, 0)
			.input(userVecMapper2)
			.name("UserVectorReducer2")
			.build();
	
	/**
	 * Groups norms into a single vector
	 */
		ReduceOperator normReducer = ReduceOperator.builder(NormReducer.class)
				.input(userVecReducer)
				.name("NormReducer")
				.build();
		
		ReduceOperator normReducer2 = ReduceOperator.builder(NormReducer.class)
				.input(userVecReducer)
				.name("NormReducer")
				.build();
		
		
		
	/**
	 * ItemVectorMapper- Takes sampled (item,<user,rating>) and emits (Transposed Matrix) as (user,(item,rating))
	 */
	MapOperator itemVecMapper = MapOperator.builder(ItemVectorMapper.class)
				.input(userVecReducer)
				.name("ItemVectorMapper")
				.build();

	MapOperator itemVecMapper2 = MapOperator.builder(ItemVectorMapper.class)
			.input(userVecReducer2)
			.name("ItemVectorMapper2")
			.build();
		
	/**
	 * ItemVectorReducer: reduces on user key and emits (user,<item,rating>) [basically the user with item history]
	 */
	ReduceOperator itemVecReducer = ReduceOperator.builder(ItemVectorReducer.class, LongValue.class, 0)
				.input(itemVecMapper)
				.name("ItemVectorReducer")
				.build();
	ReduceOperator itemVecReducer2 = ReduceOperator.builder(ItemVectorReducer.class, LongValue.class, 0)
			.input(itemVecMapper2)
			.name("ItemVector Reducer")
			.build();
	/**
	 * UserCountReducer Computes number of users
	 */
	ReduceOperator noOfUsersReducer = ReduceOperator.builder(UserCountReducer.class)
				.input(itemVecReducer)
				.name("UserCountReducer")
				.build();
		
	/**
	 * CooccurrencesMapper computes partial co-occurrences of item pairs and emits (item,<partial co-occurrences>)
	 */					
	MapOperator cofReducer = MapOperator.builder(new CooccurrencesMapper(sim_measure,threshold))
			.setBroadcastVariable("nonzeroMaxBC", normReducer2)
				.input(itemVecReducer2)
				.name("Cooccurrences-Mapper")
				.build();

	/**
	 * A cross to combine two inputs norms and user count. This cross always has two single input records and emits one output record
	 */
	CrossOperator normUserCross = CrossOperator.builder(NormUserCountCross.class)
				.input1(normReducer)
				.input2(noOfUsersReducer)
				.name("Norm-UserCount-Cross")
				.build();
	/**
	 * Takes the partial co-occurrences vector which is grouped by item and aggregates to form complete vector of co-occurences	
	 */
	ReduceOperator simReducer = ReduceOperator.builder(new SimilarityReducer(sim_measure,threshold), LongValue.class, 0)
			.setBroadcastVariable("myNormBC", normUserCross)
			.input(cofReducer)
			.name("Similarity Reducer")
			.build();

	/**
	 * Filters the most similar items based on input parameter before writing the similarity matrix to disk. 
	 */
	MapOperator mostSimilar = MapOperator.builder(new MostSimilarItemPairsMapper(maxSimilaritiesPerRow))
				.input(simReducer)
				.name("Most Similar Item Pairs")
				.build();
		
	FileDataSink sink = new FileDataSink(new CsvOutputFormat(), outputPath+"//SimilarityMatrix", mostSimilar, "Similarity Matrix");
		CsvOutputFormat.configureRecordFormat(sink)
			.recordDelimiter('\n')
			.fieldDelimiter('\n')
			.field(LongValue.class,0)
			.field(VectorW.class, 1);

		Plan plan = new Plan(sink, "Item Similarity Computation");
		plan.setDefaultParallelism(numSubtasks);
		
		return plan;
	}
	
	public static void main(String[] args) throws Exception {

		try{
			String numtask =args[0];
			String inputPath =args[1];
			String outputPath =args[2];
			
			String similarityMeasure = "SIMILARITY_EUCLIDEAN_DISTANCE";
			if(args.length >=4){
				similarityMeasure = args[3];	
			}
						
			String maxsimperitem = "100";
			if(args.length>=5){
				maxsimperitem = args[4];
			}
			String maxPrefPerUser ="500";
			if(args.length>=6){
				maxPrefPerUser = args[5];
			}
			 String threshold = String.valueOf(Double.MIN_VALUE);
			if(args.length>=7){
				threshold = args[6];
			}
			

			String randomSeed = String.valueOf(UserVectorReducer.NO_FIXED_RANDOM_SEED);
			if(args.length>=8){
				randomSeed = args[7];
			}
			
			System.out.println("Reading input from " + inputPath );
			Plan toExecute = new ItemSimilarityPlan().getPlan(numtask,inputPath, outputPath,similarityMeasure,maxsimperitem,maxPrefPerUser,threshold,randomSeed);
			
			
			//System.err.println(LocalExecutor.getPlanAsJSON(toExecute));
			
			LocalExecutor executor = new LocalExecutor();
			executor.start();
			JobExecutionResult runtime = executor.executePlan(toExecute);
			System.out.println("runtime:  " + runtime.getNetRuntime());
			
			executor.stop();
			
		}
		catch(Exception e){
			e.printStackTrace();
			System.out.println("Argument invalid");
		}
		
	}
	
	
	public static void deleteAllTempFiles(String folder) throws IOException {
		File fold = new File(folder);
		File[] tempFiles = fold.listFiles();
		for (File f : tempFiles) {
			if (f.exists()) {
				deleteRecursively(f);
			}
		}
	}
	private static void deleteRecursively(File f) throws IOException {
		if (f.isDirectory()) {
			FileUtils.deleteDirectory(f);
		} else {
			f.delete();
		}
	}
	
	public static String getSimilarityClassName(String inputMeasure){
		
		String className =null;
		if("SIMILARITY_PEARSON_CORRELATION".equalsIgnoreCase(inputMeasure))
			className=VectorSimilarityMeasures.SIMILARITY_PEARSON_CORRELATION.getClassname();
		else if("SIMILARITY_EUCLIDEAN_DISTANCE".equalsIgnoreCase(inputMeasure))
			className=VectorSimilarityMeasures.SIMILARITY_EUCLIDEAN_DISTANCE.getClassname();
		else if("SIMILARITY_COSINE".equalsIgnoreCase(inputMeasure))
			className=VectorSimilarityMeasures.SIMILARITY_COSINE.getClassname();
		else if("SIMILARITY_TANIMOTO_COEFFICIENT".equalsIgnoreCase(inputMeasure))
			className=VectorSimilarityMeasures.SIMILARITY_TANIMOTO_COEFFICIENT.getClassname();
		else if("SIMILARITY_CITY_BLOCK".equalsIgnoreCase(inputMeasure))
			className=VectorSimilarityMeasures.SIMILARITY_CITY_BLOCK.getClassname();
		else if("SIMILARITY_COOCCURRENCE".equalsIgnoreCase(inputMeasure))
			className=VectorSimilarityMeasures.SIMILARITY_COOCCURRENCE.getClassname();
		else if("SIMILARITY_LOGLIKELIHOOD".equalsIgnoreCase(inputMeasure))
			className=VectorSimilarityMeasures.SIMILARITY_LOGLIKELIHOOD.getClassname();
		
		return className;
		
	}
	
	
	
	
}
