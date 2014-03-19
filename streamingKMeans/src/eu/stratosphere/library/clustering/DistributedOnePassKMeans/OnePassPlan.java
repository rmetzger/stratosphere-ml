package eu.stratosphere.library.clustering.DistributedOnePassKMeans;

import java.io.IOException;

import eu.stratosphere.api.common.Plan;
import eu.stratosphere.api.common.Program;
import eu.stratosphere.api.common.ProgramDescription;
import eu.stratosphere.api.common.operators.FileDataSink;
import eu.stratosphere.api.common.operators.FileDataSource;
import eu.stratosphere.api.java.record.io.CsvOutputFormat;
import eu.stratosphere.api.java.record.io.TextInputFormat;
import eu.stratosphere.api.java.record.operators.MapOperator;
import eu.stratosphere.api.java.record.operators.ReduceOperator;
import eu.stratosphere.types.IntValue;
import eu.stratosphere.types.StringValue;

/**
 * 
 * @author PRATEEK GAUR (PGAUR19@GMAIL.COM)
 *
 */

public class OnePassPlan implements Program, ProgramDescription {

	public static final String maxClusterSize = "parameter.MAX_CLUSTER_SIZE";
	public static final String kappa = "parameter.KAPPA";

	public static final String facilityCostIncrement = "parameter.MAX_FACILITY_COST_INCREMENT";
	public static final String finalclusters = "parameter.FINAL_CLUSTERS";

	public static final String maxIterations="parameter.MAX_ITERATIONS";
	public static final String trimFraction="parameter.TRIM_FRACTION";
	public static final String kMeansPlusPlusInit="parameter.KMEANS_PLUS_PLUS_INIT";
	public static final String correctWeights="parameter.CORRECT_WEIGHTS";
	public static final String testProbability="parameter.TEST_PROBABILITY";
	public static final String numRuns="parameter.NUM_RUNS";

	public static final String searcherTechnique ="parameter.SEARCHER_TECHNIQUE";
	public static final String distanceTechnique ="parameter.DISTANCE_TECHNIQUE";
	public static final String numProjections = "parameter.NUM_PROJECTIONS";
	public static final String searchSize = "parameter.SEARCH_SIZE";

	@Override
	public String getDescription() {
		return "Usage: [inputPath] [outputPath] [maxClusterSize] [kappa] [facilityCostIncrement] [K] ([numSubtasks]) [maxIterations] [trimFraction] [kMeansPlusPlusInit] "
				+ "[correctWeights] [testProbability] [numRuns] [searcherTechnique] [distanceTechnique] [numProjections] [searchSize]";

	}

	@Override
	public Plan getPlan(String... args) {

		String inputPath = args.length >= 1 ? args[0] : "/input";
		String outputPath = args.length >= 2 ? args[1] : "/output";
		String maxCSize=args.length>=3? args[2]: "1.3";
		String inpkappa=args.length>=4? args[3]: "7";
		String facilitycost=args.length>=5? args[4]: "0.1";
		String k=args.length>=6?args[5]: "5";
		int numSubtasks = args.length >= 7 ? Integer.parseInt(args[6]) : 1;
		String imaxIterations=args.length>=8?args[7]: "10";
		String itrimFraction=args.length>=9?args[8]: "0.9f";
		String ikMeansPlusPlusInit=args.length>=10?args[9]: "false";
		String icorrectWeights=args.length>=11?args[10]: "false";
		String itestProbability=args.length>=12?args[11]: "0.0f";
		String inumRuns=args.length>=13?args[12]: "10";
		String isearcherTechnique =args.length>=14?args[13]: "0";
		String idistanceTechnique =args.length>=15?args[14]: "0";
		String inumProjections = args.length>=16?args[15]: "0";
		String isearchSize = args.length>=17?args[16]: "0";

		FileDataSource source = new FileDataSource(new TextInputFormat(), inputPath, "Input Documents");


		MapOperator dfMapper = MapOperator.builder(OnePassMapper.class)
				.input(source)
				.name("Onepass Mapper")
				.build();

		dfMapper.setParameter(facilityCostIncrement, facilitycost);
		dfMapper.setParameter(kappa, inpkappa);
		dfMapper.setParameter(maxClusterSize, maxCSize);
		dfMapper.setParameter(finalclusters, k);
		dfMapper.setParameter(searcherTechnique , isearcherTechnique);
		dfMapper.setParameter(distanceTechnique, idistanceTechnique);
		dfMapper.setParameter(numProjections, inumProjections);
		dfMapper.setParameter(searchSize , isearchSize );



		ReduceOperator dfReducer = ReduceOperator.builder(OnePassReducer.class, IntValue.class, 0)
				.input(dfMapper)
				.name("Iterative Reducer")
				.build();
	
		dfReducer.setParameter(finalclusters, k);
		dfReducer.setParameter(maxIterations,imaxIterations);
		dfReducer.setParameter( trimFraction,itrimFraction);
		dfReducer.setParameter(kMeansPlusPlusInit,ikMeansPlusPlusInit);
		dfReducer.setParameter(correctWeights,icorrectWeights);
		dfReducer.setParameter( testProbability,itestProbability);
		dfReducer.setParameter(numRuns,inumRuns);
		dfReducer.setParameter(searcherTechnique , isearcherTechnique);
		dfReducer.setParameter(distanceTechnique, idistanceTechnique);
		dfReducer.setParameter(numProjections, inumProjections);
		dfReducer.setParameter(searchSize , isearchSize );
		
		FileDataSink sink = new FileDataSink(CsvOutputFormat.class, outputPath, dfReducer, "Centers");
		CsvOutputFormat.configureRecordFormat(sink)
		.recordDelimiter('\n')
		.fieldDelimiter(' ')
		.field(IntValue.class, 0) 
		.field(StringValue.class, 1); 

		Plan plan = new Plan(sink, "One Pass KMeans");
		plan.setDefaultParallelism(numSubtasks);

		return plan;
	}

	public static void main(String[] args)  {

		String inputPath = "file:///Users/prateekgaur/Documents/work/src/eu/stratosphere/library/clustering/DistributedOnePassKMeans/input";

		String outputPath = "file:///Users/prateekgaur/Documents/work/src/eu/stratosphere/library/clustering/DistributedOnePassKMeans/output";



		System.out.println("Reading input from " + inputPath);
		System.out.println("Writing output to " + outputPath);


		Plan toExecute = new OnePassPlan().getPlan(args[0],args[1],args[2],args[3],args[4],args[5],args[6],args[7],args[8],args[9],args[10],args[11],args[12],args[13],args[14],args[15],args[16]);
		try {
			Util.executePlan(toExecute);
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

		try {
			Util.deleteAllTempFiles();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		System.exit(0);

	}
}
