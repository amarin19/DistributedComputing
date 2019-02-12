package miis.Lab4.Proves;

import java.io.IOException;
import java.util.StringTokenizer;
import java.util.HashMap;
import java.io.BufferedReader;
import java.io.InputStreamReader;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class project {

	public static class countMatrix extends Mapper<Object, Text, Text, Text>{

		private  Text val = new Text();
		private Text keymtext = new Text();

		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
			String keym= "";
			String[] columns = value.toString().split(",");

			if (columns[0].equals("B")){
				keym = "N";
				keymtext.set(keym);
				val.set(String.valueOf(columns[2]));
				context.write(keymtext, val);  
			}else{
				keym = "L";
				keymtext.set(keym);
				val.set(String.valueOf(columns[1]));
				context.write(keymtext, val);				
			}  
		} 
	}

	public static class MultiplierMapper extends Mapper<Object, Text, Text, Text>{
		//A(LxM) and B(MxN)
		//Output (key),(value)
		//Output for A: (i,k),(Aj,Aij)
		//Output for B: (i,k),(Bj,Bjk) 
		// Where 
		// i=0,1,...L
		// j=0,1,..,M
		// k=0,1,..,N

		private  Text val = new Text();
		private Text keymtext = new Text();

		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
			Configuration conf = context.getConfiguration();
			int L=Integer.parseInt(conf.get("L"));
			int N=Integer.parseInt(conf.get("N"));

			System.out.println("Entries here");
			String rows=value.toString();
			String keym= "";
			String[] columns = rows.split(",");

			if (columns[0].equals("B")){
				for (int i=0;i<L;i++){
					keym = i+","+columns[2];
					keymtext.set(keym);
					val.set("B,"+columns[1]+","+ columns[3]);
					context.write(keymtext, val);
					System.out.println("("+keym+"),("+val+")");

				}
			}else{
				for (int k=0;k<N;k++){
					keym = columns[1]+","+k;
					keymtext.set(keym);
					val.set("A,"+columns[2]+","+columns[3]);
					context.write(keymtext, val);
					System.out.println("("+keym+"),("+val+")");
				}
			}  

		}
	}


	public static class SumMapper extends Mapper<Object, Text, Text, Text>{
		private Text keymtext = new Text();
		private Text val = new Text();
		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {

			System.out.println("Entries here: " + value.toString());
			StringTokenizer itr = new StringTokenizer(value.toString());
			keymtext.set(itr.nextToken());
			val.set(itr.nextToken());
			context.write(keymtext, val);
		}
	}


	public static class IntMultReducer extends Reducer<Text,Text,Text,Text> {
		private Text result = new Text();

		public void reduce(Text key, Iterable<Text> values,Context context) throws IOException, InterruptedException {
			System.out.println("Multiplier Reducer");

			int mul = 1;
			HashMap<Integer,Integer> A = new HashMap<Integer,Integer>();
			HashMap<Integer,Integer> B = new HashMap<Integer,Integer>();


			for (Text val : values) {
				String[] multval = val.toString().split(",");	
				if (multval[0].equals("A")){
					A.put(Integer.parseInt(multval[1]),Integer.parseInt(multval[2]));
					//System.out.println("setmapA key["+Integer.parseInt(multval[1])+"] value["+Integer.parseInt(multval[2])+"]");

				}else{
					B.put(Integer.parseInt(multval[1]),Integer.parseInt(multval[2]));
					///System.out.println("setmapB key["+Integer.parseInt(multval[1])+"] value["+Integer.parseInt(multval[2])+"]");

				}
			}

			for (int i=0;i<A.size();i++){
				mul=A.get(i)*B.get(i);
				System.out.println("multipliying from ["+key.toString()+"]["+i+"] "+A.get(i)+"*"+B.get(i)+"="+mul);

				result.set(String.valueOf(mul));
				context.write(key, result);
			}

		}
	}

	public static class IntSumReducer extends Reducer<Text,Text,Text,Text> {
		private Text result = new Text();

		public void reduce(Text key, Iterable<Text> values,Context context) throws IOException, InterruptedException {
			System.out.println("Entra al reducer de suma");

			int sum = 0;
			for (Text val : values) {
				sum += Integer.parseInt(val.toString());
			}
			System.out.println("sum of ["+key.toString()+"] = "+ sum);

			result.set(String.valueOf(sum));
			context.write(key, result);
		}
	}


	public static class IntCount extends Reducer<Text,Text,Text,Text> {
		private Text result = new Text();

		public void reduce(Text key, Iterable<Text> values,Context context) throws IOException, InterruptedException {
			System.out.println("Entra al reducer del count");

			int bigger = 0;
			for (Text val : values) {
				int possible =  Integer.parseInt(val.toString())+1;
				if (possible>bigger){
					bigger=possible;
				}
			}
			System.out.println("sum of ["+key.toString()+"] = "+ bigger);

			result.set(String.valueOf(bigger));
			context.write(key, result);
		}
	}



	public static void main(String[] args) throws Exception {
		//Job1
		Path outputpath = new Path("intermediate_output");

		Configuration conf = new Configuration();
		FileSystem hdfs = FileSystem.get(conf);
		hdfs.delete(outputpath,true);
		hdfs.delete(new Path(args[1]),true);

		Job job3 = Job.getInstance(conf, "matrix counter");
		job3.setJarByClass(project.class);
		job3.setMapperClass(countMatrix.class);
		job3.setReducerClass(IntCount.class);
		job3.setOutputKeyClass(Text.class);
		job3.setOutputValueClass(Text.class);
		FileInputFormat.addInputPath(job3, new Path(args[0]));
		FileOutputFormat.setOutputPath(job3, outputpath);
		boolean ok = job3.waitForCompletion(true);
		if (ok){
			outputpath = new Path("intermediate_output/part-r-00000");
			BufferedReader br=new BufferedReader(new InputStreamReader(hdfs.open(outputpath)));
			String line;
			line=br.readLine();
			while (line != null){

				StringTokenizer itr = new StringTokenizer(line);
				String val=itr.nextToken();
				if(val.equals("L")){
					val=itr.nextToken();
					conf.set("L",val);
				}else{
					val=itr.nextToken();
					conf.set("N",val);
				}
				line=br.readLine();
			}
			hdfs.delete(outputpath,true);


			Job job = Job.getInstance(conf, "matrix mult");
			job.setJarByClass(project.class);
			job.setMapperClass(MultiplierMapper.class);
			job.setReducerClass(IntMultReducer.class);
			job.setOutputKeyClass(Text.class);
			job.setOutputValueClass(Text.class);
			FileInputFormat.addInputPath(job, new Path(args[0]));
			FileOutputFormat.setOutputPath(job, outputpath);
			ok = job.waitForCompletion(true);
			if (ok){
				//Job2
				Job job2 = Job.getInstance(conf, "matrix multsum");
				job2.setJarByClass(project.class);
				job2.setMapperClass(SumMapper.class);
				job2.setReducerClass(IntSumReducer.class);
				job2.setOutputKeyClass(Text.class);
				job2.setOutputValueClass(Text.class);
				FileInputFormat.addInputPath(job2, outputpath);
				FileOutputFormat.setOutputPath(job2, new Path(args[1]));
				ok = job2.waitForCompletion(true);
				hdfs.delete(outputpath,true);

				if (ok){
					System.exit(0);
				}
			}

		}
		System.exit(1);

	}
}
