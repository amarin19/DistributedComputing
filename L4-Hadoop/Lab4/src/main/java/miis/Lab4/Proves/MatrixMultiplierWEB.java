package miis.Lab4.Proves;
//package miis.Lab4;

import java.util.Scanner;

import org.apache.hadoop.conf.*;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;


public class MatrixMultiplierWEB {

	/*public static void main(String[] args) {
		//Create a scanner to take input from the user:
		Scanner reader = new Scanner(System.in);

		//Receive the dimensions of matrices P and Q:
		System.out.println("Enter matrix sizes p_row,p_col,q_row,q_col: ");
		String[] s = reader.next().split(",");
		reader.close();

		//Parse the input:
		int p_row = Integer.parseInt(s[0]);
		int p_col = Integer.parseInt(s[1]);
		int q_row = Integer.parseInt(s[2]); //Note this has to be equal to p_col! 
		int q_col = Integer.parseInt(s[3]);

		//Make the matrices:
		int[][] p = MatrixGenerator.create_matrix(p_row,p_col);
		int[][] q = MatrixGenerator.create_matrix(q_row,q_col);

		//Show them (optional)
		for (int i=0; i<p_row; i++) {
			for (int j=0; j<p_col; j++) {
				System.out.print(p[i][j] + " ");
			}
			System.out.println("");
		}
		System.out.println();
		for (int i=0; i<q_row; i++) {
			for (int j=0; j<q_col; j++) {
				System.out.print(q[i][j] + " ");
			}
			System.out.println("");
		}
	}*/

	//public class MatrixMultiply {

	public static void main(String[] args) throws Exception {
		if (args.length != 2) {
			System.err.println("Usage: MatrixMultiply <in_dir> <out_dir>");
			System.exit(2);
		}
		Configuration conf = new Configuration();
		// M is an m-by-n matrix; N is an n-by-p matrix.
		// IT HAS TO BE MANUALLY WRITTEN EVERY TIME.
		conf.set("m", "2");
		conf.set("n", "8");
		conf.set("p", "2");
		@SuppressWarnings("deprecation")
		Job job = new Job(conf, "MatrixMultiplier");
		job.setJarByClass(MatrixMultiplierWEB.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);

		job.setMapperClass(MapWEB.class);
		job.setReducerClass(ReduceWEB.class);

		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);

		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));

		job.waitForCompletion(true);
	}
}
