/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package solve;

/**
 *
 * @author anz
 */
import org.apache.commons.io.FileUtils;
import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.List;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class Driver {
	public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {
		String[] label = {"Rad Flow","Fpv Close","Fpv Open","High","Bypass","Bpv Close","Bpv Open"};
                int num_features=0;
                int no_of_true = 0;
                List<String> predict = new ArrayList<String>();
		Configuration conf=new Configuration();
		FileSystem hdfs=FileSystem.get(conf);
		//args[0] is the path to the file which has features of the input waiting to be classified.
		BufferedReader br = new BufferedReader(new InputStreamReader(hdfs.open(new Path(args[0]))));
		String line=null;
                int count = 0;
		while(true){
                        line = br.readLine();
                        if(line==null || line.equals("") ||line.charAt(0)==' ')    break;
                        line = line.replaceAll("\\s+"," ");
			String[] feat=line.toString().split("\\ ");
			for(int i=0;i<feat.length-1;i++)
                            conf.setInt("feat"+i, Integer.parseInt(feat[i]));
			num_features=feat.length-1;
                        String result = feat[feat.length-1];
                        conf.setInt("num_features",num_features);
		//args[1] is the name of the entity to be classified.
                    conf.set("name",args[1]);
                    Job job = new Job(conf,"KNN Classification MapReduce"+count);
                    job.setJarByClass(Driver.class);
                    //args[2] is the path to the input file which will be used for training.
                    FileInputFormat.setInputPaths(job, new Path(args[2]));
                    //args[3] is the path to the output file.
                    FileOutputFormat.setOutputPath(job, new Path(args[3]));
                    job.setMapperClass(Map.class);
                    //job.setCombinerClass(Reduce.class);
                    job.setReducerClass(Reduce.class);
                    job.setNumReduceTasks(1);
                    job.setOutputKeyClass(Text.class);
                    job.setOutputValueClass(Text.class);
                    
                    job.waitForCompletion(true);
                        count++;
                        BufferedReader bre = new BufferedReader(new InputStreamReader(hdfs.open(new Path("/output/part-r-00000"))));
                        String pred = bre.readLine();
                        String r=label[Integer.parseInt(result)-1];
                        predict.add(r);
                        if(r.equals(pred)){
                            no_of_true++;
                        }
                        hdfs.delete(new Path("/output"), true);
                        conf.unset("name");
                        conf.unset("num_features");
                        for(int i=0;i<feat.length;i++)
                            conf.unset("feat"+i);
//                        FileUtils.deleteDirectory(new File("/home/anz/Desktop/output"));
		}
                try{
      //delete file if exist
      if(hdfs.exists(new Path("/result.txt"))){
        hdfs.delete(new Path("/result.txt"), true);
      }
      if(hdfs.exists(new Path("/predict.txt"))){
        hdfs.delete(new Path("/predict.txt"), true);
      }
      FSDataOutputStream fos = hdfs.create(new Path("/predict.txt"));
      for(int i=0;i<predict.size();i++){
          fos.writeBytes(predict.get(i)+"\n");
      }
      fos.close();
      //create file and write content to file
      fos = hdfs.create(new Path("/result.txt"));
      no_of_true*=100;
      Float temp = (float)no_of_true/(float)(count);
      fos.writeBytes(Float.toString(temp));
      fos.close();
    }catch(IOException ex){
      System.out.println(ex.getMessage());
    }
    br.close();
    hdfs.close();
		
    }
}