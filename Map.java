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
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class Map extends Mapper<LongWritable, Text, Text, Text> {
	public static long byteoffset=0;
	public static int[] feat=null;
	public static String species=null;
        String[] label = {"Rad Flow","Fpv Close","Fpv Open","High","Bypass","Bpv Close","Bpv Open"};
	public static ArrayList<String> dists=new ArrayList<String>();
	public static float min_dist=0;
	public static int num_features=0;
	public static float euc_dist(int[] feat, int[] test,int num){
		float distance=0;
		float val=0;
		for(int i=0;i<num;i++){
			val+=((feat[i]-test[i])*(feat[i]-test[i]));
		}
		distance=(float) Math.sqrt(val);
		return distance;
	}
	@Override
	public void setup(Context context) throws IOException, InterruptedException{
		num_features=(context.getConfiguration().getInt("num_features",1));
		feat=new int[num_features];
		for(int i=0;i<num_features;i++){
			feat[i]=(context.getConfiguration().getInt("feat"+i, 0));
		}
	}
	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		String temp = value.toString();
                temp = temp.replaceAll("\\s+"," ");
                String[] characteristics=temp.split("\\ ");
		int[] test=new int[num_features];
		for(int i=0;i<num_features;i++){
			test[i]=Integer.parseInt(characteristics[i]);
		}
		species=characteristics[num_features];
                String type=label[Integer.parseInt(species)-1];
		dists.add(String.valueOf(euc_dist(feat,test,num_features))+type);
//                context.write(new Text(type),new Text(String.valueOf(euc_dist(feat,test,num_features))));
		byteoffset=Long.parseLong(key.toString());
//                context.write(new Text("real_predict"),new Text());
	}
	@Override
	public void cleanup(Context context) throws IOException, InterruptedException{
		Collections.sort(dists);
		int iter=0;
		String[] species=new String[3];
		String str="";
		for(int i=0;i<3;i++){
			str=dists.get(i);
			String spec=String.valueOf(str.replaceAll("[\\d.]", ""));
			species[iter]=spec;
			iter++;
		}
		Arrays.sort(species);
		for(int i=0;i<species.length-1;i++){
			if(species[i].equals(species[i+1])){
				context.write(new Text("1"), new Text(species[i]));
				break;
			}
		}
	}
}

