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
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map.Entry;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import static solve.Map.dists;

public class Reduce extends Reducer<Text, Text, Text, Text> {
	String flower_name=null;
	@Override
	public void setup(Context context){
		flower_name=String.valueOf(context.getConfiguration().get("name"));
	}
	public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException{
		HashMap<String,Integer> map=new HashMap<String,Integer>();
                String maxkey=null;int maxvalue=-1;
		for(Text value:values){
			if(!map.containsKey(value.toString())){
				map.put(value.toString(), 1);
			}
			else{
				map.put(value.toString(), map.get(value.toString())+1);
			}
		}
		for(Entry<String, Integer> entry: map.entrySet()){
			if(entry.getValue()>maxvalue){
				maxkey=entry.getKey();
				maxvalue=entry.getValue();
			}
		}
		context.write(null, new Text(maxkey));
//                ArrayList<String> dists=new ArrayList<String>();
//		Iterator iter = values.iterator();
//                while(iter.hasNext()){
//                    dists.add(iter.next().toString());
//                }
//		Collections.sort(dists);
//		int iter2=0;
//		String[] species=new String[3];
//		String str="";
//		for(int i=0;i<3;i++){
//			str=dists.get(i);
//			String spec=String.valueOf(str.replaceAll("[\\d.]", ""));
//			species[iter2]=spec;
//			iter2++;
//		}
//		Arrays.sort(species);
//		for(int i=0;i<species.length-1;i++){
//			if(species[i].equals(species[i+1])){
//				context.write(null, new Text(species[i]));
//				break;
//			}
//		}
	}
}
