package mr;

import java.io.IOException;
import java.util.StringTokenizer;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;

public class MapClass extends MapReduceBase
implements Mapper<LongWritable, Text, Text, IntWritable> {

	private final static IntWritable one = new IntWritable(1);
	private Text attValue = new Text();
	private Text cLabel = new Text();
	private int i;
	private String token;
	public static int no_Attr;
	//public static int splitAttr[];
	private int flag=0;


	public void map(LongWritable key, 
					Text value,OutputCollector<Text, 
					IntWritable> output,Reporter reporter) throws IOException {

		C45 id = new C45();
		//System.out.println(" IN MAP CLASS ");
		Split split = null;
		int size_split = 0;
		split = id.currentsplit;

		String line = value.toString();      //changing input instance value to string
		StringTokenizer itr = new StringTokenizer(line);
		int index = 0;
		String attr_value = null;
		no_Attr = itr.countTokens()-1;
		String attr[] = new String[no_Attr];
		boolean match = true;
		for(i =0;i<no_Attr;i++)
		{
			attr[i]=itr.nextToken();		//Finding the values of different attributes
		}
		String classLabel=itr.nextToken();
		size_split=split.attr_index.size();
		//System.out.println("size_split " + size_split);
		for(int count=0;count<size_split;count++)
		{
			index=(Integer) split.attr_index.get(count);
			attr_value=(String)split.attr_value.get(count);
			//System.out.println(" index = " + index + " attr_value = " + attr_value);
			if(attr[index].equals(attr_value))   //may also use attr[index][z][1].contentEquals(attr_value)
			{
				//System.out.println("EQUALS IN MAP  nodes  "+attr[index]+"   inline  "+attr_value);
			}
			else
			{
				//System.out.println("NOT EQUAL IN MAP  nodes  "+attr[index]+"   inline  "+attr_value);
				match=false;
				break;
			}
		}

	  	//id.attr_count=new int[no_Attr];

		if(match)
		{
			//System.out.println( " IN IF ---> attrValue = " + attr_value + " one = " + one);
			for(int l=0;l<no_Attr;l++)
			{  
				if(split.attr_index.contains(l))
				{
				  
				}
				else
				{
				  token=l + " " + attr[l] + " " + classLabel;
				  attValue.set(token);
				  //System.out.println( " ADDING TO OUTPUT ---> attrValue = " + attValue + " one = " + one);
				  output.collect(attValue, one);
				  //System.out.println("Added after mapper");
				}
			}
			if(size_split == no_Attr)
			{
				token=no_Attr+" "+"null"+" "+classLabel;
				attValue.set(token);
				//System.out.println( " ADDING TO OUTPUT IN SECOND IF ---> attrValue = " + attValue + " one = " + one);
				output.collect(attValue, one);
				//System.out.println("Added after mapper");
			}
		}
	}
}
  
