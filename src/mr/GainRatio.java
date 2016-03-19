package mr;

import java.io.BufferedReader;
import java.io.DataInputStream;
import java.io.FileInputStream;
import java.io.InputStreamReader;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
class GainRatio
{   
	int linenumber=0;
	static String count[][]=new String[100000][4];
	int currnode[]=new int[10000];
	String majorityLabel=null;
	public String majorityLabel()
	{
		return majorityLabel;
	}

	//Calculation of entrophy
	public double currNodeEntophy()
	{
		System.out.println(" CALCULATING ENTHOPY");
		int currentindex = 0;
		double entropy = 0;
		try {
			currentindex=Integer.parseInt(count[0][0]);
		}
		catch ( NumberFormatException e) {
			e.printStackTrace();
		}
		int i=0;
		int covered[]=new int[1000];
		String classLabel=count[0][2];
		int j=0;
		int ind=-1;
		int maxStrength=0;
		System.out.println("Values in node rep to classwise");
		while(currentindex==Integer.parseInt(count[j][0]))
		{
			if(covered[j]==0)
		    {
				classLabel=count[j][2];
				ind++;
		       	i=j;
				while(currentindex==Integer.parseInt(count[i][0]))
				{   if(covered[i]==0)
				  	{
			  			if(classLabel.contentEquals(count[i][2]))
			  			{
			  				currnode[ind]=currnode[ind]+Integer.parseInt(count[i][3]);
			  				covered[i]=1;
			  			}	
				  	}
		  			i++;
		  			if(i==linenumber)
						  break;
				}
				if(currnode[ind]>maxStrength)
				{
					maxStrength=currnode[ind];
					majorityLabel=classLabel;
				}
				System.out.print("    "+classLabel+"    "+currnode[ind]);
		    }
			else
			{
				j++;
			}
			if(j==linenumber)
				break;
		}
		entropy=entropy(currnode);  
		return entropy;
	}
	  
	public double entropy(int c[])
	{
		System.out.println(" IN ENTROPY OF GAINRATIO");
		
		double entropy=0;

		int i=0;
		int sum=0;
		double frac;
		while(c[i]!=0)
		{
			sum=sum+c[i];
			i++;
		}
		i=0;
		while(c[i]!=0)
		{
			frac=(double)c[i]/sum;
		  	entropy=entropy-frac*(Math.log(frac)/Math.log(2));
		  	i++;
		}
		return entropy;
	}
	  
	  
	  
	public void getcount()
	{ 
		System.out.println(" IN GET COUNT OF GAIN RATIO ");
		C45 id = new C45();
		//FileInputStream fstream;
		try {

			
			FileSystem hdfs = FileSystem.get(new Configuration());
	    	
	    	//FSDataInputStream is = hdfs.open(new Path("hdfs://lazy:54310/output_dm/output/inter"+id.current_index+".txt"));
	    	//FSDataInputStream is = hdfs.open(new Path("/home/anurag/eclipse_mars_workspace/mapreduce/output/inter"+id.current_index+".txt"));
	    	
	    	
	    	//FSDataInputStream is = hdfs.open(new Path("/home/anurag/eclipse_mars_workspace/mapreduce/1/output"+id.current_index+"/part-00000"));
	    	FSDataInputStream is = hdfs.open(new Path("hdfs://lazy:54310/output_dm/1/output"+id.current_index+"/part-00000"));
			
			
			BufferedReader br = new BufferedReader(new InputStreamReader(is));
			String line;
			//Read File Line By Line
			StringTokenizer itr;
			System.out.println("READING FROM intermediate  "+id.current_index);

			
			while ((line = br.readLine()) != null)   {
				itr= new StringTokenizer(line);

				count[linenumber][0]=itr.nextToken();
				count[linenumber][1]=itr.nextToken();
				count[linenumber][2]=itr.nextToken();
				count[linenumber][3]=itr.nextToken();

				int i = linenumber;
	
				linenumber++;
			}
			count[linenumber][0]=null;
			count[linenumber][1]=null;
			count[linenumber][2]=null;
			count[linenumber][3]=null;
			
			is.close();
			br.close();
			hdfs.close();
			
		} 
		catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();  
			//Close the input stream
		}
	}
	  
	  
	  
	public double gainratio(int index,double enp)
	{

		System.out.println(" IN GAIN RATIO OF GAIN RATIO");
		//100 is considered as max ClassLabels
		int c[][]=new int[10000][1000];
		int sum[]=new int[10000]; //
		String currentatrrval="@3#441get";
		double gainratio=0;
		int j=0;
		int m=-1;  //index for split number 
		int lines=linenumber;
		int totalsum=0;
		for(int i=0;i<lines;i++)
		{
			if(Integer.parseInt(count[i][0])==index)
			{
				if(count[i][1].contentEquals(currentatrrval))
				{
					j++;
					c[m][j]=Integer.parseInt(count[i][3]);
					sum[m]=sum[m]+c[m][j];
				}
				else
				{
					  j=0;
					  m++;
					  currentatrrval=count[i][1];
					  c[m][j]=Integer.parseInt(count[i][3]); //(different class) data sets count per m index split
					  sum[m]=c[m][j];
				}     
		  	}
		}
		int p=0;
		while(sum[p]!=0)
		{
			totalsum=totalsum+sum[p]; //calculating total instance in node
			p++;
		}

		double wtenp=0;
		double splitenp=0;
		double part=0;
		for(int splitnum=0;splitnum<=m;splitnum++)
		{
			part=(double)sum[splitnum]/totalsum;
		 	wtenp=wtenp+part*entropy(c[splitnum]);
		}
		splitenp=entropy(sum);
		gainratio=(enp-wtenp)/(splitenp);
		return gainratio;
	}
	  
	  
	public String getvalues(int n)
	{   
		System.out.println(" IN GET VALUES OF GAIN RATIO");
		int flag=0;
		String values="";
		String temp="%%%%%!!@";
		for(int z=0;z<1000;z++)
		{
			if(count[z][0]!=null)
			{
				if(n==Integer.parseInt(count[z][0]))
				{
					flag=1;
	
					if(count[z][1].contentEquals(temp))
					{
						System.out.println("Equals  COUNT  Index z "+z+"   "+count[z][1]+ "temp  "+temp);
					}
					else
					{
						values=values+" "+count[z][1];
						temp=count[z][1];
					}
				}
				else if(flag==1)
					break;
			}
			else
				break;
			}
		return values;
	}
}
	  
