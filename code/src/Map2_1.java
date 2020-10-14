import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;

import java.util.HashSet;
import java.util.StringTokenizer;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.filecache.DistributedCache;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class Map2_1 extends Mapper<Object,Text,Text,Text>
{
    //默认读取：每行的行号作为key，每行的内容作为value
    //Map2_1 输出key：word;className;textName;          输出value：1
    //Combiner2_1 输出key：className;textName          输出value：word;num
    //reducer2_1 输出key：className;textName           输出value：word;freq
    private static Text word=new Text();
    private static Text one=new Text("1");

    @Override
    public void map(Object key,Text value,Context context) throws IOException,InterruptedException
    {
        FileSplit fs=(FileSplit) context.getInputSplit();
        String TextName=fs.getPath().getName();//文件名（作品名）
        String PathStr=fs.getPath().getParent().toString();
        String []dirName=PathStr.split("/");
        String className=dirName[dirName.length-1];
        StringTokenizer str=new StringTokenizer(value.toString());
        while (str.hasMoreTokens()) {
            String a=str.nextToken();
            int index=0;
            StringBuffer sb=new StringBuffer();
            while(index<a.length()){
                char tmp=a.charAt(index);
                if(Character.isUpperCase(tmp))
                    sb.append(Character.toLowerCase(tmp));

                else if(Character.isLowerCase(tmp))
                    sb.append(tmp);
                index=index+1;
            }
            if(sb.length()>0) {
                String b = new String(sb);
                word.set(b + "&" + className + "&" + TextName);
                context.write(word, one);
            }
        }
    }
}
