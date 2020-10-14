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



public class Map1_1 extends Mapper<Object,Text,Text,Text>
{
    //默认读取：每行的行号作为key，每行的内容作为value
    //Map1_1 输出key：词语;className;textName          输出value：1
    //Combiner1_1 输出key：词语;className           输出value：textName
    //reducer1_1 输出key：词语                    输出value：Text次数 for certain class...
    private static Text word=new Text();
    private static Text one=new Text("1");
    private HashSet<String> keyWord;
    private Path[] localFiles;

    @Override
    public void setup(Context context) throws IOException,InterruptedException{
        keyWord=new  HashSet<String>();
        Configuration conf=context.getConfiguration();
        localFiles=context.getLocalCacheFiles();
        //将缓存文件内容读入到当前Map Task的全局变量中
        String aKeyWord;
        BufferedReader br=new BufferedReader(new FileReader(localFiles[0].toString()));
        while((aKeyWord=br.readLine())!=null){
            keyWord.add(aKeyWord);
        }
        br.close();
    }

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
            if(!keyWord.contains(a)) {
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
                String b=new String(sb);
                if(!keyWord.contains(b)){
                    word.set(b+"&"+className+"&"+TextName);
                    context.write(word, one);
                }
            }
        }
    }
}


