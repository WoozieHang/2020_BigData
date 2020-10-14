import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;

import java.util.HashSet;
import java.util.StringTokenizer;

import org.apache.hadoop.io.DoubleWritable;
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
import java.util.Vector;

public class Map2_2 extends Mapper<Object,Text,Text,Text>
{
    //默认读取：每行的行号作为key，每行的内容作为value
    //Map2_2 输出key：className;textName;vector dimension          输出value：column_id;tf-idf
    //reducer2_2 输出key：className                   输出value：vector_of_tf_idf
    private static Text word=new Text();
    private static Text one=new Text("1");
    private static  Vector<String> vectorWord=new Vector<String>();
    private static Vector<String> vectorTotalNum=new Vector<String>();
    private Path[] localFiles;

    @Override
    public void setup(Context context) throws IOException,InterruptedException{
        Configuration conf=context.getConfiguration();
        localFiles=context.getLocalCacheFiles();
        //将缓存文件内容读入到当前Map Task的全局变量中
        String lineStr;
        BufferedReader br=new BufferedReader(new FileReader(localFiles[0].toString()));
        while((lineStr=br.readLine())!=null){
            vectorWord.add(lineStr.split("\t")[0]);
            vectorTotalNum.add(lineStr.split("\t")[1]);
        }
        br.close();
    }

    @Override
    public void map(Object key,Text value,Context context) throws IOException,InterruptedException
    {
        String str=value.toString();
        String className=str.split("\t")[0];
        String TextName=str.split("\t")[1];
        String word=str.split("\t")[2];
        String freq=str.split("\t")[3];
        int col_id=0;
        for(;col_id<vectorWord.size();col_id++){
            if(vectorWord.get(col_id).equals(word))
                break;
        }
        if(col_id<vectorWord.size()) {
            double tf=Double.parseDouble(freq);
            double idf=((double)20000)/(Double.parseDouble(vectorTotalNum.get(col_id))+1);
            idf=Math.log(idf);
            double tf_idf=tf*idf;
            context.write(new Text(className+"\t"+TextName+"\t"+vectorWord.size()),new Text(col_id+"\t"+tf_idf));
        }
    }
}
