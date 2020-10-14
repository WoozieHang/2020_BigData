import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.filecache.DistributedCache;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.text.DecimalFormat;
import java.util.HashSet;
import java.util.Vector;

public class Reduce2_1 extends Reducer<Text,Text,Text,Text> {
    //默认读取：每行的行号作为key，每行的内容作为value
    //Map2_1 输出key：word;className;textName;          输出value：1
    //Combiner2_1 输出key：className;textName          输出value：word;num
    //reducer2_1 输出key：className;textName           输出value：word;freq

    private static Text index=new Text();

    @Override
    public void reduce(Text key,Iterable<Text> value,Context context) throws IOException,InterruptedException
    {
        int total_num=0;
        Vector<String> vector=new Vector<String >();
        for(Text v:value) {
            vector.add(v.toString());
            total_num+=Integer.parseInt(v.toString().split("&")[1]);
        }
        for (int i = 0; i < vector.size(); i++) {
            int num = Integer.parseInt(vector.get(i).split("&")[1]);
            double freq=((double)num)/((double)total_num);
            String word=vector.get(i).split("&")[0];
            index.set(word+"\t"+freq);
            context.write(key,index);
        }
    }
}
