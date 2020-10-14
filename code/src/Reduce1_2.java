import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.text.DecimalFormat;
import java.util.Vector;

public class Reduce1_2 extends Reducer<Text,Text,Text,Text>{
    //默认读取：每行的行号作为key，每行的内容作为value
    //Map1_2 输出key：词语             输出value：次数 for certain class
    //reducer1_2 输出key：词语                    输出value：[information entropy][\t][total mail num]
    private static Text word=new Text();
    @Override
    public void reduce(Text key,Iterable<Text> value,Context context) throws IOException,InterruptedException
    {

        Vector<String> vector=new Vector<String >();
        int total_num=0;

        for(Text v:value) {
            total_num+=Integer.parseInt(v.toString());
            vector.add(v.toString());
        }

        if(total_num>2) {
            double entropy = 0;
            double tmp = 0;
            for (int i = 0; i < vector.size(); i++) {
                int a = Integer.parseInt(vector.get(i));
                tmp = ((double) (a)) / ((double) (total_num));
                entropy += -1 * (tmp * Math.log(tmp));
            }

            word.set(key.toString());
            context.write(word, new Text(entropy+"\t"+total_num));
        }

    }
}

