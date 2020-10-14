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
public class Reduce2_2 extends Reducer<Text,Text,Text,Text>{
    //默认读取：每行的行号作为key，每行的内容作为value
    //Map2_2 输出key：className;textName;vector dimension          输出value：column_id;tf-idf
    //reducer2_2 输出key：className                   输出value：vector_of_tf_idf

    @Override
    public void reduce(Text key,Iterable<Text> value,Context context) throws IOException,InterruptedException
    {
        String className=key.toString().split("\t")[0];
        int dimension=Integer.parseInt(key.toString().split("\t")[2]);

        Vector<String> vector=new Vector<String>();
        for(Text v:value) {
            vector.add(v.toString());
        }
        Vector<String> vector_of_tf_idf=new Vector<String>();
        for(int i=0;i<dimension;i++){
            vector_of_tf_idf.add("0");
        }

        double total_tf_idf=0;
        for(int i=0;i<vector.size();i++){
            int col_id=Integer.parseInt(vector.get(i).split("\t")[0]);
            double tf_idf=Double.parseDouble(vector.get(i).split("\t")[1]);
            vector_of_tf_idf.set(col_id,String.valueOf(tf_idf));
            total_tf_idf+=tf_idf*tf_idf;
        }

        total_tf_idf=Math.sqrt(total_tf_idf);

        DecimalFormat d=new DecimalFormat("#.000000");
        double standardTfIdf;
        String OutKey1="";
        String Outkey2="";
        boolean first=true;
        for (int i=0;i<dimension;i++){
            standardTfIdf=Double.parseDouble(vector_of_tf_idf.get(i));
            if(standardTfIdf>0){
                standardTfIdf/=total_tf_idf;
                if(first){
                    first=false;
                }
                else{
                    OutKey1+=",";
                    Outkey2+=",";
                }
                OutKey1+=d.format(standardTfIdf);
                Outkey2+=i;
            }
        }
        context.write(new Text(className),new Text(OutKey1+"&"+Outkey2));
    }
}