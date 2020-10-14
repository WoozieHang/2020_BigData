import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;

import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class Main {
    public static void main(String[] argv) throws Exception {
        if (argv.length < 3) {
            System.err.println("Usage: feature selection <in> <out>");
            System.exit(2);
        }
        else if (argv.length == 3) {
            Configuration conf = new Configuration();
            Job job1_1 = new Job(conf, "feature selection - frequency count for each word in each class");

            job1_1.setJarByClass(Main.class);

            job1_1.setMapperClass(Map1_1.class);
            job1_1.setMapOutputKeyClass(Text.class);
            job1_1.setMapOutputValueClass(Text.class);

            job1_1.setCombinerClass(Combiner1_1.class);

            job1_1.setReducerClass(Reduce1_1.class);
            job1_1.setOutputKeyClass(Text.class);
            job1_1.setOutputValueClass(Text.class);

            NonSplittableFileInputFormat.addInputPath(job1_1, new Path(argv[1]));
            NonSplittableFileInputFormat.setInputDirRecursive(job1_1, true);
            FileOutputFormat.setOutputPath(job1_1, new Path(argv[2] + "/task1/step1"));
            job1_1.addCacheFile(new Path(argv[0]).toUri());

            if (job1_1.waitForCompletion(true)) {
                Job job1_2 = new Job(conf, "feature selection - information entropy computation for each word");

                job1_2.setJarByClass(Main.class);

                job1_2.setMapperClass(Map1_2.class);
                job1_2.setMapOutputKeyClass(Text.class);
                job1_2.setMapOutputValueClass(Text.class);

                //job1_2.setCombinerClass(Combiner1_2.class);

                job1_2.setReducerClass(Reduce1_2.class);
                job1_2.setOutputKeyClass(Text.class);
                job1_2.setOutputValueClass(Text.class);

                FileInputFormat.addInputPath(job1_2, new Path(argv[2] + "/task1/step1/part-r-00000"));
                FileOutputFormat.setOutputPath(job1_2, new Path(argv[2] + "/task1/step2"));
                if (job1_2.waitForCompletion(true)) {
                    Job job1_3 = new Job(conf, "feature selection - information entropy sort");

                    job1_3.setJarByClass(Main.class);

                    job1_3.setMapperClass(Map1_3.class);
                    job1_3.setMapOutputKeyClass(DoubleWritable.class);
                    job1_3.setMapOutputValueClass(Text.class);


                    job1_3.setReducerClass(Reduce1_3.class);
                    job1_3.setOutputKeyClass(DoubleWritable.class);
                    job1_3.setOutputValueClass(Text.class);

                    FileInputFormat.addInputPath(job1_3, new Path(argv[2] + "/task1/step2/part-r-00000"));
                    FileOutputFormat.setOutputPath(job1_3, new Path(argv[2] + "/task1/step3"));
                    if (job1_3.waitForCompletion(true)) {
                        Job job1_4 = new Job(conf, "feature selection - formation");

                        job1_4.setJarByClass(Main.class);

                        job1_4.setMapperClass(Map1_4.class);
                        job1_4.setMapOutputKeyClass(Text.class);
                        job1_4.setMapOutputValueClass(Text.class);


                        job1_4.setReducerClass(Reduce1_4.class);
                        job1_4.setOutputKeyClass(Text.class);
                        job1_4.setOutputValueClass(Text.class);
                        NonSplittableFileInputFormat.addInputPath(job1_4, new Path(argv[2] + "/task1/step3/part-r-00000"));
                        FileOutputFormat.setOutputPath(job1_4, new Path(argv[2] + "/task1/step4"));
                        if (job1_4.waitForCompletion(true)) {
                            Job job2_1 = new Job(conf, "feature vector compute - word frequency count for every text");
                            job2_1.setJarByClass(Main.class);
                            job2_1.setMapperClass(Map2_1.class);
                            job2_1.setMapOutputKeyClass(Text.class);
                            job2_1.setMapOutputValueClass(Text.class);
                            job2_1.setCombinerClass(Combiner2_1.class);
                            job2_1.setReducerClass(Reduce2_1.class);
                            job2_1.setOutputKeyClass(Text.class);
                            job2_1.setOutputValueClass(Text.class);
                            NonSplittableFileInputFormat.addInputPath(job2_1, new Path(argv[1]));
                            NonSplittableFileInputFormat.setInputDirRecursive(job2_1, true);
                            FileOutputFormat.setOutputPath(job2_1, new Path(argv[2] + "/task2/step1"));
                            if (job2_1.waitForCompletion(true)) {
                                Job job2_2 = new Job(conf, "feature vector compute - compute vector of tf-idf for every text");
                                job2_2.setJarByClass(Main.class);
                                job2_2.setMapperClass(Map2_2.class);
                                job2_2.setMapOutputKeyClass(Text.class);
                                job2_2.setMapOutputValueClass(Text.class);
                                job2_2.setReducerClass(Reduce2_2.class);
                                job2_2.setOutputKeyClass(Text.class);
                                job2_2.setOutputValueClass(Text.class);
                                FileInputFormat.addInputPath(job2_2, new Path(argv[2] + "/task2/step1/part-r-00000"));
                                FileOutputFormat.setOutputPath(job2_2, new Path(argv[2] + "/task2/step2"));
                                job2_2.addCacheFile(new Path(argv[2] + "/task1/step4/part-r-00000").toUri());
                                if (job2_2.waitForCompletion(true)) {
                                    Job job3_1 = new Job(conf, "Machine Learning - get Training Set");
                                    job3_1.setJarByClass(Main.class);
                                    job3_1.setMapperClass(Map3_1.class);
                                    job3_1.setMapOutputKeyClass(Text.class);
                                    job3_1.setMapOutputValueClass(Text.class);
                                    job3_1.setReducerClass(Reduce3_1.class);
                                    job3_1.setOutputKeyClass(Text.class);
                                    job3_1.setOutputValueClass(Text.class);
                                    FileInputFormat.addInputPath(job3_1, new Path(argv[2] + "/task2/step2/part-r-00000"));
                                    FileOutputFormat.setOutputPath(job3_1, new Path(argv[2] + "/task3/step1"));
                                    if (job3_1.waitForCompletion(true)) {
                                        Job job3_2 = new Job(conf, "Machine Learning - get Test Set");
                                        job3_2.setJarByClass(Main.class);
                                        job3_2.setMapperClass(Map3_2.class);
                                        job3_2.setMapOutputKeyClass(Text.class);
                                        job3_2.setMapOutputValueClass(Text.class);
                                        job3_2.setReducerClass(Reduce3_2.class);
                                        job3_2.setOutputKeyClass(Text.class);
                                        job3_2.setOutputValueClass(Text.class);
                                        FileInputFormat.addInputPath(job3_2, new Path(argv[2] + "/task2/step2/part-r-00000"));
                                        FileOutputFormat.setOutputPath(job3_2, new Path(argv[2] + "/task3/step2"));
                                        if (job3_2.waitForCompletion(true)) {
                                            Job job3_3 = new Job(conf, "Machine Learning - KNN");
                                            job3_3.setJarByClass(Main.class);
                                            job3_3.setMapperClass(Map3_3.class);
                                            job3_3.setMapOutputKeyClass(Text.class);
                                            job3_3.setMapOutputValueClass(Text.class);
                                            job3_3.setReducerClass(Reduce3_3.class);
                                            job3_3.setOutputKeyClass(Text.class);
                                            job3_3.setOutputValueClass(Text.class);
                                            FileInputFormat.addInputPath(job3_3, new Path(argv[2] + "/task3/step2/part-r-00000"));
                                            FileOutputFormat.setOutputPath(job3_3, new Path(argv[2] + "/task3/step3"));
                                            job3_3.addCacheFile(new Path(argv[2] + "/task3/step1/part-r-00000").toUri());
                                            if (job3_3.waitForCompletion(true)) {
                                                Job job3_4 = new Job(conf, "Machine Learning - Bayes preparation");
                                                job3_4.setJarByClass(Main.class);
                                                job3_4.setMapperClass(Map3_4.class);
                                                job3_4.setMapOutputKeyClass(Text.class);
                                                job3_4.setMapOutputValueClass(Text.class);
                                                job3_4.setReducerClass(Reduce3_4.class);
                                                job3_4.setOutputKeyClass(Text.class);
                                                job3_4.setOutputValueClass(Text.class);
                                                FileInputFormat.addInputPath(job3_4, new Path(argv[2] + "/task3/step1/part-r-00000"));
                                                FileOutputFormat.setOutputPath(job3_4, new Path(argv[2] + "/task3/step4"));
                                                if (job3_4.waitForCompletion(true)) {
                                                    Job job3_5 = new Job(conf, "Machine Learning - Bayes compute");
                                                    job3_5.setJarByClass(Main.class);
                                                    job3_5.setMapperClass(Map3_5.class);
                                                    job3_5.setMapOutputKeyClass(Text.class);
                                                    job3_5.setMapOutputValueClass(Text.class);
                                                    job3_5.setReducerClass(Reduce3_5.class);
                                                    job3_5.setOutputKeyClass(Text.class);
                                                    job3_5.setOutputValueClass(Text.class);
                                                    FileInputFormat.addInputPath(job3_5, new Path(argv[2] + "/task3/step2/part-r-00000"));
                                                    FileOutputFormat.setOutputPath(job3_5, new Path(argv[2] + "/task3/step5"));
                                                    job3_5.addCacheFile(new Path(argv[2] + "/task3/step4/part-r-00000").toUri());
                                                    if (job3_5.waitForCompletion(true)) {
                                                        Job job4_1 = new Job(conf, "result compute - knn");
                                                        job4_1.setJarByClass(Main.class);
                                                        job4_1.setMapperClass(Map4.class);
                                                        job4_1.setMapOutputKeyClass(Text.class);
                                                        job4_1.setMapOutputValueClass(Text.class);
                                                        job4_1.setReducerClass(Reduce4.class);
                                                        job4_1.setOutputKeyClass(Text.class);
                                                        job4_1.setOutputValueClass(Text.class);
                                                        FileInputFormat.addInputPath(job4_1, new Path(argv[2] + "/task3/step3/part-r-00000"));
                                                        FileOutputFormat.setOutputPath(job4_1, new Path(argv[2] + "/task4/step1"));
                                                        if (job4_1.waitForCompletion(true)) {
                                                            Job job4_2 = new Job(conf, "result compute - bayes");
                                                            job4_2.setJarByClass(Main.class);
                                                            job4_2.setMapperClass(Map4.class);
                                                            job4_2.setMapOutputKeyClass(Text.class);
                                                            job4_2.setMapOutputValueClass(Text.class);
                                                            job4_2.setReducerClass(Reduce4.class);
                                                            job4_2.setOutputKeyClass(Text.class);
                                                            job4_2.setOutputValueClass(Text.class);
                                                            FileInputFormat.addInputPath(job4_2, new Path(argv[2] + "/task3/step5/part-r-00000"));
                                                            FileOutputFormat.setOutputPath(job4_2, new Path(argv[2] + "/task4/step2"));
                                                            System.exit(job4_2.waitForCompletion(true) ? 0 : 1);
                                                        } else
                                                            System.exit(1);
                                                    } else
                                                        System.exit(1);
                                                } else
                                                    System.exit(1);
                                            } else
                                                System.exit(1);
                                        } else
                                            System.exit(1);
                                    } else
                                        System.exit(1);
                                } else
                                    System.exit(1);
                            } else
                                System.exit(1);
                        } else
                            System.exit(1);
                    } else
                        System.exit(1);
                } else
                    System.exit(1);
            } else
                System.exit(1);
        }
        else{
            Configuration conf = new Configuration();
            if(argv[3].equals("1")){
                if(argv[4].equals("1")){
                    Job job1_1 = new Job(conf, "feature selection - frequency count for each word in each class");

                    job1_1.setJarByClass(Main.class);

                    job1_1.setMapperClass(Map1_1.class);
                    job1_1.setMapOutputKeyClass(Text.class);
                    job1_1.setMapOutputValueClass(Text.class);

                    job1_1.setCombinerClass(Combiner1_1.class);

                    job1_1.setReducerClass(Reduce1_1.class);
                    job1_1.setOutputKeyClass(Text.class);
                    job1_1.setOutputValueClass(Text.class);

                    NonSplittableFileInputFormat.addInputPath(job1_1, new Path(argv[1]));
                    NonSplittableFileInputFormat.setInputDirRecursive(job1_1, true);
                    FileOutputFormat.setOutputPath(job1_1, new Path(argv[2] + "/task1/step1"));
                    job1_1.addCacheFile(new Path(argv[0]).toUri());
                    System.exit(job1_1.waitForCompletion(true) ? 0 : 1);
                }
                else if(argv[4].equals("2")){
                    Job job1_2 = new Job(conf, "feature selection - information entropy computation for each word");

                    job1_2.setJarByClass(Main.class);

                    job1_2.setMapperClass(Map1_2.class);
                    job1_2.setMapOutputKeyClass(Text.class);
                    job1_2.setMapOutputValueClass(Text.class);

                    //job1_2.setCombinerClass(Combiner1_2.class);

                    job1_2.setReducerClass(Reduce1_2.class);
                    job1_2.setOutputKeyClass(Text.class);
                    job1_2.setOutputValueClass(Text.class);

                    FileInputFormat.addInputPath(job1_2, new Path(argv[2] + "/task1/step1/part-r-00000"));
                    FileOutputFormat.setOutputPath(job1_2, new Path(argv[2] + "/task1/step2"));
                    System.exit(job1_2.waitForCompletion(true) ? 0 : 1);
                }
                else if(argv[4].equals("3")){
                    Job job1_3 = new Job(conf, "feature selection - information entropy sort");

                    job1_3.setJarByClass(Main.class);

                    job1_3.setMapperClass(Map1_3.class);
                    job1_3.setMapOutputKeyClass(DoubleWritable.class);
                    job1_3.setMapOutputValueClass(Text.class);


                    job1_3.setReducerClass(Reduce1_3.class);
                    job1_3.setOutputKeyClass(DoubleWritable.class);
                    job1_3.setOutputValueClass(Text.class);

                    FileInputFormat.addInputPath(job1_3, new Path(argv[2] + "/task1/step2/part-r-00000"));
                    FileOutputFormat.setOutputPath(job1_3, new Path(argv[2] + "/task1/step3"));
                    System.exit(job1_3.waitForCompletion(true) ? 0 : 1);
                }
                else {
                    Job job1_4 = new Job(conf, "feature selection - formation");

                    job1_4.setJarByClass(Main.class);

                    job1_4.setMapperClass(Map1_4.class);
                    job1_4.setMapOutputKeyClass(Text.class);
                    job1_4.setMapOutputValueClass(Text.class);


                    job1_4.setReducerClass(Reduce1_4.class);
                    job1_4.setOutputKeyClass(Text.class);
                    job1_4.setOutputValueClass(Text.class);
                    NonSplittableFileInputFormat.addInputPath(job1_4, new Path(argv[2] + "/task1/step3/part-r-00000"));
                    FileOutputFormat.setOutputPath(job1_4, new Path(argv[2] + "/task1/step4"));
                    System.exit(job1_4.waitForCompletion(true) ? 0 : 1);
                }
            }
            else if(argv[3].equals("2")){
                if(argv[4].equals("1")){
                    Job job2_1 = new Job(conf, "feature vector compute - word frequency count for every text");
                    job2_1.setJarByClass(Main.class);
                    job2_1.setMapperClass(Map2_1.class);
                    job2_1.setMapOutputKeyClass(Text.class);
                    job2_1.setMapOutputValueClass(Text.class);
                    job2_1.setCombinerClass(Combiner2_1.class);
                    job2_1.setReducerClass(Reduce2_1.class);
                    job2_1.setOutputKeyClass(Text.class);
                    job2_1.setOutputValueClass(Text.class);
                    NonSplittableFileInputFormat.addInputPath(job2_1, new Path(argv[1]));
                    NonSplittableFileInputFormat.setInputDirRecursive(job2_1, true);
                    FileOutputFormat.setOutputPath(job2_1, new Path(argv[2] + "/task2/step1"));
                    System.exit(job2_1.waitForCompletion(true) ? 0 : 1);
                }
                else {
                    Job job2_2 = new Job(conf, "feature vector compute - compute vector of tf-idf for every text");
                    job2_2.setJarByClass(Main.class);
                    job2_2.setMapperClass(Map2_2.class);
                    job2_2.setMapOutputKeyClass(Text.class);
                    job2_2.setMapOutputValueClass(Text.class);
                    job2_2.setReducerClass(Reduce2_2.class);
                    job2_2.setOutputKeyClass(Text.class);
                    job2_2.setOutputValueClass(Text.class);
                    FileInputFormat.addInputPath(job2_2, new Path(argv[2] + "/task2/step1/part-r-00000"));
                    FileOutputFormat.setOutputPath(job2_2, new Path(argv[2] + "/task2/step2"));
                    job2_2.addCacheFile(new Path(argv[2] + "/task1/step4/part-r-00000").toUri());
                    System.exit(job2_2.waitForCompletion(true) ? 0 : 1);
                }
            }
            else if(argv[3].equals("3")){
                if(argv[4].equals("1")){
                    Job job3_1 = new Job(conf, "Machine Learning - get Training Set");
                    job3_1.setJarByClass(Main.class);
                    job3_1.setMapperClass(Map3_1.class);
                    job3_1.setMapOutputKeyClass(Text.class);
                    job3_1.setMapOutputValueClass(Text.class);
                    job3_1.setReducerClass(Reduce3_1.class);
                    job3_1.setOutputKeyClass(Text.class);
                    job3_1.setOutputValueClass(Text.class);
                    FileInputFormat.addInputPath(job3_1, new Path(argv[2] + "/task2/step2/part-r-00000"));
                    FileOutputFormat.setOutputPath(job3_1, new Path(argv[2] + "/task3/step1"));
                    System.exit(job3_1.waitForCompletion(true) ? 0 : 1);
                }
                else if(argv[4].equals("2")){
                    Job job3_2 = new Job(conf, "Machine Learning - get Test Set");
                    job3_2.setJarByClass(Main.class);
                    job3_2.setMapperClass(Map3_2.class);
                    job3_2.setMapOutputKeyClass(Text.class);
                    job3_2.setMapOutputValueClass(Text.class);
                    job3_2.setReducerClass(Reduce3_2.class);
                    job3_2.setOutputKeyClass(Text.class);
                    job3_2.setOutputValueClass(Text.class);
                    FileInputFormat.addInputPath(job3_2, new Path(argv[2] + "/task2/step2/part-r-00000"));
                    FileOutputFormat.setOutputPath(job3_2, new Path(argv[2] + "/task3/step2"));
                    System.exit(job3_2.waitForCompletion(true) ? 0 : 1);
                }
                else if(argv[4].equals("3")){
                    Job job3_3 = new Job(conf, "Machine Learning - KNN");
                    job3_3.setJarByClass(Main.class);
                    job3_3.setMapperClass(Map3_3.class);
                    job3_3.setMapOutputKeyClass(Text.class);
                    job3_3.setMapOutputValueClass(Text.class);
                    job3_3.setReducerClass(Reduce3_3.class);
                    job3_3.setOutputKeyClass(Text.class);
                    job3_3.setOutputValueClass(Text.class);
                    FileInputFormat.addInputPath(job3_3, new Path(argv[2] + "/task3/step2/part-r-00000"));
                    FileOutputFormat.setOutputPath(job3_3, new Path(argv[2] + "/task3/step3"));
                    job3_3.addCacheFile(new Path(argv[2] + "/task3/step1/part-r-00000").toUri());
                    System.exit(job3_3.waitForCompletion(true) ? 0 : 1);
                }
                else if(argv[4].equals("4")){
                    Job job3_4 = new Job(conf, "Machine Learning - Bayes preparation");
                    job3_4.setJarByClass(Main.class);
                    job3_4.setMapperClass(Map3_4.class);
                    job3_4.setMapOutputKeyClass(Text.class);
                    job3_4.setMapOutputValueClass(Text.class);
                    job3_4.setReducerClass(Reduce3_4.class);
                    job3_4.setOutputKeyClass(Text.class);
                    job3_4.setOutputValueClass(Text.class);
                    FileInputFormat.addInputPath(job3_4, new Path(argv[2] + "/task3/step1/part-r-00000"));
                    FileOutputFormat.setOutputPath(job3_4, new Path(argv[2] + "/task3/step4"));
                    System.exit(job3_4.waitForCompletion(true) ? 0 : 1);
                }
                else{
                    Job job3_5 = new Job(conf, "Machine Learning - Bayes compute");
                    job3_5.setJarByClass(Main.class);
                    job3_5.setMapperClass(Map3_5.class);
                    job3_5.setMapOutputKeyClass(Text.class);
                    job3_5.setMapOutputValueClass(Text.class);
                    job3_5.setReducerClass(Reduce3_5.class);
                    job3_5.setOutputKeyClass(Text.class);
                    job3_5.setOutputValueClass(Text.class);
                    FileInputFormat.addInputPath(job3_5, new Path(argv[2] + "/task3/step2/part-r-00000"));
                    FileOutputFormat.setOutputPath(job3_5, new Path(argv[2] + "/task3/step5"));
                    job3_5.addCacheFile(new Path(argv[2] + "/task3/step4/part-r-00000").toUri());
                    System.exit(job3_5.waitForCompletion(true) ? 0 : 1);
                }
            }
            else {
                Job job4_1 = new Job(conf, "result compute - knn");
                job4_1.setJarByClass(Main.class);
                job4_1.setMapperClass(Map4.class);
                job4_1.setMapOutputKeyClass(Text.class);
                job4_1.setMapOutputValueClass(Text.class);
                job4_1.setReducerClass(Reduce4.class);
                job4_1.setOutputKeyClass(Text.class);
                job4_1.setOutputValueClass(Text.class);
                FileInputFormat.addInputPath(job4_1, new Path(argv[2] + "/task3/step3/part-r-00000"));
                FileOutputFormat.setOutputPath(job4_1, new Path(argv[2] + "/task4/step1"));
                if (job4_1.waitForCompletion(true)) {
                    Job job4_2 = new Job(conf, "result compute - bayes");
                    job4_2.setJarByClass(Main.class);
                    job4_2.setMapperClass(Map4.class);
                    job4_2.setMapOutputKeyClass(Text.class);
                    job4_2.setMapOutputValueClass(Text.class);
                    job4_2.setReducerClass(Reduce4.class);
                    job4_2.setOutputKeyClass(Text.class);
                    job4_2.setOutputValueClass(Text.class);
                    FileInputFormat.addInputPath(job4_2, new Path(argv[2] + "/task3/step5/part-r-00000"));
                    FileOutputFormat.setOutputPath(job4_2, new Path(argv[2] + "/task4/step2"));
                    System.exit(job4_2.waitForCompletion(true) ? 0 : 1);
                }
                else
                    System.exit(1);
            }

        }
    }
}
