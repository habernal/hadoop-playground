/*
 * Copyright 2015
 * Ubiquitous Knowledge Processing (UKP) Lab
 * Technische Universität Darmstadt
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package de.tudarmstadt.ukp.dkpro.hadoop;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;
import java.util.Arrays;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Greps lines in text files given the regex; writes the mathing lines to the output file.
 *
 * @author Ivan Habernal
 */
public class Grep
        extends Configured
        implements Tool
{
    public static final String MAPRED_MAPPER_REGEX = "mapred.mapper.regex";

    public static void main(String[] args)
            throws Exception
    {
        ToolRunner.run(new Grep(), args);
    }

    @Override
    public int run(String[] args)
            throws Exception
    {
        org.apache.hadoop.conf.Configuration conf = getConf();
        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();

        System.out.println("Other args: " + Arrays.toString(otherArgs));

        Job job = Job.getInstance();
        job.setJarByClass(Grep.class);

        job.setJobName(Grep.class.getName());
        job.setMapperClass(GrepMapper.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(NullWritable.class);

        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        // comma-separated list of input files
        String commaSeparatedInputFiles = otherArgs[0];
        String outputPath = otherArgs[1];

        FileInputFormat.addInputPaths(job, commaSeparatedInputFiles);
        FileOutputFormat.setOutputPath(job, new Path(outputPath));

        // adding the regex
        String regex = otherArgs[2];
        job.getConfiguration().set(MAPRED_MAPPER_REGEX, regex);

        return job.waitForCompletion(true) ? 0 : 1;
    }

    public static class GrepMapper
            extends Mapper<LongWritable, Text, Text, NullWritable>
    {
        private Pattern pattern;

        @Override
        protected void setup(Context context)
                throws IOException, InterruptedException
        {
            pattern = Pattern.compile(context.getConfiguration().get(MAPRED_MAPPER_REGEX));
        }

        @Override
        public void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException
        {
            String line = value.toString();
            Matcher matcher = pattern.matcher(line);
            if (matcher.find()) {
                context.write(new Text(line), NullWritable.get());
            }
        }
    }
}
