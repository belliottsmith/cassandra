/*
* Licensed to the Apache Software Foundation (ASF) under one
* or more contributor license agreements.  See the NOTICE file
* distributed with this work for additional information
* regarding copyright ownership.  The ASF licenses this file
* to you under the Apache License, Version 2.0 (the
* "License"); you may not use this file except in compliance
* with the License.  You may obtain a copy of the License at
*
*    http://www.apache.org/licenses/LICENSE-2.0
*
* Unless required by applicable law or agreed to in writing,
* software distributed under the License is distributed on an
* "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
* KIND, either express or implied.  See the License for the
* specific language governing permissions and limitations
* under the License.
*/
package org.apache.cassandra.stress;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.Writer;
import java.text.NumberFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.EnumMap;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.regex.Pattern;

public class PlotComparison
{

    static final Pattern header = Pattern.compile("total ops *, *adj row/s, *op/s, *pk/s, *row/s, *mean, *med, *.95, *.99, *.999, *max, *time, *stderr, *gc: \\#, *max ms, *sum ms, *sdv ms, *mb");
    private static final Pattern dataPattern = Pattern.compile("([0-9]+(\\.[0-9]+)?)(( *, *)[0-9]+(\\.[0-9]+)?){17}");
    static final int timeIndex = 11;

    static enum DataField
    {
        p50(6, "p50", DataType.Latency, 7, 0.3f),
        p95(7, "p95", DataType.Latency, 7, 0.4f),
        p99(8, "p99", DataType.Latency, 7, 0.5f),
        p999(9, "p999", DataType.Latency, 10, 0.6f),
        pMax(10, "pMax", DataType.Latency, 8, 0.8f),
        ops(2, "op/s", DataType.Throughput, 7, 0.5f),
        pks(3, "pk/s", DataType.Throughput, 7, 0.5f),
        rows(4, "row/s", DataType.Throughput, 7, 0.5f),
        gcmaxms(14, "max ms", DataType.GcTime, 7, 0.5f),
        gcsumms(15, "sum ms", DataType.GcTime, 7, 0.5f),
        gcmb(17, "mb", DataType.GcMb, 7, 0.5f);

        final int index;
        final String label;
        final DataType type;
        final int pointType;
        final double pointSize;

        DataField(int index, String label, DataType type, int pointType, double pointSize)
        {
            this.index = index;
            this.label = label;
            this.type = type;
            this.pointType = pointType;
            this.pointSize = pointSize;
        }
        static EnumSet<DataField> allOf(DataType type)
        {
            EnumSet<DataField> set = EnumSet.noneOf(DataField.class);
            for (DataField field : values())
                if(field.type == type)
                    set.add(field);
            return set;

        }
    }

    static enum DataType
    {
        Latency(true, "latency"),
        Throughput(false, "throughput"),
        GcTime(true, "ms"),
        GcMb(false, "mb");

        final boolean logscale;
        final String title;

        DataType(boolean logscale, String title)
        {
            this.logscale = logscale;
            this.title = title;
        }

        static DataType typeOf(Set<DataField> fields)
        {
            DataType type = null;
            for (DataField field : fields)
            {
                if (type == null)
                    type = field.type;
                else if (!type.equals(field.type))
                    throw new IllegalStateException();
            }
            return type;
        }
    }


    static enum Form
    {
        RAW(true, ""),
        NORMALISED(false, ""),
        NORMALISED_AND_SCALED(true, ""),
        CUMULATIVE(false, ""),
        CUMULATIVE_BY_WORK(false, "scaled by work")
        ;
        final boolean logscale;
        final String title;

        Form(boolean logscale, String title)
        {
            this.logscale = logscale;
            this.title = title;
        }
    }

    static enum GraphType
    {
        LINE, POINTS
    }

    public static void main(String[] args) throws IOException, InterruptedException
    {
        boolean tidy = true;
        Map<String, String> runDefs = new HashMap<>();
        for (String arg : args)
        {
            if (arg.equals("-tidy"))
            {
                tidy = false;
            }
            else
            {
                String[] split = arg.split(":");
                if (split.length != 2)
                {
                    System.out.println("usage: base:<file> name1:<file> [name2:<file> ...]");
                    System.exit(1);
                }
                runDefs.put(split[0], split[1]);
            }
        }
        if (!runDefs.containsKey("base"))
        {
            System.out.println("usage: base:<file> name1:<file> [name2:<file> ...]");
            System.exit(1);
        }

        MultiComparison comparisons = read(runDefs);

        int run = 1;
        for (String title : comparisons.titles())
        {
            Comparison comparison = comparisons.get(title);
            Multiplot plot = new Multiplot(comparison, new File("run" + run + ".plot"));
            plot.writeLine(HEADER, "@out", "run" + run, "@title", title);

            // first row: throughput
            plot.startPlot(0f, 0.65f, 0.3f, 0.32f, "Spot Performance Increase", "Throughput");
            realtimePlot(plot, DataField.allOf(DataType.Throughput), Form.NORMALISED, GraphType.LINE);
            plot.startPlot(0.3f, 0.65f, 0.5f, 0.32f, "Raw", "");
            realtimePlot(plot, DataField.allOf(DataType.Throughput), Form.RAW, GraphType.LINE);
            plot.startPlot(0.8f, 0.65f, 0.2f, 0.32f, "Raw", "");
            boxPlot(plot, DataField.allOf(DataType.Throughput), Form.RAW);

            // second row: latency
            plot.startPlot(0f, 0.325f, 0.3f, 0.333f, "Normalised And Scaled By Median", "Latency");
            realtimePlot(plot, DataField.allOf(DataType.Latency), Form.NORMALISED_AND_SCALED, GraphType.POINTS);
            plot.startPlot(.3f, 0.325f, 0.5f, 0.333f, "Raw", "");
            realtimePlot(plot, RealTimePlotSpec.cross(false, GraphType.LINE, EnumSet.of(Form.RAW), EnumSet.of(DataField.p50, DataField.p95, DataField.p99)), RealTimePlotSpec.cross(true, GraphType.POINTS, EnumSet.of(Form.RAW), EnumSet.of(DataField.p999, DataField.pMax)));
            plot.startPlot(0.8f, 0.325f, 0.2f, 0.32f, "", "");
            boxPlot(plot, DataField.allOf(DataType.Latency), Form.RAW);

//            // third row: gc
            plot.startPlot(0f, 0f, 0.25f, 0.33f, "Cumulative", "GC (ms)");
            realtimePlot(plot, RealTimePlotSpec.cross(true, GraphType.LINE, EnumSet.of(Form.CUMULATIVE, Form.CUMULATIVE_BY_WORK), EnumSet.of(DataField.gcsumms)));
            plot.startPlot(0.25f, 0f, 0.25f, 0.33f, "Cumulative", "GC (Mb)");
            realtimePlot(plot, RealTimePlotSpec.cross(true, GraphType.LINE, EnumSet.of(Form.CUMULATIVE, Form.CUMULATIVE_BY_WORK), EnumSet.of(DataField.gcmb)));
            plot.startPlot(.5f, 0f, 0.3f, 0.33f, "Raw", "GC (ms)");
            realtimePlot(plot, DataField.allOf(DataType.GcTime), Form.RAW, GraphType.LINE);
            plot.startPlot(.8f, 0f, 0.2f, 0.33f, "", "");
            boxPlot(plot, DataField.allOf(DataType.GcTime), Form.RAW);
            plot.plotWriter.write("unset multiplot");
            plot.runGnuplot();
            if (tidy)
                plot.tidy();
            run++;
        }
    }

    private static MultiComparison read(Map<String, String> runDefs) throws IOException
    {
        Map<String, MultiRun> runs = new LinkedHashMap<>();
        for (Map.Entry<String, String> runDef : runDefs.entrySet())
            runs.put(runDef.getKey(), read(runDef.getValue()));
        Set<String> titles = new LinkedHashSet<>();
        for (MultiRun run : runs.values())
            titles.addAll(run.titles());
        Set<String> runIds = new LinkedHashSet<>();
        runIds.add("base");
        runIds.addAll(runs.keySet());
        Map<String, Comparison> comparisons = new LinkedHashMap<>();
        for (String title : titles)
        {
            Map<String, Run> sameTitleRuns = new LinkedHashMap<>();
            for (String runId : runIds)
                sameTitleRuns.put(runId, runs.get(runId).get(title));
            comparisons.put(title, new Comparison(sameTitleRuns));
        }
        return new MultiComparison(comparisons);
    }

    private static MultiRun read(String path) throws IOException
    {
        // first index is time, remainder are ops, pks, rows, p50, p95, p99, p999
        EnumMap<DataField, DoubleAccumulator> accs = new EnumMap<>(DataField.class);
        DoubleAccumulator timeAcc = new DoubleAccumulator();
        for (DataField field : DataField.values())
            accs.put(field, new DoubleAccumulator());

        BufferedReader reader = new BufferedReader(new FileReader(path));
        try
        {
            final Map<String, Run> result = new LinkedHashMap<>();
            String line, prev = null, title = null;
            boolean inData = false;
            while ( null != (line = reader.readLine()) )
            {
                if (!inData)
                {
                    if (header.matcher(line).matches())
                    {
                        title = prev;
                        inData = true;
                    }
                    prev = line;
                    continue;
                }

                if (!dataPattern.matcher(line).matches())
                {
                    if (line.startsWith("Results:"))
                    {
                        EnumMap<DataField, Series> fieldResults = new EnumMap<>(DataField.class);
                        for (DataField field : accs.keySet())
                            fieldResults.put(field, new Series(accs.get(field).finish(true)));
                        result.put(title, new Run(fieldResults, timeAcc.finish(true)));
                        inData = false;
                        title = null;
                    }
                    continue;
                }

                String[] values = line.split(" *, *");
                for (DataField field : accs.keySet())
                    accs.get(field).add(Double.parseDouble(values[field.index]));
                timeAcc.add(Double.parseDouble(values[timeIndex]));
            }

            return new MultiRun(result);
        }
        finally
        {
            reader.close();
        }
    }

    static void realtimePlot(Multiplot plot, EnumSet<DataField> fields, Form form, GraphType graphType) throws IOException, InterruptedException
    {
        realtimePlot(plot, fields, form, graphType, false);
    }
    static void realtimePlot(Multiplot plot, EnumSet<DataField> fields, Form form, GraphType graphType, boolean printLabels) throws IOException, InterruptedException
    {
        realtimePlot(plot, Collections.singletonList(new RealTimePlotSpec(printLabels, graphType, form, DataType.typeOf(fields), fields)));
    }
    static class RealTimePlotSpec
    {
        final boolean printLabels;
        final GraphType graphType;
        final Form form;
        final DataType type;
        final EnumSet<DataField> fields;
        RealTimePlotSpec(boolean printLabels, GraphType graphType, Form form, DataType type, EnumSet<DataField> fields)
        {
            this.printLabels = printLabels;
            this.graphType = graphType;
            this.form = form;
            this.type = type;
            this.fields = fields;
        }
        static List<RealTimePlotSpec> cross(boolean printLabels, GraphType graphType, EnumSet<Form> forms, EnumSet<DataField> fields)
        {
            EnumMap<DataType, EnumSet<DataField>> types = new EnumMap<>(DataType.class);
            for (DataField field : fields)
            {
                EnumSet<DataField> set = types.get(field.type);
                if (set == null)
                    types.put(field.type, set = EnumSet.noneOf(DataField.class));
                set.add(field);
            }
            List<RealTimePlotSpec> r = new ArrayList<>();
            for (Form form : forms)
                for (DataType type : types.keySet())
                    r.add(new RealTimePlotSpec(printLabels, graphType, form, type, types.get(type)));
            return r;
        }
    }
    static void realtimePlot(Multiplot plot, List<RealTimePlotSpec> ... specs) throws IOException, InterruptedException
    {
        List<RealTimePlotSpec> combined = new ArrayList<>();
        for (List<RealTimePlotSpec> spec : specs)
            combined.addAll(spec);
        realtimePlot(plot, combined);
    }
    static void realtimePlot(Multiplot plot, List<RealTimePlotSpec> specs) throws IOException, InterruptedException
    {
        double maxX = 0;
        Map<Integer, Integer> axisLookup = new HashMap<>();
        Map<RealTimePlotSpec, String> filePrefixes = new HashMap<>();
        for (RealTimePlotSpec spec : specs)
        {
            WriteResult result = writeRealtimeDats(plot, spec.form, spec.type, spec.fields);
            maxX = Math.max(maxX, result.maxX);
            filePrefixes.put(spec, result.filePrefix);
            int scaleId = plot.scaleId(spec.type, spec.form);
            if (axisLookup.containsKey(scaleId))
                continue;
            if (axisLookup.size() == 2)
                throw new IllegalStateException("Too many conflicting scales");
            axisLookup.put(scaleId, axisLookup.size() + 1);
            plot.setScale("y", axisLookup.size(), spec.type, spec.form);
        }
//        plot.writeLine(REALTIME_SETUP_TEMPLATE, "@xlimit", String.format("%.2f", maxX + 1));

        plot.plotWriter.write("plot ");
        int run = 1;
        for (String key : plot.comparison.runIds())
        {
            int linestyle = 0;
            for (RealTimePlotSpec spec : specs)
            {
                // skip reprinting normalised realtime series
                if (spec.form == Form.CUMULATIVE_BY_WORK && key.equals("base"))
                    continue;
                int column = 2;
                for (DataField field : spec.fields)
                {
                    String title = "";
                    if (spec.printLabels)
                        title = key + " " + spec.form.title + " " + field.label;
                    String style;
                    if (spec.graphType == GraphType.POINTS)
                        style = "pt " + field.pointType + " ps " + field.pointSize;
                    else
                        style = "lw 2";
                    plot.writeLine(REALTIME_RUN_TEMPLATE,
                                   "@datfile", filePrefixes.get(spec) + run,
                                   "@colour", COLOURS[run - 1][linestyle],
                                   "@axes", "axes x1y" + axisLookup.get(plot.scaleId(spec.type, spec.form)),
                                   "@style", style,
                                   "@column", column++,
                                   "@graphtype", spec.graphType.name().toLowerCase(),
                                   "@title", title,
                                   "@run", run
                    );
                }
                linestyle++;
            }
            run++;
        }
        plot.plotWriter.write(";");
    }

    static class WriteResult
    {
        final double maxX;
        final String filePrefix;

        WriteResult(double maxX, String filePrefix)
        {
            this.maxX = maxX;
            this.filePrefix = filePrefix;
        }
    }
    static WriteResult writeRealtimeDats(Multiplot plot, Form form, DataType type, EnumSet<DataField> fields) throws IOException
    {
        String prefix = "realtime." + form + "." + fields.toString().replaceAll("\\[|\\]", "") + ".dat";
        Writer[] out = new Writer[plot.comparison.count()];
        for (int i = 0 ; i < out.length ; i++)
        {
            File file = new File(prefix + (i + 1));
            plot.dataFiles.add(file);
            out[i] = new BufferedWriter(new FileWriter(file));
            out[i].write("x ");
            for (DataField field : fields)
            {
                out[i].write(field.label);
                out[i].write(" ");
            }
            out[i].write("\n");
        }
        try
        {
            double maxX = 0;
            int i = 0;
            for (PartOfRun stats : plot.comparison.select(fields).get(form, plot.comparison).byRunId.values())
            {
                final NumberFormat nf = NumberFormat.getInstance();
                nf.setGroupingUsed(false);
                nf.setMaximumFractionDigits(2);
                for (int j = 0 ; j < stats.time.length ; j++)
                {
                    out[i].write(nf.format(stats.time[j]));
                    out[i].write(" ");
                    for (DataField field : fields)
                    {
                        out[i].write(nf.format(stats.data.get(field).values[j]));
                        out[i].write(" ");
                    }
                    out[i].write("\n");
                }
                maxX = Math.max(stats.time[stats.time.length - 1], maxX);
                i++;
            }
            return new WriteResult(maxX, prefix);
        }
        finally
        {
            for (Writer w : out)
                w.close();
        }
    }

    static void boxPlot(Multiplot plot, EnumSet<DataField> fields, Form form) throws IOException, InterruptedException
    {
        DataType type = DataType.typeOf(fields);
        String filePrefix = writeBoxplotDats(plot, form, fields);
        plot.writeLine(BOXPLOT_SETUP_TEMPLATE, "@xlimit", plot.comparison.count() * fields.size());
        plot.setScale("y", 2, type, form);

        plot.plotWriter.write("plot ");
        int i = 0;
        for (String key : plot.comparison.runIds())
        {
            plot.writeLine(BOXPLOT_RUN_TEMPLATE,
                           "@title", "",
                           "@datfile", filePrefix + (i + 1),
                           "@i2", i * 2 + 2,
                           "@i1", i * 2 + 1,
                           "@i", i + 1);
            i++;
        }
        plot.plotWriter.write(";");
    }

    static String writeBoxplotDats(Multiplot plot, Form form, EnumSet<DataField> fields) throws IOException
    {
        String prefix = "boxplot." + form + "." + fields.toString().replaceAll("\\[|\\]", "") + ".dat";
        Writer[] out = new Writer[plot.comparison.count()];
        for (int i = 0 ; i < out.length ; i++)
        {
            File file = new File(prefix + (i + 1));
            out[i] = new BufferedWriter(new FileWriter(file));
            out[i].write("x min d1 q1 med q3 d9 max xtic\n");
            plot.dataFiles.add(file);
        }
        PartialComparison cmp = plot.comparison.select(fields).get(form, plot.comparison);
        try
        {
            double x = 0.5d;
            for (DataField field : fields)
            {
                int o = 0;
                for (PartOfRun data : cmp.byRunId.values())
                {
                    out[o].write(data.data.get(field).boxPlotRow(x, o == 0 ? field.label : "") + "\n");
                    x += 1d;
                    o++;
                }
            }
        }
        finally
        {
            for (Writer w : out)
                w.close();
        }
        return prefix;
    }

    private static class Multiplot
    {
        final Comparison comparison;
        final List<File> dataFiles = new ArrayList<>();
        final BufferedWriter plotWriter;
        final File plotFile;

        private Multiplot(Comparison comparison, File plotFile) throws IOException
        {
            this.comparison = comparison;
            this.plotFile = plotFile;
            this.plotWriter = new BufferedWriter(new FileWriter(plotFile));
        }

        void runGnuplot() throws IOException, InterruptedException
        {
            plotWriter.close();
            int result = new ProcessBuilder().redirectError(new File("err"))
                                             .redirectOutput(new File("err"))
                                             .command("gnuplot", plotFile.getPath())
                                             .start().waitFor();
            if (result != 0)
            {
                System.out.println("gnuplot exited with error; failing without tidying up");
                System.exit(1);
            }
        }

        void startPlot(double hOffset, double vOffset, double hSize, double vSize, String title, String ylabel) throws IOException
        {
            startPlot(hOffset, vOffset, hSize, vSize, title, ylabel, "");
        }

        void startPlot(double hOffset, double vOffset, double hSize, double vSize, String title, String ylabel, String y2label) throws IOException
        {
            reset();
            plotWriter.write(String.format("set size %.2f,%.2f\nset origin %.2f,%.2f\n", hSize, vSize, hOffset, vOffset));
            plotWriter.write("set title \"" + title + "\" enhanced offset 0,-1\n");
            if (ylabel.isEmpty())
                plotWriter.write("unset ylabel\n");
            else
                plotWriter.write("set ylabel \"" + ylabel + "\" enhanced offset 2,0\n");
            if (y2label.isEmpty())
                plotWriter.write("unset y2label\n");
            else
                plotWriter.write("set y2label \"" + y2label + "\" enhanced offset -2,0\n");
        }

        void tidy()
        {
            for (File file : dataFiles)
                file.delete();
        }

        public int scaleId(DataType type, Form form)
        {
            if (type.logscale && form.logscale)
                return 10 + type.ordinal();
            else if (form == Form.NORMALISED)
                return 1;
            else
                return 20 + type.ordinal();
        }

        public void setScale(String direction, int num, DataType type, Form form) throws IOException
        {
            if (num < 1 || num > 2)
                throw new IllegalStateException();
            String scale = direction + (num == 1 ? "" : "2");
            writeLine("set @scaletics", "@scale", scale);
            if (type.logscale && form.logscale)
            {
                writeLine("set logscale @scale 2\n", "@scale", scale);
                writeLine("set @scalerange [1:*]\n", "@scale", scale);
                writeLine("set @scaletics autofreq", "@scale", scale);
            }
            else
            {
                writeLine("unset logscale @scale", "@scale", scale);
                if (form == Form.NORMALISED)
                {
                    writeLine("set @scalerange [*:*]", "@scale", scale);
                    writeLine("set @scaletics autofreq 0.2", "@scale", scale);
                    writeLine("set grid @scaletics", "@scale", scale);
                }
                else
                {
                    writeLine("set @scaletics autofreq", "@scale", scale);
                    writeLine("set @scalerange [*:*]", "@scale", scale);
                    writeLine("set @scaletics", "@scale", scale);
                }
            }
        }

        void writeLine(String template, Object... bindVarPairs) throws IOException
        {
            for (int i = 0 ; i < bindVarPairs.length ; i += 2)
            {
                template = template.replaceAll(bindVarPairs[i].toString(), bindVarPairs[i + 1].toString());
            }
            plotWriter.write(template);
            if (!template.endsWith("\n"))
                plotWriter.write("\n");
        }

        void reset() throws IOException
        {
            writeLine("set xtics\nunset y2tics\nunset grid\nunset ytics\nset xrange[*:*]");
            writeLine("unset xtics\nunset y2tics\nunset grid\nunset ytics\nset xrange[*:*]");
            writeLine("set key on left");
            writeLine("set style fill empty");
        }
    }

    private static class DoubleAccumulator
    {
        double[] values = new double[1024];
        int count = 0;
        void add(double value)
        {
            if (count == values.length)
                values = Arrays.copyOf(values, count * 2);
            values[count++] = value;
        }
        double[] finish(boolean reset)
        {
            double[] r = Arrays.copyOf(values, count);
            if (reset)
                count = 0;
            return r;
        }
        public void reset()
        {
            count = 0;
        }
    }

    private static class MultiComparison
    {
        final Map<String, Comparison> comparisons;
        private MultiComparison(Map<String, Comparison> comparisons)
        {
            this.comparisons = comparisons;
        }
        Iterable<String> titles()
        {
            return comparisons.keySet();
        }
        Comparison get(String title)
        {
            return comparisons.get(title);
        }
    }

    private static class Comparison
    {
        final Map<String, Run> byRunId;
        private Comparison(Map<String, Run> byRunId)
        {
            this.byRunId = byRunId;
        }
        PartialComparison select(DataField field, DataField ... fields)
        {
            return select(EnumSet.of(field, fields));
        }
        PartialComparison select(Set<DataField> fields)
        {
            Map<String, PartOfRun> result = new LinkedHashMap<>();
            for (Map.Entry<String, Run> e : byRunId.entrySet())
                result.put(e.getKey(), e.getValue().select(fields));
            return new PartialComparison(result);
        }
        int count()
        {
            return byRunId.size();
        }
        Collection<String> runIds()
        {
            return byRunId.keySet();
        }
    }

    private static class PartialComparison
    {
        final Map<String, PartOfRun> byRunId;
        private PartialComparison(Map<String, PartOfRun> byRunId)
        {
            this.byRunId = byRunId;
        }
        PartialComparison get(Form action, Comparison full)
        {
            if (action == Form.RAW)
                return this;
            Map<String, PartOfRun> result = new LinkedHashMap<>();
            for (Map.Entry<String, PartOfRun> e : byRunId.entrySet())
            {
                PartOfRun base = byRunId.get("base");
                switch (action)
                {
                    case CUMULATIVE:
                        result.put(e.getKey(), e.getValue().sum());
                        break;
                    case CUMULATIVE_BY_WORK:
                        Series throughput = full.select(DataField.pks).byRunId.get(e.getKey()).data.get(DataField.pks).sum();
                        Series baseThroughput = full.select(DataField.pks).byRunId.get("base").data.get(DataField.pks).sum();
                        Series scaleBy = throughput.divide(baseThroughput.values);
                        result.put(e.getKey(), e.getValue().sum().divide(scaleBy));
                        break;
                    case NORMALISED:
                        result.put(e.getKey(), e.getValue().merge(base, true, false));
                        break;
                    case NORMALISED_AND_SCALED:
                        result.put(e.getKey(), e.getValue().merge(base, true, true));
                        break;
                }
            }
            return new PartialComparison(result);
        }
    }

    private static class MultiRun
    {
        // title => Run
        final Map<String, Run> runs;
        private MultiRun(Map<String, Run> runs)
        {
            this.runs = runs;
        }
        Collection<String> titles()
        {
            return runs.keySet();
        }
        public Run get(String title)
        {
            return runs.get(title);
        }
    }

    private static class Run
    {
        final EnumMap<DataField, Series> data;
        final double[] time;
        private Run(EnumMap<DataField, Series> data, double[] time)
        {
            this.data = data;
            this.time = time;
        }

        PartOfRun select(Set<DataField> fields)
        {
            DataType type = DataType.typeOf(fields);
            EnumMap<DataField, Series> selection = new EnumMap<>(DataField.class);
            for (DataField field : fields)
                selection.put(field, data.get(field));
            return new PartOfRun(type, time, selection);
        }
    }

    private static class PartOfRun
    {
        final DataType datatype;
        final EnumMap<DataField, Series> data;
        final double[] time;
        private PartOfRun(DataType datatype, double[] time, EnumMap<DataField, Series> data)
        {
            this.datatype = datatype;
            this.data = data;
            this.time = time;
        }
        private PartOfRun merge(PartOfRun base, boolean normalise, boolean scaleByMedian)
        {
            DoubleAccumulator time = new DoubleAccumulator();
            EnumMap<DataField, DoubleAccumulator> vals = new EnumMap<>(DataField.class);
            for (DataField field : data.keySet())
                vals.put(field, new DoubleAccumulator());
            for (int i1 = 0, i2 = 0 ; i1 < this.time.length || i2 < base.time.length ; )
            {
                int c = cmp(i1, i2, this.time, base.time);
                if (c == 0)
                {
                    time.add(this.time[i1]);
                    for (DataField v : data.keySet())
                    {
                        double normalised = !normalise ? this.data.get(v).values[i1]
                                            : (this.data.get(v).values[i1] - base.data.get(v).values[i2]) / base.data.get(v).values[i2];
                        vals.get(v).add(!scaleByMedian ? normalised : this.data.get(v).med * (1d + normalised));
                    }
                    i1++;
                    i2++;
                }
                else if (c < 0)
                {
                    // TODO : possibly do something to print these neatly - maybe fill in a blank and print a separate series?
                    // scaling to huge/tiny throughput makes the graphs ugly, though
//                    time.add(this.time[i1]);
//                    for (DataField v : data.keySet())
//                    {
//                        double normalised = datatype == DataType.Latency ? normalise ? 2 : this.data.get(v).max : normalise ? this.data.get(v).min : -2;
//                        vals.get(v).add(!scaleByMedian ? normalised : this.data.get(v).med * (1d + normalised));
//                    }
                    i1++;
                }
                else
                {
//                    time.add(base.time[i2]);
//                    for (DataField v : data.keySet())
//                    {
//                        double normalised = datatype == DataType.Latency ? -2 : normalise ? 2 : this.data.get(v).max;
//                        vals.get(v).add(!scaleByMedian ? normalised : this.data.get(v).med * (1d + normalised));
//                    }
                    i2++;
                }
            }
            EnumMap<DataField, Series> normalised = new EnumMap<>(DataField.class);
            for (DataField v : vals.keySet())
                normalised.put(v, new Series(vals.get(v).finish(true)));
            return new PartOfRun(datatype, time.finish(true), normalised);
        }

        public PartOfRun sum()
        {
            EnumMap<DataField, Series> data = this.data.clone();
            for (DataField f : data.keySet())
                data.put(f, data.get(f).sum());
            return new PartOfRun(datatype, time, data);
        }

        public PartOfRun divide(Series series)
        {
            EnumMap<DataField, Series> data = this.data.clone();
            for (DataField f : data.keySet())
                data.put(f, data.get(f).divide(series.values));
            return new PartOfRun(datatype, time, data);
        }
    }

    // -1 means should print first series, 0 both, 1 second
    private static int cmp(int i1, int i2, double[] time1, double[] time2)
    {
        if (i1 == time1.length)
            return 1;
        if (i2 == time2.length)
            return -1;
        if (i1 + 1 == time1.length || i2 + 1 == time2.length)
        {
            if (i1 + 1 == time1.length && i2 + 1 == time2.length)
                return 0;
            if (i1 + 1 == time1.length)
                return time1[i1] >= time2[i2 + 1] ? 1 : -1;
            return time2[i2] >= time1[i1 + 1] ? -1 : 1;
        }
        if (time1[i1] < time2[i2 + 1] && time2[i2] < time1[i1 + 1])
            return 0;
        if (time1[i1] < time2[i2 + 1])
            return -1;
        return 1;
    }

    private static class Series
    {
        final double[] values;
        final double min, d1, q1, med, q3, d9, max;
        private Series(double[] values)
        {
            this.values = values;
            double[] v2 = values.clone();
            Arrays.sort(v2);
            min = v2[0];
            d1 = v2[(int) Math.floor(v2.length * (0.1d))];
            q1 = v2[(int) Math.floor(v2.length * (0.25d))];
            med = v2[(int) Math.floor(v2.length * (0.5d))];
            q3 = v2[(int) Math.floor(v2.length * (0.75d))];
            d9 = v2[(int) Math.floor(v2.length * (0.9d))];
            max = v2[v2.length - 1];
        }
        String boxPlotRow(double x, String xlabel)
        {
            return String.format("%.2f %.2f %.2f %.2f %.2f %.2f %.2f %.2f %s", x, min, d1, q1, med, q3, d9, max, xlabel);
        }
        public Series sum()
        {
            double sum = 0;
            double[] values = this.values.clone();
            for (int i = 0 ; i < values.length ; i++)
                values[i] = (sum += values[i]);
            return new Series(values);
        }
        public Series divide(double[] by)
        {
            double[] values = this.values.clone();
            for (int i = 0 ; i < values.length ; i++)
                values[i] = values[i] / by[i];
            return new Series(values);
        }
    }

    private static final String HEADER =
        "set terminal svg size 2560,1600 dynamic background rgb \"white\"\n" +
        "set output \"@out.svg\"\n" +
        "set multiplot title \"@title\"\n";

    private static String[][] COLOURS = new String[][]
    {
        new String[] { "#FFA000", "#B07000", "#804000", "402000" },
        new String[] { "#00A0FF", "#0070B0", "#004080", "002040" },
        new String[] { "#FFA0FF", "#B070B0", "#804080", "402040" }
    };

    private static final String REALTIME_RUN_TEMPLATE =
        "'@datfile' using 1:@column @axes with @graphtype title \"@title\" @style lc rgb \"@colour\", \\\n";

//    private static final String

    private static final String BOXPLOT_SETUP_TEMPLATE =
        "set bars 1\n" +
        "set xtics\n" +
        "set style fill empty\n" +
        "set boxwidth 0.5\n" +
        "set xrange[0:@xlimit]\n" +
        "set grid y2tics\n" +
        "set linetype 1 lc rgb \"#FFA000\" lw 1\n" +
        "set linetype 2 lc rgb \"#A06000\" lw 1\n" +
        "set linetype 3 lc rgb \"#00A0FF\" lw 1\n" +
        "set linetype 4 lc rgb \"#0060A0\" lw 1\n" +
        "set linetype 5 lc rgb \"#FFA0FF\" lw 1\n" +
        "set linetype 6 lc rgb \"#A060A0\" lw 1\n";

    private static final String BOXPLOT_RUN_TEMPLATE =
        "'@datfile' using 1:3:2:8:7:xticlabels(9) axes x1y2 lt @i1 title \"@title\" with candlesticks whiskerbars 0.25 fs transparent solid 0.5 border lt @i2, \\\n" +
        "'@datfile' using 1:4:3:7:6:xticlabels(9) axes x1y2 lt @i2 title \"\" with candlesticks, \\\n" +
        "'@datfile' using 1:5:5:5:5:xticlabels(9) axes x1y2 with candlesticks lt -1 notitle,\\\n";

}
