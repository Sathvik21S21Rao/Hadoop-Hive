#!/bin/bash

for dist in {1..4}
do
    for agg in {1..2}
    do
        dir_name="dist_${dist}_agg_${agg}"

        mkdir -p "$dir_name"

        time hadoop jar StripesAggregation.jar StripesAggregation /B_input "/4d_output_stripes/$dir_name" /4d_input/top_freq_4a "$dist" "$agg"

        hdfs dfs -get "/4d_output_stripes/$dir_name/*" "./$dir_name"
    done
done

echo "All Hadoop jobs completed!"

