0. codepath:

```
    https://github.com/sighingnow/GraphScope/tree/htap/gaia-runner
    https://github.com/v6d-io/v6d -b v0.3.21

    https://github.com/GraphScope/gstest 
```

1. build:

```
    cd GraphScope/interactive_engine/executor
    cargo build --bin gaia_runner

    cd v6d/
    git submodule update --init
    mkdir build
    cd build
    cmake ..
    make install arrow_fragment_test
```

2. start vineyardd

```
    vineyardd --socket=/tmp/vineyard.sock --meta=local
```

3. loading modern graph

```
    export GSTEST= .... where your gstest repo

    ./bin/arrow_fragment_test /tmp/vineyard.sock 1 "$HOME/gstest/modern_graph/knows.csv#header_row=true&src_label=person&dst_label=person&label=knows&delimiter=|" 1 "$HOME/gstest/modern_graph/person.csv#header_row=true&label=person&delimiter=|"

    ./bin/arrow_fragment_test /tmp/vineyard.sock \
        2 \
        "$GS_TEST/modern_graph/knows.csv#header_row=true&src_label=person&dst_label=person&label=knows&delimiter=|" \
        "$GS_TEST/modern_graph/created.csv#header_row=true&src_label=person&dst_label=software&label=created&delimiter=|" \
        2 \
        "$GS_TEST/modern_graph/person.csv#header_row=true&label=person&delimiter=|" \
        "$GS_TEST/modern_graph/software.csv#header_row=true&label=software&delimiter=|"
```

    the command above will generate a line like


```
         arrow_fragment_test.cc:37] Loaded graph to vineyard: 108570287117353174
```

    The object id will be used in step 4

4. run query

```
    export QUERY_PLAN=<Where your graphscope repo>/research/query_service/gremlin/compiler/plans/q2.plan

    # export GLOG_logtostderr=1
    # export GLOG_stderrthreshold=0
    export RUST_LOG=trace
    ./target/debug/gaia_runner --graph-name modern --partition-num 1 --worker-id 0 --graph-vineyard-object-id <the object id from step 3>
```





extra. generate query plan

3.0: build

```
    cd htap/src/research/query_service/gremlin/compiler
    mvn package -DskipTests=true
```

3.1. start compiler

```
java -cp ".:gremlin-server-plugin/target/gremlin-server-plugin-1.0-SNAPSHOT-jar-with-dependencies.jar:gremlin-server-plugin/target/classes" com.alibaba.graphscope.gaia.GremlinServiceMain
```

3.2. generate query plan

```
    export HTAP_ROOT=.....

    java -cp .:./benchmark-tool/target/benchmark-tool-1.0-SNAPSHOT-jar-with-dependencies.jar com.alibaba.graphscope.gaia.GeneratePlanBinaryTool conf/modern_queries ".txt" conf/modern_plans ".plan" 0.0.0.0 8182

    java -cp .:./benchmark-tool/target/benchmark-tool-1.0-SNAPSHOT-jar-with-dependencies.jar com.alibaba.graphscope.gaia.GeneratePlanBinaryTool $HTAP_ROOT/src/gremlin-queries/tpcc/tpcc_queries ".txt" $HTAP_ROOT/src/gremlin-queries/tpcc/tpcc_plans ".plan" 0.0.0.0 8182

    java -cp .:./benchmark-tool/target/benchmark-tool-1.0-SNAPSHOT-jar-with-dependencies.jar com.alibaba.graphscope.gaia.GeneratePlanBinaryTool /disk1/wanglei/htap/src/gremlin-queries/ldbc/queries ".txt" /disk1/wanglei/htap/src/gremlin-queries/ldbc/query_plans ".plan" 0.0.0.0 8182
```

