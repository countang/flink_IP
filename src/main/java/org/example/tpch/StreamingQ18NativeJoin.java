package org.example.tpch;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

public class StreamingQ18NativeJoin {
    public static void main(String[] args) throws Exception {
        // 1. 流式执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //env.setParallelism(24);
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);

        // 2. 数据输入目录和结果输出文件路径
        String dataDir    = "/home/countang/streaming-q18/data";
        String outputPath = "/home/countang/streaming-q18/output/q18_native.csv";

        // 3. 注册三张输入表（使用 '|' 作为分隔符）
        tEnv.executeSql(
            "CREATE TABLE customer (" +
            "  c_custkey INT," +
            "  c_name STRING," +
            "  c_address STRING," +
            "  c_nationkey INT," +
            "  c_phone STRING," +
            "  c_acctbal DOUBLE," +
            "  c_mktsegment STRING," +
            "  c_comment STRING" +
            ") WITH (" +
            "  'connector'            = 'filesystem'," +
            "  'path'                 = '" + dataDir + "/customer.csv'," +
            "  'format'               = 'csv'," +
            "  'csv.field-delimiter'  = '|'" +
            ")"
        );

        tEnv.executeSql(
            "CREATE TABLE orders (" +
            "  o_orderkey INT," +
            "  o_custkey INT," +
            "  o_orderstatus STRING," +
            "  o_totalprice DOUBLE," +
            "  o_orderdate DATE," +
            "  o_orderpriority STRING," +
            "  o_clerk STRING," +
            "  o_shippriority INT," +
            "  o_comment STRING" +
            ") WITH (" +
            "  'connector'            = 'filesystem'," +
            "  'path'                 = '" + dataDir + "/orders.csv'," +
            "  'format'               = 'csv'," +
            "  'csv.field-delimiter'  = '|'" +
            ")"
        );

        tEnv.executeSql(
            "CREATE TABLE lineitem (" +
            "  l_orderkey INT," +
            "  l_partkey INT," +
            "  l_suppkey INT," +
            "  l_linenumber INT," +
            "  l_quantity DOUBLE," +
            "  l_extendedprice DOUBLE," +
            "  l_discount DOUBLE," +
            "  l_tax DOUBLE," +
            "  l_returnflag STRING," +
            "  l_linestatus STRING," +
            "  l_shipdate DATE," +
            "  l_commitdate DATE," +
            "  l_receiptdate DATE," +
            "  l_shipinstruct STRING," +
            "  l_shipmode STRING," +
            "  l_comment STRING" +
            ") WITH (" +
            "  'connector'            = 'filesystem'," +
            "  'path'                 = '" + dataDir + "/lineitem.csv'," +
            "  'format'               = 'csv'," +
            "  'csv.field-delimiter'  = '|'" +
            ")"
        );

        // 4. 创建一个真正写到文件的 sink 表
        tEnv.executeSql(
            "CREATE TABLE file_sink (" +
            "  c_name STRING," +
            "  c_custkey INT," +
            "  o_orderkey INT," +
            "  o_orderdate DATE," +
            "  o_totalprice DOUBLE," +
            "  total_quantity DOUBLE" +
            ") WITH (" +
            "  'connector' = 'print'\n" +
            ")"
        );

        // 5. 执行 Q18，并把结果写入 file_sink
        String q18 =
            "INSERT INTO file_sink\n" +
            "SELECT\n" +
            "  c.c_name,\n" +
            "  c.c_custkey,\n" +
            "  o.o_orderkey,\n" +
            "  o.o_orderdate,\n" +
            "  o.o_totalprice,\n" +
            "  SUM(l.l_quantity) AS total_quantity\n" +
            "FROM customer AS c\n" +
            "JOIN orders AS o ON c.c_custkey = o.o_custkey\n" +
            "JOIN lineitem AS l ON o.o_orderkey = l.l_orderkey\n" +
            "WHERE o.o_orderkey IN (\n" +
            "  SELECT l_orderkey\n" +
            "  FROM lineitem\n" +
            "  GROUP BY l_orderkey\n" +
            "  HAVING SUM(l_quantity) > 300\n" +
            ")\n" +
            "GROUP BY\n" +
            "  c.c_name, c.c_custkey, o.o_orderkey, o.o_orderdate, o.o_totalprice\n" +
            "ORDER BY o.o_totalprice DESC, o.o_orderdate\n" +
            "LIMIT 100";

        tEnv.executeSql(q18);
    }
}
