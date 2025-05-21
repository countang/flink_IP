package org.example.tpch;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.*;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.KeyedCoProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.core.fs.FileSystem;

public class StreamingQ18HandWritten {
    public static void main(String[] args) throws Exception {
        // 1. 创建执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //env.setParallelism(24);

        String dataDir = "/home/countang/streaming-q18/data";
        String outputPath = "/home/countang/streaming-q18/output/q18_handwritten.csv";

        // 2. 读取并解析 CSV
        DataStream<Customer> customers = env
            .readTextFile(dataDir + "/customer.csv")
            .map(new ParseCustomer());

        DataStream<Order> orders = env
            .readTextFile(dataDir + "/orders.csv")
            .map(new ParseOrder());

        DataStream<LineItem> lineItems = env
            .readTextFile(dataDir + "/lineitem.csv")
            .map(new ParseLineItem());

        // 3. 聚合：计算每个 orderKey 的总 quantity，并过滤 >300
        DataStream<OrderQty> orderQty = lineItems
            .keyBy(LineItem::getOrderKey)
            .sum("quantity")  // 依赖字段名 quantity
            .filter(li -> li.getQuantity() > 300)
            .map(li -> new OrderQty(li.getOrderKey(), li.getQuantity()));

        // 4. Orders 与 orderQty 手写 Join
        DataStream<EnrichedOrder> enriched = orders
            .keyBy(Order::getOrderKey)
            .connect(orderQty.keyBy(OrderQty::getOrderKey))
            .process(new KeyedCoProcessFunction<Integer, Order, OrderQty, EnrichedOrder>() {
                // 缓存两侧事件
                private transient java.util.HashMap<Integer, Order> orderMap;
                private transient java.util.HashMap<Integer, OrderQty> qtyMap;

                @Override
                public void open(org.apache.flink.configuration.Configuration cfg) {
                    orderMap = new java.util.HashMap<>();
                    qtyMap = new java.util.HashMap<>();
                }

                @Override
                public void processElement1(Order o, Context ctx, Collector<EnrichedOrder> out) {
                    orderMap.put(o.getOrderKey(), o);
                    OrderQty q = qtyMap.get(o.getOrderKey());
                    if (q != null) {
                        out.collect(new EnrichedOrder(o, q.getQuantity()));
                        // 清理状态
                        orderMap.remove(o.getOrderKey());
                        qtyMap.remove(o.getOrderKey());
                    }
                }

                @Override
                public void processElement2(OrderQty q, Context ctx, Collector<EnrichedOrder> out) {
                    qtyMap.put(q.getOrderKey(), q);
                    Order o = orderMap.get(q.getOrderKey());
                    if (o != null) {
                        out.collect(new EnrichedOrder(o, q.getQuantity()));
                        orderMap.remove(q.getOrderKey());
                        qtyMap.remove(q.getOrderKey());
                    }
                }
            });

        // 5. Customer 与 EnrichedOrder 手写 Join
        DataStream<ResultRecord> result = enriched
            .keyBy(e -> e.getOrder().getCustKey())
            .connect(customers.keyBy(Customer::getCustKey))
            .process(new KeyedCoProcessFunction<Integer, EnrichedOrder, Customer, ResultRecord>() {
                private transient java.util.HashMap<Integer, EnrichedOrder> enrichedMap;
                private transient java.util.HashMap<Integer, Customer> custMap;

                @Override
                public void open(org.apache.flink.configuration.Configuration cfg) {
                    enrichedMap = new java.util.HashMap<>();
                    custMap = new java.util.HashMap<>();
                }

                @Override
                public void processElement1(EnrichedOrder e, Context ctx, Collector<ResultRecord> out) {
                    enrichedMap.put(e.getOrder().getCustKey(), e);
                    Customer c = custMap.get(e.getOrder().getCustKey());
                    if (c != null) {
                        out.collect(new ResultRecord(c, e));
                        enrichedMap.remove(e.getOrder().getCustKey());
                        custMap.remove(e.getOrder().getCustKey());
                    }
                }

                @Override
                public void processElement2(Customer c, Context ctx, Collector<ResultRecord> out) {
                    custMap.put(c.getCustKey(), c);
                    EnrichedOrder e = enrichedMap.get(c.getCustKey());
                    if (e != null) {
                        out.collect(new ResultRecord(c, e));
                        enrichedMap.remove(c.getCustKey());
                        custMap.remove(c.getCustKey());
                    }
                }
            });

        // 6. 写出到CSV 文件
        result
            .map(ResultRecord::toCsv)
            .setParallelism(1)
            .writeAsText(outputPath, FileSystem.WriteMode.OVERWRITE)
            .setParallelism(1);

        env.execute("Streaming Q18 HandWritten Join");
    }

    // 对象属性定义
    public static class Customer {
        private int custKey;
        private String name;
        public int getCustKey() { return custKey; }
        public String getName() { return name; }
        public void setCustKey(int k) { custKey = k; }
        public void setName(String n) { name = n; }
    }

    public static class Order {
        private int orderKey;
        private int custKey;
        private double totalPrice;
        private String orderDate;
        public int getOrderKey() { return orderKey; }
        public int getCustKey() { return custKey; }
        public double getTotalPrice() { return totalPrice; }
        public String getOrderDate() { return orderDate; }
        public void setOrderKey(int v) { orderKey = v; }
        public void setCustKey(int v) { custKey = v; }
        public void setTotalPrice(double v) { totalPrice = v; }
        public void setOrderDate(String d) { orderDate = d; }
    }

    public static class LineItem {
        private int orderKey;
        private double quantity;
        public int getOrderKey() { return orderKey; }
        public double getQuantity() { return quantity; }
        public void setOrderKey(int k) { orderKey = k; }
        public void setQuantity(double q) { quantity = q; }
    }

    // sum-> (orderKey, quantity)
    public static class OrderQty {
        private int orderKey;
        private double quantity;
        public OrderQty() {}
        public OrderQty(int k, double q) { orderKey = k; quantity = q; }
        public int getOrderKey() { return orderKey; }
        public double getQuantity() { return quantity; }
    }

    public static class EnrichedOrder {
        private Order order;
        private double totalQty;
        public EnrichedOrder() {}
        public EnrichedOrder(Order o, double q) { order = o; totalQty = q; }
        public Order getOrder() { return order; }
        public double getTotalQty() { return totalQty; }
    }

    public static class ResultRecord {
        private String name;
        private int custKey;
        private int orderKey;
        private String orderDate;
        private double totalPrice;
        private double totalQty;
        public ResultRecord() {}
        public ResultRecord(Customer c, EnrichedOrder e) {
            name = c.getName();
            custKey = c.getCustKey();
            orderKey = e.getOrder().getOrderKey();
            orderDate = e.getOrder().getOrderDate();
            totalPrice = e.getOrder().getTotalPrice();
            totalQty = e.getTotalQty();
        }
        public String toCsv() {
            return String.join(",",
                name,
                String.valueOf(custKey),
                String.valueOf(orderKey),
                orderDate,
                String.valueOf(totalPrice),
                String.valueOf(totalQty)
            );
        }
    }

    // 解析 MapFunction
    public static class ParseCustomer implements MapFunction<String, Customer> {
        @Override public Customer map(String line) {
            String[] f = line.split("\\|", -1);
            Customer c = new Customer();
            c.setCustKey(Integer.parseInt(f[0]));
            c.setName(f[1]);
            return c;
        }
    }
    public static class ParseOrder implements MapFunction<String, Order> {
        @Override public Order map(String line) {
            String[] f = line.split("\\|", -1);
            Order o = new Order();
            o.setOrderKey(Integer.parseInt(f[0]));
            o.setCustKey(Integer.parseInt(f[1]));
            o.setTotalPrice(Double.parseDouble(f[3]));
            o.setOrderDate(f[4]);
            return o;
        }
    }
    public static class ParseLineItem implements MapFunction<String, LineItem> {
        @Override public LineItem map(String line) {
            String[] f = line.split("\\|", -1);
            LineItem li = new LineItem();
            li.setOrderKey(Integer.parseInt(f[0]));
            li.setQuantity(Double.parseDouble(f[4]));
            return li;
        }
    }
}
