package com.ym.gmall.common.constant;

import com.ym.gmall.common.mock.InternetUtil;

/**
 * 这里的常量基本上没用到，只是复制粘贴过来的
 * 有必要的话再搞个 kafka 和 hbase 吧。
 * 只是做一个模拟项目，也没有必要搞到这么复杂
 */
public class Constant {

//    public static final String LOCALHOST = "192.168.3.200";
    public static final String LOCALHOST = InternetUtil.getLocalHost();

    public static final String CHECHKPOINT_SOTRAGE = "file:///Users/ym/Downloads/tmp/flink_checkpoint/";

    public static final String KAFKA_BROKERS = LOCALHOST + ":9092";

    public static final String TOPIC_DB = "topic_db";
    public static final String TOPIC_LOG = "topic_log";
    public static final String TOPIC_DIM = "dim_all";

    public static final String MYSQL_HOST = LOCALHOST;
    public static final int MYSQL_PORT = 3306;
    public static final String MYSQL_USER_NAME = "root";
    public static final String MYSQL_PASSWORD = "123456";
    public static final String MYSQL_DATABASE = "gmall";
    public static final String MYSQL_TABLELIST = "gmall.activity_info, gmall.activity_rule, gmall.activity_sku, gmall.base_category1, gmall.base_category2, gmall.base_category3, gmall.base_dic, gmall.base_province, gmall.base_region, gmall.base_trademark, gmall.coupon_info, gmall.coupon_range, gmall.financial_sku_cost, gmall.sku_info, gmall.spu_info, gmall.user_info";
    public static final String HBASE_NAMESPACE = "gmall";
    public static final String HBASE_ZOOKEEPER_QUORUM = "localhost";
    public static final String HBASE_ZOOKEEPER_PROPERTY_CLIENTPORT = "2182";

    public static final String MYSQL_DRIVER = "com.mysql.cj.jdbc.Driver";
    public static final String MYSQL_URL = "jdbc:mysql://localhost:3306?useSSL=false";

    public static final String TOPIC_DWD_TRAFFIC_START = "dwd_traffic_start";
    public static final String TOPIC_DWD_TRAFFIC_ERR = "dwd_traffic_err";
    public static final String TOPIC_DWD_TRAFFIC_PAGE = "dwd_traffic_page";
    public static final String TOPIC_DWD_TRAFFIC_ACTION = "dwd_traffic_action";
    public static final String TOPIC_DWD_TRAFFIC_DISPLAY = "dwd_traffic_display";
    public static final String TOPIC_DWD_INTERACTION_COMMENT_INFO = "dwd_interaction_comment_info";
    public static final String TOPIC_DWD_TRADE_CART_ADD = "dwd_trade_cart_add";
    public static final String TOPIC_DWD_TRADE_ORDER_DETAIL = "dwd_trade_order_detail";
    public static final String TOPIC_DWD_TRADE_ORDER_CANCEL = "dwd_trade_order_cancel";
    public static final String TOPIC_DWD_TRADE_ORDER_PAYMENT_SUCCESS = "dwd_trade_order_payment_success";
    public static final String TOPIC_DWD_TRADE_ORDER_REFUND = "dwd_trade_order_refund";
    public static final String TOPIC_DWD_TRADE_REFUND_PAYMENT_SUCCESS = "dwd_trade_refund_payment_success";
    public static final String TOPIC_DWD_USER_REGISTER = "dwd_user_register";

    public static final int TWO_DAY_SECONDS = 2 * 24 * 60 * 60;

    public static final String DORIS_FE_NODES = "";
    public static final String DORIS_DATABASE = "";
}
