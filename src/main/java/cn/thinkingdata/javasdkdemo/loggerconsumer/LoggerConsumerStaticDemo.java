package cn.thinkingdata.javasdkdemo.loggerconsumer;

import cn.thinkingdata.tga.javasdk.ThinkingDataAnalytics;

import java.io.File;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;

public class LoggerConsumerStaticDemo {
    public static void main(String[] args) throws Exception {
        //使用LoggerConsumer
        //整个环节，如何使用 TA 的产品来记录日志。注意的是demo是TA的一个样式展示，具体的埋点方案可以有我们的数据分析师来提供
        String log_dirctory = "/data/log/logdata/";          //填写logbus监控的目录，目录一定要存在
        //LoggerConsumer 有几种实例方式，根据业务需求设置，选择一种即可
        /**
         * 1.该方式在1.3.1版本后不默认以大小切分文件，只按天切分文件
         */
        ThinkingDataAnalytics tga = new ThinkingDataAnalytics(new ThinkingDataAnalytics.LoggerConsumer(log_dirctory));
        /**
         * 2.该方式可以设置文件大小切分，以天切分为前提,这里设置的是5GB
         */
        //ThinkingDataAnalytics tga = new ThinkingDataAnalytics(new ThinkingDataAnalytics.LoggerConsumer(log_dirctory,5*1024));
        /**
         * 3.该方式是在版本1.2.0以后的实例，之前的实例方式也保留着，版本1.3.1以后默认大小1GB 取消，用户可根据数据量设计按小时切分还是按大小切分，默认按天切分
         */
        //ThinkingDataAnalytics.LoggerConsumer.Config config = new ThinkingDataAnalytics.LoggerConsumer.Config(log_dirctory);
        //config.setRotateMode(ThinkingDataAnalytics.LoggerConsumer.RotateMode.DAILY);//可以设置是按天(DAILY)切分，还是按小时（HOURLY）切分，默认按天切分文件，可不设置
        //config.setFileSize(2*1024);//设置在按天切分的前提下，按大小切分文件，可不设置
        //config.setBufferSize(8192);//默认是8192字节(8k)然后刷新数据(flush)，可设置字节
        //ThinkingDataAnalytics tga = new ThinkingDataAnalytics(new ThinkingDataAnalytics.LoggerConsumer(config));

        // 1. 用户访问
        String distinct_id = "ABCDEFG123456789";         //设置访客ID"ABCDEFG123456789", 用户未登录时，可以使用产品自己生成的distinct_id来标注用户，没有可不填写
        String account_id = "TA_10001";                 //  设置账号ID"TA_10001",如果主要以账号为分析单位，则直接以账号作为主ID，按照场景来设置
        Map<String, Object> properties = new HashMap<String, Object>(); //设置事件属性
        // 注意：前面有#开头的property字段，是TA提供给用户的预置字段，已经确定好了字段类型和字段的显示名，不需要另外添加
        //       规定只能是预置属性，或以字母开头，包含数字，字母和下划线“_”，长度最大为50个字符，对字母大小写不敏感。
        properties.clear();
        properties.put("#time", new Date());                                //设置事件发生的时间，如果不设置的话，则默认使用为当前时间，注意-- #time的类型必须是Date
        properties.put("#os", "ios");                                       // 通过请求中的UA，可以解析出用户使用设备的操作系统是ios还是android，Windows
        properties.put("#os_version", "11.1.2");                            // 操作系统的具体版本
        properties.put("#ip", "192.168.1.1");                               // 请求中能够拿到用户的IP,国内TA会自动根据这个解析省份、城市，国外TA会解析到洲
        properties.put("channel", "app store");                             // 当前渠道
        tga.track(account_id, distinct_id, "page_view", properties); //网页加载完成时触发的事件名为page_view
        tga.flush();//可立即刷新数据
        //2.用户注册登录
        //用户进行登录，创建角色,访客ID和账号ID进行绑定，具体逻辑可看官网文档
        Map<String, Object> userSetProperties = new HashMap<String, Object>();//记录用户属性
        //用户注册时，填充了一些个人信息，可以用user_set接口记录下来
        userSetProperties.clear();
        userSetProperties.put("#city", "江苏");                           // 用户所在城市,可以根据上传的#ip来生成
        userSetProperties.put("#province", "南京");                       // 用户所在省份,可以根据上传的#ip来生成
        userSetProperties.put("register_time", new Date());               // 注册时间
        userSetProperties.put("gender", "male");                          // 用户的性别
        userSetProperties.put("nickname", "122");                         // 用户的昵称
        userSetProperties.put("birthday", new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").parse("1998-11-03 " + "00:00:00"));  // 用户的出生日期
        userSetProperties.put("RegisterChannel", "app store");            // 用户的注册渠道
        tga.user_set(account_id, distinct_id, userSetProperties);          // 此时传入的是注册ID了

        // 充值事件的信息
        properties.clear();
        properties.put("#os", "ios");                                   // 操作系统
        properties.put("#os_version", "11.1.2");                        // 操作系统的具体版本
        properties.put("#ip", "192.168.1.1");                           // 请求中能够拿到用户的IP,国内TA会自动根据这个解析省份、城市，国外TA会解析到洲
        properties.put("OrderId", "1111111");                           // 订单ID
        properties.put("recharge_grade", 12);                           //充值等级
        properties.put("OrderTotalPrice", 123.0);                       // 充值的总价格
        properties.put("ProductAllowanceAmount", 23.0);                 // 假设这个补贴是在活动上的折扣
        properties.put("ProductPaymentAmount", 100.0);                  // 实际支付了这么多
        tga.track(account_id, distinct_id, "recharge", properties); // 注意，此时使用的account_id为主ID了
        // 消费战斗金币情况
        properties.clear();
        properties.put("#os", "ios");                                      // 通过请求中的UA，可以解析出用户使用设备的操作系统是ios还是android，Windows
        properties.put("#os_version", "11.1.2");                           // 操作系统的具体版本
        properties.put("#ip", "192.168.1.1");                              // 请求中能够拿到用户的IP,国内TA会自动根据这个解析省份、城市，国外TA会解析到洲
        properties.put("ProductName", "XX英雄");                           // 道具名称
        properties.put("count", 12000.0);                                  // 道具金币价格
        properties.put("count_left", 8000.0);                              // 剩余金币数量
        tga.track(account_id, distinct_id, "consume_coin", properties);
        // 立刻刷新一下，让数据传到TA中
        tga.flush();
        //最后关闭sdk
        tga.close();
    }
}
