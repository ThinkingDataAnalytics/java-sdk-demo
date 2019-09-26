package cn.thinkingdata.javasdkdemo.loggerconsumer;

import cn.thinkingdata.tga.javasdk.ThinkingDataAnalytics;
import com.alibaba.fastjson.JSONObject;

import java.io.*;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;

public class LoggerConsumerDemo {
    private static final String LOGBUS_MONITOR_DIR = "/data/log/logdata/";  //logbus监控的目录

    private static final String SOURCE_DIR = "/data/log/source_log/";//源数据目录
    public static void main(String[] args){
        /***
         * 该demo适用于数据在指定文件中，以json的形式，以行来取值
         */
        //使用LoggerConsumer
        File dir = new File(LOGBUS_MONITOR_DIR);
        if(!dir.exists()){
            dir.mkdir();
        }
        try {
            //LoggerConsumer 有几种实例方式，根据业务需求设置，选择一种即可
            /**
             * 1.该方式在1.3.1版本后不默认以大小切分文件，只按天切分文件
             */
            ThinkingDataAnalytics tga = new ThinkingDataAnalytics(new ThinkingDataAnalytics.LoggerConsumer(LOGBUS_MONITOR_DIR));
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
            File[] files = new File(SOURCE_DIR).listFiles();
            if (files != null) {
                for (File file:files){
                    FileReader dimStream = new FileReader(file);
                    BufferedReader reader = new BufferedReader(dimStream);
                    String line = "";
                    while ((line = reader.readLine()) != null) {
                        String[] jsonStr = line.split("\n");//\n获取一个json文件
                        for (String json : jsonStr){
                            JSONObject jsonObject = JSONObject.parseObject(json);
                            String distinct_id = jsonObject.getString("distinct_id");//未登录时，可设置为设备id
                            String account_id = jsonObject.getString("account_id");//用户登录后，设置的唯一张账号id
                            Map<String,Object> properties = new HashMap<String,Object>(); //设置事件属性
                            // 注意：前面有#开头的property字段，是TA提供给用户的预置字段，已经确定好了字段类型和字段的显示名，不需要另外添加
                            properties.clear();
                            properties.put("#time", new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").parse(jsonObject.getString("log_time")));         //设置事件发生的时间，如果不设置的话，则默认使用为当前时间，注意-- #time的类型必须是Date
                            properties.put("#os", jsonObject.getString("os"));                 // 通过请求中的UA，可以解析出用户使用设备的操作系统是ios还是android
                            properties.put("#os_version", jsonObject.getString("os_version")); // 操作系统的具体版本
                            properties.put("#ip", jsonObject.getString("ip"));                 // 请求中能够拿到用户的IP,国内TA会自动根据这个解析省份、城市，国外TA会解析到洲
                            properties.put("channel", jsonObject.getString("channel"));        // 当前渠道
                            properties.put("is_money",jsonObject.getBoolean("is_money"));      //是否充值过
                            tga.track(account_id, distinct_id, "page_view", properties);              //网页加载完成时触发的事件名为page_view

                            Map<String,Object> userSetProperties  = new HashMap<String,Object>();//记录用户属性
                            //登录时，用户个人信息，可以用user_set接口记录下来
                            userSetProperties.put("#ip",jsonObject.getString("ip"));                                // 请求中能够拿到用户的IP,国内TA会自动根据这个解析省份、城市，国外TA会解析到洲
                            userSetProperties.put("last_login_time", new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").parse(jsonObject.getString("last_login_time")));                 //最后登录时间
                            userSetProperties.put("register_time",new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").parse(jsonObject.getString("register_time")));          // 注册时间
                            userSetProperties.put("gender",jsonObject.getString("gender"));                         // 用户的性别
                            userSetProperties.put("nickname", jsonObject.getString("nickname"));                    // 用户的昵称
                            userSetProperties.put("birthday", new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").parse("1998-11-03 " + "00:00:00"));  // 用户的出生日期
                            userSetProperties.put("register_channel", jsonObject.getString("register_channel"));     // 用户的注册渠道
                            tga.user_set(account_id, distinct_id,userSetProperties);
                            //更多的tag的接口功能可查看官网http://www.thinkinggame.cn/manual.html
                            // 立刻刷新一下，让数据写入到logbus监控的文件中
                        }
                    }
                    reader.close();
                }
            }
            // 可以不做此操作，到了一定数量也可以自动刷新
            tga.flush();
            //最后关闭sdk
            tga.close();
        } catch (Exception e) {
            e.printStackTrace();
        }

    }
}
