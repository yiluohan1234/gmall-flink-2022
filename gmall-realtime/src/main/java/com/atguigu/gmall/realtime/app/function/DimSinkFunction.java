package com.atguigu.gmall.realtime.app.function;

import com.alibaba.fastjson.JSONObject;
import com.atguigu.gmall.realtime.common.GmallConfig;
import com.atguigu.gmall.realtime.utils.DimUtil;
import org.apache.commons.lang.StringUtils;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Collection;
import java.util.Set;

public class DimSinkFunction extends RichSinkFunction<JSONObject> {

    private Connection connection = null;

    @Override
    public void open(Configuration parameters) throws Exception {
        // 初始化 Phoenix 连接
        Class.forName(GmallConfig.PHOENIX_DRIVER);
        connection = DriverManager.getConnection(GmallConfig.PHOENIX_SERVER);
        //connection.setAutoCommit(true);

    }

    // 将数据写入 Phoenix：upsert into tb.tn(id,name,sex) values(...,...,...)
    @Override
    public void invoke(JSONObject value, Context context) throws Exception {
        PreparedStatement preparedStatement = null;
        try {
            //获取SQL语句
            String sinkTable = value.getString("sinkTable");
            JSONObject after = value.getJSONObject("after");
            String upsertSql = genUpdsertSql(sinkTable, after);
            System.out.println(upsertSql);

            //预编译SQL
            preparedStatement = connection.prepareStatement(upsertSql);

            // 判断如果当前数据是更新操作，则先删除redis中数据
            if ("update".equals(value.getString("type"))) {
                DimUtil.delRedisDimInfo(sinkTable.toUpperCase(), after.getString("id"));
            }


            //执行插入操作
            preparedStatement.executeUpdate();
            connection.commit();

        } catch (SQLException throwables) {
            throwables.printStackTrace();
            System.out.println("插入 Phoenix 数据失败！");
        } finally {
            if (preparedStatement != null) {
                preparedStatement.close();
            }
        }

    }

    // upsert into db.tn(id,name,sex) values('...','...','...')
    private String genUpdsertSql(String sinkTable, JSONObject data) {
        Set<String> keySet = data.keySet();
        Collection<Object> values = data.values();

        return "upsert into " + GmallConfig.HBASE_SCHEMA + "." +
                sinkTable + "(" +
                StringUtils.join(keySet, ",") + ")values('" +
                StringUtils.join(values, "','") + "')";
    }

}
