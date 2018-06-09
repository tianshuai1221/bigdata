package com.atguigu.mr;

import com.atguigu.convertor.DimensionConvertorImpl;
import com.atguigu.kv.key.CommDimension;
import com.atguigu.kv.value.CountDurationValue;
import com.atguigu.kv.base.BaseDimension;
import com.atguigu.util.JDBCInstance;
import com.atguigu.util.JDBCUtil;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.output.FileOutputCommitter;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;

public class MySQLOutPutFormat extends OutputFormat<BaseDimension, CountDurationValue> {

    private FileOutputCommitter committer = null;

    @Override
    public RecordWriter<BaseDimension, CountDurationValue> getRecordWriter(TaskAttemptContext context) throws IOException, InterruptedException {

        Connection connection = null;
        try {
            connection = JDBCInstance.getInstance();
            connection.setAutoCommit(false);
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return new MysqlRecordWriter(connection);
    }

    @Override
    public void checkOutputSpecs(JobContext context) throws IOException, InterruptedException {

    }

    @Override
    public OutputCommitter getOutputCommitter(TaskAttemptContext context) throws IOException, InterruptedException {
        if (committer == null) {
            Path output = getOutputPath(context);
            committer = new FileOutputCommitter(output, context);
        }
        return committer;
    }

    private static Path getOutputPath(JobContext job) {
        String name = job.getConfiguration().get(FileOutputFormat.OUTDIR);
        return name == null ? null : new Path(name);
    }

    static class MysqlRecordWriter extends RecordWriter<BaseDimension, CountDurationValue> {
        private Connection connection = null;
        private PreparedStatement preparedStatement = null;
        private int batchBound = 500;//缓存sql条数边界
        private int batchSize = 0;//客户端已经缓存的条数

        public MysqlRecordWriter(Connection connection) {
            this.connection = connection;
        }

        @Override
        public void write(BaseDimension key, CountDurationValue value) throws IOException, InterruptedException {
            CommDimension commDimension = (CommDimension) key;
            //插入数据到mysql
            String sql = "INSERT INTO ct.tb_call VALUES (?,?,?,?,?) ON DUPLICATE KEY UPDATE `call_sum`=?, `call_duration_sum`=?;";

            //维度转换
            DimensionConvertorImpl convertor = new DimensionConvertorImpl();

            int dateId = convertor.getDimensionID(commDimension.getDateDimension());
            int contactId = convertor.getDimensionID(commDimension.getContactDimension());

            String date_contact = dateId + "_" + contactId;

            int countSum = Integer.valueOf(value.getCountSum());
            int durationSum = Integer.valueOf(value.getDurationSum());

            try {
                preparedStatement = connection.prepareStatement(sql);
                int i = 0;
                preparedStatement.setString(++i, date_contact);
                preparedStatement.setInt(++i, dateId);
                preparedStatement.setInt(++i, contactId);
                preparedStatement.setInt(++i, countSum);
                preparedStatement.setInt(++i, durationSum);
                preparedStatement.setInt(++i, countSum);
                preparedStatement.setInt(++i, durationSum);

                //将sql缓存到客户端
                preparedStatement.addBatch();

                batchSize++;
                if (batchSize >= batchBound) {
                    //批量执行sql
                    preparedStatement.executeBatch();
                    connection.commit();
                }
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }

        @Override
        public void close(TaskAttemptContext context) throws IOException, InterruptedException {
            try {
                if (preparedStatement != null) {
                    preparedStatement.executeBatch();
                    connection.commit();
                }
                JDBCUtil.close(connection, preparedStatement, null);
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
    }

}
