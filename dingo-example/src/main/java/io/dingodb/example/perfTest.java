/*
 * Copyright 2021 DataCanvas
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.dingodb.example;

import org.apache.commons.lang3.RandomStringUtils;

import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.Date;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.Statement;
import java.sql.Time;
import java.sql.Timestamp;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.List;
import java.util.Random;


public class perfTest {
    static final String JDBC_DRIVER = "io.dingodb.driver.client.DingoDriverClient";
    static final List<String> performTableList = Arrays.asList("scalar4");
    private static Random rd = new Random();

    public static void main(String[] args) {
        String hostIP = "172.30.14.223:8765?timeout=3600";
        if (args.length >= 1) {
            hostIP = args[0];
        }
        String connectUrl = "jdbc:dingo:thin:url=" + hostIP;
        Connection connection = null;
        PreparedStatement ps = null;
        Statement statment = null;

        try {
            Class.forName(JDBC_DRIVER);
            connection = DriverManager.getConnection(connectUrl, "root", "123123");
            statment = connection.createStatement();

            for (String s : performTableList) {
//                statment.execute("drop table if exists " + s);
                String createSql = "create table " + s + " (\n" +
                    "    id varchar(32) not null,\n" +
                    "    name varchar(32),\n" +
                    "    age int,\n" +
                    "    gmt bigint,\n" +
                    "    price FLOAT,\n" +
                    "    amount DOUBLE,\n" +
                    "    address varchar(255),\n" +
                    "    birthday DATE,\n" +
                    "    create_time TIME,\n" +
                    "    update_time TIMESTAMP,\n" +
                    "    zip_code varchar(10),\n" +
                    "    is_delete boolean,\n" +
                    "    PRIMARY KEY (id)\n" +
                    ") engine=TXN_LSM";
                statment.execute(createSql);
                System.out.println("表" + s + "创建完成");

                Thread.sleep(500);
                String insertSql = "insert into " + s + " values (?,?,?,?,?,?,?,?,?,?,?,?)";
                ps = connection.prepareStatement(insertSql);
                int insertCount = 10000;
                for (int i = 1; i <= insertCount; i++) {
//                    ps.setInt(1, i);
                    String randKey1 = getFixLengStr(8);
                    String randKey2 = getFixLengStr(7);
                    ps.setString(1, randKey1 + i + randKey2 + "gx5");
                    String randStr = getRandStr(6);
                    ps.setString(2, randStr);
                    int randInt = getRandInt(100);
                    ps.setInt(3, randInt);
                    long randLong = getRandLong();
                    ps.setLong(4, randLong);
                    float randFloat = getRandFloat(0, 10000, 2);
                    ps.setFloat(5, randFloat);
                    double randDouble = getRandDouble(-1000000, 1000000, 2);
                    ps.setDouble(6, randDouble);
                    String randAddr = getRandStr(32);
                    ps.setString(7, randAddr);
                    Date randDate = getRandomDate("yyyy-MM-dd", "1970-10-01", "2024-12-31");
                    ps.setDate(8, randDate);
                    Time randTime = getRandomTime("HH:mm:ss", "00:00:00", "23:59:59");
                    ps.setTime(9, randTime);
                    Timestamp randTimestamp = getRandomTimestamp("yyyy-MM-dd HH:mm:ss", "1990-01-01 00:00:00", "2023-03-21 23:59:59");
                    ps.setTimestamp(10, randTimestamp);
                    ps.setString(11, null);
                    ps.setBoolean(12, getRandBoolean());

                    ps.addBatch();

                    if (i % 1000 == 0) {
                        ps.executeBatch();
                        ps.clearBatch();
                        System.out.println("已写入数据：" + i);
                    }
                }
                System.out.println("表" + s + "写入数据完成");
            }

        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            try{
                if(ps!= null) {
                    ps.close();
                }
            } catch (Exception e) {
                e.printStackTrace();
            }
            try{
                if(statment != null) {
                    statment.close();
                }
            } catch (Exception e) {
                e.printStackTrace();
            }
            try{
                if(connection != null) {
                    connection.close();
                }
            }catch (Exception e) {
                e.printStackTrace();
            }
        }
    }

    public static String getRandStr(int maxLength) {
        return RandomStringUtils.randomAlphanumeric(getRandInt(maxLength));
    }

    public static String getFixLengStr(int length) {
        return RandomStringUtils.randomAlphanumeric(length);
    }

    public static int getRandInt(int max) {
        return rd.nextInt(max) + 1;
    }

    public static long getRandLong() {
        return rd.nextLong();
    }

    public static float getRandFloat(int min, int max, int scaleNum) {
        float d0 = min + ((max -min) * rd.nextFloat());
        BigDecimal bg = new BigDecimal(d0);
        float d1 = bg.setScale(scaleNum, BigDecimal.ROUND_HALF_UP).floatValue();
        return d1;
    }

    public static double getRandDouble(int min, int max, int scaleNum) {
        double d0 = min + ((max -min) * rd.nextDouble());
        BigDecimal bg = new BigDecimal(d0);
        double d1 = bg.setScale(scaleNum, BigDecimal.ROUND_HALF_UP).doubleValue();
        return d1;
    }

    public static Date getRandomDate(String patternFormat, String startDateStr, String endDateStr) throws ParseException {
        SimpleDateFormat sdf = new SimpleDateFormat(patternFormat);
        long start = sdf.parse(startDateStr).getTime();
        long end = sdf.parse(endDateStr).getTime();
        long randomDate = nextLong(start, end);
        return Date.valueOf(sdf.format(randomDate));
    }

    //获取随机时间，返回时间型
    public static Time getRandomTime(String patternFormat, String startTimeStr, String endTimeStr) throws ParseException {
        SimpleDateFormat sdf = new SimpleDateFormat(patternFormat);
        long start = sdf.parse(startTimeStr).getTime();
        long end = sdf.parse(endTimeStr).getTime();
        long randomTime = nextLong(start, end);
        return Time.valueOf(sdf.format(randomTime));
    }

    //获取随机日期时间，返回timestamp型
    public static Timestamp getRandomTimestamp(String patternFormat, String startTimestampStr, String endTimestampStr) throws ParseException {
        SimpleDateFormat sdf = new SimpleDateFormat(patternFormat);
        long start = sdf.parse(startTimestampStr).getTime();
        long end = sdf.parse(endTimestampStr).getTime();
        long randomTimestamp = nextLong(start, end);
        return Timestamp.valueOf(sdf.format(randomTimestamp));
    }

    public static long nextLong(long start, long end) {
        Random random = new Random();
        return start + (long) (random.nextDouble() * (end - start + 1));
    }

    public static boolean getRandBoolean() {
        return rd.nextBoolean();
    }
}
