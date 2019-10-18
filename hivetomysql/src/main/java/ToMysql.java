
import org.apache.hadoop.hive.ql.exec.UDF;

import java.sql.*;
import java.util.ArrayList;


/**
 * author dinghh
 * time 2019-10-17 15:11
 */
public class ToMysql extends UDF {

    /**
     * 将sql的查询结果写入到指定的mysql表中，
     * 首先hive字段需要和mysql字段一一对应，并且hive字段的类型也需要和mysql字段的类型一致.
     * 增量更新
     *
     * @param url       mysqlurl
     * @param username  mysql用户名
     * @param password  mysql密码
     * @param tableName mysql表名
     * @param sql       查询hive语句
     * @return
     */
    public String evaluate(String url, String username, String password, String tableName, String sql) {
        return evaluate(url, username, password, tableName, sql, true);
    }

    /**
     * 将sql的查询结果写入到指定的mysql表中，
     * 首先hive字段需要和mysql字段一一对应，并且hive字段的类型也需要和mysql字段的类型一致.
     * 增量更新或者全量
     *
     * @param url       mysqlurl
     * @param username  mysql用户名
     * @param password  mysql密码
     * @param tableName mysql表名
     * @param sql       查询hive语句
     * @param isAppend  true增量,false全量
     */
    public String evaluate(String url, String username, String password, String tableName, String sql, boolean isAppend) {
        Connection mysqlConn = null;
        Connection hiveConn = null;

        String result="completed";
        //关闭自动提交
        try {
            //获取mysql连接
            mysqlConn = MysqlUtl.getMysqlConnection(url, username, password);
            mysqlConn.setAutoCommit(false);
            if (!isAppend) {
                //全量则清空表
                String deleteSql = "delete from " + tableName;
                PreparedStatement deleteStmt = mysqlConn.prepareStatement(deleteSql);
                deleteStmt.executeUpdate();
            }

            //获取hive连接
            hiveConn = MysqlUtl.getHiveConnection();
            PreparedStatement hiveQuery = hiveConn.prepareStatement(sql);

            ResultSet hiveResultSet = hiveQuery.executeQuery();

            ResultSetMetaData hiveMeta = hiveResultSet.getMetaData();

            int length = hiveMeta.getColumnCount();//获取字段长度

            String insertSql = "insert into " + tableName + " values(?";
            //配好插入语句
            for (int i = 1; i < length; i++) {
                insertSql += ",?";
            }
            insertSql += ")";
            
            PreparedStatement pstmt = mysqlConn.prepareStatement(insertSql);

            ArrayList<String> typeNameList = new ArrayList<String>();

            for (int i = 0; i < length; i++) {
                typeNameList.add(hiveMeta.getColumnTypeName(i + 1));//获取字段类型
            }

            while (hiveResultSet.next()) {
                for (int i = 0; i < length; i++) {
                    String typeName = typeNameList.get(i);
                    typeName = typeName.toUpperCase();
                    switch (typeName) {
                        case "TINYINT":
                            pstmt.setByte(i + 1, hiveResultSet.getByte(i + 1));
                            break;
                        case "SMALINT":
                            pstmt.setShort(i + 1, hiveResultSet.getShort(i + 1));
                            break;
                        case "INT":
                            int data = hiveResultSet.getInt(i + 1);
                            pstmt.setInt(i + 1, data);
                            break;
                        case "FLOAT":
                            pstmt.setFloat(i + 1, hiveResultSet.getFloat(i + 1));
                            break;
                        case "DOUBLE":
                            pstmt.setDouble(i + 1, hiveResultSet.getDouble(i + 1));
                            break;
                        case "STRING":
                            pstmt.setString(i + 1, hiveResultSet.getString(i + 1));
                            break;
                        case "BOOLEAN":
                            pstmt.setBoolean(i + 1, hiveResultSet.getBoolean(i + 1));
                            break;
                    }

                }
                pstmt.addBatch();
            }
            pstmt.executeBatch();
            mysqlConn.commit();

        } catch (SQLException e) {
            e.printStackTrace();
            try {
                mysqlConn.rollback();
            } catch (SQLException e1) {
                e1.printStackTrace();
            }

        } catch (ClassNotFoundException e) {
            e.printStackTrace();

        } finally {
            try {
                if (mysqlConn != null) {
                    mysqlConn.close();
                }
                if (hiveConn != null) {
                    hiveConn.close();
                }
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }


        return result;
    }


    public static void main(String[] args) {
        ToMysql toMysql = new ToMysql();
        String url = "jdbc:mysql://192.168.5.66/dhh";
        String username = "root";
        String password = "password";
       // toMysql.evaluate(url, username, password, "alerts", "select * from ods.primary_table",false);

       /* try {
            Connection conn = MysqlUtl.getMysqlConnection(url, username, password);
            Statement stmt = conn.createStatement();
            stmt.executeUpdate("insert into test_date(id,name) values(3,'lisi')");

        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        } catch (SQLException e) {
            e.printStackTrace();
        }*/


        toMysql.evaluate("jdbc:mysql://192.168.5.66/dhh","root","password","alerts","select * from ods.primary_table",true);
    }
}
