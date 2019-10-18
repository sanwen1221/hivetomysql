import java.io.IOException;
import java.sql.*;
import java.util.Properties;

/**
 * mysql工具类
 * author dinghh
 * time 2019-10-16 18:10
 */
public class MysqlUtl {

    private static Properties prop=new Properties();

    /**
     * 获取mysql connection
     *
     * @param url
     * @param userName
     * @param password
     * @return
     */
    public static Connection getMysqlConnection(String url, String userName, String password) throws ClassNotFoundException, SQLException {

        Class.forName("com.mysql.jdbc.Driver");

        Connection connection = DriverManager.getConnection(url, userName, password);

        return connection;

    }


    public static Connection getHiveConnection() throws ClassNotFoundException, SQLException {

        try {
            prop.load(MysqlUtl.class.getClassLoader().getResourceAsStream("config.properties"));
        } catch (IOException e) {
            e.printStackTrace();
        }

        String driverName = "org.apache.hive.jdbc.HiveDriver";

        Class.forName(driverName);

        Connection connection = DriverManager.getConnection(prop.getProperty("hive_url"), prop.getProperty("hive_username"),prop.getProperty("hive_password"));

        return connection;

    }





    public static void main(String[] args) {


        try {
            Connection conn = getMysqlConnection("jdbc:mysql://192.168.5.66:3306/dhh", "root", "password");
            Statement stmt = conn.createStatement();
            ResultSet resultSet = stmt.executeQuery("select * from order_info");

            ResultSetMetaData metaData = resultSet.getMetaData();

            int columnCount = metaData.getColumnCount();//获取长度

            /*for (int i = 0; i < columnCount; i++) {
                String columnName = metaData.getColumnName(i + 1);
                System.out.println(columnName);
            }*/

            for (int i = 0; i < columnCount; i++) {
                String columnTypeName = metaData.getColumnTypeName(i + 1);
                System.out.println(columnTypeName);
            }

            /*while (resultSet.next()){

                String col="";

                for (int i = 0; i < columnCount; i++) {
                    col+=metaData.getColumnName(i+1);
                    col+=resultSet.getString(i+1);

                }
                System.out.println(col);


            }*/
/*
            while(resultSet.next()){

                System.out.println(resultSet.getString(i++));
            }
*/

            conn.close();


        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        } catch (SQLException e) {
            e.printStackTrace();
        }

    }

}
