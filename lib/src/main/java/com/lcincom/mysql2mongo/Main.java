package com.lcincom.mysql2mongo;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Types;
import java.util.Arrays;
import java.util.List;

import static com.mongodb.client.model.Filters.eq;
import org.bson.Document;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.ReplaceOptions;

import io.github.cdimascio.dotenv.Dotenv;

public class Main {

  public static void main(String[] args) throws Exception {
    String envPath = System.getProperty("env");
    Dotenv dotenv = Dotenv.configure().directory(envPath).load();
    String mongoUri = dotenv.get("MONGO_URI");
    String mysqlUri = dotenv.get("MYSQL_URI");
    String table = dotenv.get("TABLE");
    String tableId = dotenv.get("TABLE_ID");
    String tableIdType = dotenv.get("TABLE_ID_TYPE");
    int interval = Integer.parseInt(dotenv.get("INTERVAL"));
    int queryLimit = Integer.parseInt(dotenv.get("QUERY_LIMIT"));
    String mongoDb = dotenv.get("MONGO_DB");

    List<Integer> stringTypes = Arrays.asList(Types.VARCHAR, Types.CHAR, Types.LONGVARCHAR);
    List<Integer> nstringTypes = Arrays.asList(Types.NVARCHAR, Types.NCHAR, Types.LONGNVARCHAR);
    List<Integer> intTypes = Arrays.asList(Types.INTEGER, Types.SMALLINT, Types.TINYINT);

    Connection mysqlConn = null;
    MongoClient mongoClient = null;
    PreparedStatement stmtSelect = null;
    PreparedStatement stmtUpdate = null;
    ResultSet rs = null;
    try {
      Class.forName("com.mysql.cj.jdbc.Driver").getDeclaredConstructor().newInstance();
      mysqlConn = DriverManager.getConnection(mysqlUri);

      mongoClient = MongoClients.create(mongoUri);
      MongoDatabase database = mongoClient.getDatabase(mongoDb);
      MongoCollection<Document> collection = database.getCollection(table);

      ReplaceOptions options = new ReplaceOptions();
      options.upsert(true);

      String selectSql = String.format("SELECT * FROM %s WHERE is_mongo=0 LIMIT ?", table);
      stmtSelect = mysqlConn.prepareStatement(selectSql);
      stmtSelect.setInt(1, queryLimit);

      String updateSql = String.format("UPDATE %s SET is_mongo=1 WHERE %s=?", table, tableId);
      stmtUpdate = mysqlConn.prepareStatement(updateSql);

      while (true) {
        rs = stmtSelect.executeQuery();
        ResultSetMetaData rsm = rs.getMetaData();

        int count = 0;
        while (rs.next()) {
          count += 1;
          Document document = new Document();
          for (int i = 0, n = rsm.getColumnCount(); i < n; i++) {
            int columnIndex = i + 1;
            String columnName = rsm.getColumnName(columnIndex);
            int columnType = rsm.getColumnType(columnIndex);
            if (!columnName.equalsIgnoreCase("is_mongo")) {
              if (intTypes.contains(columnType)) {
                document.append(columnName, rs.getInt(columnIndex));
              } else if (stringTypes.contains(columnType)) {
                document.append(columnName, rs.getString(columnIndex));
              } else if (nstringTypes.contains(columnType)) {
                document.append(columnName, rs.getNString(columnIndex));
              } else if (columnType == Types.FLOAT) {
                document.append(columnName, rs.getFloat(columnIndex));
              } else if (columnType == Types.DATE) {
                document.append(columnName, rs.getDate(columnIndex));
              }  else {
                String message = String.format("Does not handle type %d", columnType);
                System.out.println(message);
              }
            }
          }

          if (tableIdType.equalsIgnoreCase("string")) {
            String id = document.getString(tableId);
            collection.replaceOne(eq(tableId, id), document, options);
            stmtUpdate.setString(1, id);
          } else {
            int id = document.getInteger(tableId);
            collection.replaceOne(eq(tableId, id), document, options);
            stmtUpdate.setInt(1, id);
          }

          stmtUpdate.executeUpdate();
        }

        System.out.println(String.format("%d records has been updated.", count));

        Thread.sleep(interval);
      }
    } finally {
      if (rs != null) {
        try {
          rs.close();
        } catch (SQLException sqlEx) {}

        rs = null;
      }

      if (stmtSelect != null) {
        try {
          stmtSelect.close();
        } catch (SQLException sqlEx) {}

        stmtSelect = null;
      }

      if (stmtUpdate != null) {
        try {
          stmtUpdate.close();
        } catch (SQLException sqlEx) {}

        stmtUpdate = null;
      }

      if (mysqlConn != null) {
        try {
          mysqlConn.close();
        } catch (SQLException e) {}

        mysqlConn = null;
      }

      if (mongoClient != null) {
        mongoClient.close();
        mysqlConn = null;
      }
    }
  }

}
