package com.jayyin.demo.demomongodb;

import com.mongodb.MongoCredential;
import com.mongodb.ServerAddress;
import com.mongodb.client.*;
import org.bson.Document;
import org.bson.conversions.Bson;

import java.util.ArrayList;
import java.util.List;

public class MongoDBJDBC {


    public static MongoDBJDBC mInstance;

    private MongoClient mClient = null;
    private MongoDatabase mDatabase = null;

    private MongoDBJDBC() {

    }

    public static MongoDBJDBC getInstance() {
        if (mInstance == null) {
            synchronized (MongoDBJDBC.class) {
                if (mInstance == null)
                    mInstance = new MongoDBJDBC();
            }
        }
        return mInstance;
    }


    /**
     * 连接数据库 新版
     *
     * @param host
     * @param port
     * @param username
     * @param passw
     * @param dbname
     * @return
     */
    MongoDatabase connect(String host, String port, String username, String passw, String dbname) {
        try {
            String conn = "mongodb://" + host + ":" + port;
            mClient = MongoClients.create(conn);

            if (dbname == null || dbname.isEmpty())
                dbname = "test_db";
            mDatabase = mClient.getDatabase(dbname);
            System.out.println(">>> Connect to database " + dbname + " successfully !");

        } catch (Exception e) {
            e.printStackTrace();
        }
//        if (client != null)
//            client.close();
        return mDatabase;
    }

    /**
     * 老版本驱动连接方式
     */
    private void oldConnect() {
        try {
            //连接到MongoDB服务 如果是远程连接可以替换“localhost”为服务器所在IP地址
            //ServerAddress()两个参数分别为 服务器地址 和 端口
            ServerAddress serverAddress = new ServerAddress("localhost", 27017);
            List<ServerAddress> addrs = new ArrayList<ServerAddress>();
            addrs.add(serverAddress);

            //MongoCredential.createScramSha1Credential()三个参数分别为 用户名 数据库名称 密码
            MongoCredential credential = MongoCredential.createScramSha1Credential("username", "databaseName", "password".toCharArray());
            List<MongoCredential> credentials = new ArrayList<MongoCredential>();
            credentials.add(credential);

//            //通过连接认证获取MongoDB连接
//            MongoClient mongoClient = new MongoClient(addrs, credentials);
//
//            //连接到数据库
//            MongoDatabase mongoDatabase = mongoClient.getDatabase("databaseName");
//            System.out.println("Connect to database successfully");
        } catch (Exception e) {
            System.err.println(e.getClass().getName() + ": " + e.getMessage());
        }
    }


    /**
     * 获取集合（表）
     *
     * @param database
     * @param s        collection--等于mysql中的表table   s-表名
     * @return
     */
    public MongoCollection getCollection(MongoDatabase database, String s) {
        try {

            MongoCollection collection = database.getCollection(s);
            System.out.println(">>> Get collection: " + collection.toString());
            return collection;
        } catch (Exception e) {
            System.out.println(">>> e 获取集合: " + e.getMessage());
            e.printStackTrace();
            return null;
        }
    }

    /**
     * 插入文档(数据)
     *
     * @param collection 集合(表)名
     *                   <p>
     *                   1. 创建文档 org.bson.Document 参数为key-value的格式
     *                   2. 创建文档集合List<Document>
     *                   3. 将文档集合插入数据库集合中 mongoCollection.insertMany(List<Document>) 插入单个文档可以用 mongoCollection.insertOne(Document)
     */
    public void insert(MongoCollection collection) {
        try {

            List<Document> documentList = new ArrayList<>();
            Document document = new Document("title", "MongoDb")
                    .append("t-k1", 1)
                    .append("t-k2", "value2")
                    .append("createtime", System.currentTimeMillis());

            documentList.add(document);
            collection.insertMany(documentList);
            System.out.println(">>> 文档插入成功!");
        } catch (Exception e) {
            System.out.println(">>> e 文档插入异常: " + e.getMessage());
            e.printStackTrace();
        }
    }


    /**
     * 读取某个集合中的数据
     *
     * @param collection
     * @param bson
     * @return
     */
    public List<Document> getDocument(MongoCollection collection, Bson bson) {
        List<Document> documents = new ArrayList<>();
        try {
            FindIterable<Document> iterable = null;
            if (bson != null)
                iterable = collection.find(bson);
            else
                iterable = collection.find();
            MongoCursor<Document> cursor = iterable.iterator();
            while (cursor.hasNext()) {
                Document document = cursor.next();
                documents.add(document);
                System.out.println(">>> 获取数据: " + document.toJson());
            }
        } catch (Exception e) {
            System.out.println(">>> e 获取数据: " + e.getMessage());
            e.printStackTrace();
        }
        return documents;
    }


    /**
     * 关闭数据库连接
     */
    public void close() {
        if (mClient != null)
            mClient.close();
        System.out.println(">>> 关闭连接成功!");
    }
}
