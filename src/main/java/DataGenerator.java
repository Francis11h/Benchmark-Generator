import com.complexible.stardog.StardogException;
import com.complexible.stardog.api.Connection;
import com.complexible.stardog.api.ConnectionPool;
import com.complexible.stardog.api.SelectQuery;
import com.stardog.stark.io.RDFFormats;
import com.stardog.stark.query.SelectQueryResult;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Random;

public class DataGenerator {

    /**
     * Obtains the Stardog connection from the connection pool
     * @param connectionPool the connection pool to get our connection
     * @return Stardog Connection
     */
    public static Connection getConnection(ConnectionPool connectionPool) {
        return connectionPool.obtain();
    }

    /**
     * Releases the Stardog connection from the connection pool
     * @param connection Stardog Connection
     */

    public static void releaseConnection(ConnectionPool connectionPool, Connection connection) {
        try {
            connectionPool.release(connection);
        } catch (StardogException e) {
            e.printStackTrace();
        }
    }

    private static List<String> generateKeys(int numOfKeys, ConnectionPool connectionPool) {
        try (Connection connection = getConnection(connectionPool)) {

            try {
                connection.begin();

                connection.add().io().format(RDFFormats.RDFXML).stream(new FileInputStream("src/main/resources/ManuServiceOntology.xml"));

                connection.commit();

                SelectQuery squery = connection.select("select ?o1 where \n" +
                        "{  ?s rdfs:domain ?o1.\n" +
                        "   ?s rdfs:range ?o2 .\n" +
                        "}");

                SelectQueryResult sresult = squery.execute();
//                        TextTableQueryResultWriter.FORMAT);
//                        List<String> keys = new ArrayList<>();
                HashMap<String,String> keys = new HashMap<>();

                while(sresult.hasNext()) {
                    String temp = sresult.next().get("o1").toString();
//                            System.out.print(temp.substring(42) + "------------------------------");
                    if(!temp.substring(42).equals("Material") && !temp.substring(42).equals("Quantity")) {
//                                System.out.println(temp);
                        keys.put(temp, temp.substring(42));
                    }
                }

                Random rand = new Random();
                Object[] keySet = keys.keySet().toArray();
                List<String> randomKeys = new ArrayList<>();
                for(int i=0;i<numOfKeys;i++){
                    int index = rand.nextInt(keySet.length);
                    randomKeys.add(keys.get(keySet[index]));
                }

                for(int i=0;i<randomKeys.size();i++){
                    System.out.println("KEYS FOR METADATA ---------- " + randomKeys.get(i));
                }
                return randomKeys;


            } catch (StardogException| IOException e) {
                e.printStackTrace();
            } finally {
                releaseConnection(connectionPool, connection);
                connectionPool.shutdown();
            }
        }

        return null;
    }



    public static void main(String[] args) {
        ConnectionPool connectionPool = StardogConnection.connection();
        generateKeys(6, connectionPool);

    }
}
