package pluradj.janusgraph.example;

import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.janusgraph.core.*;
import org.apache.tinkerpop.gremlin.structure.T;
import org.janusgraph.core.schema.ConsistencyModifier;
import org.janusgraph.core.schema.JanusGraphIndex;
import org.janusgraph.core.schema.JanusGraphManagement;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;
import java.util.stream.Stream;

public class JavaExample {
    private static final Logger LOGGER = LoggerFactory.getLogger(JavaExample.class);
    JanusGraph graph;
    JanusGraphTransaction tx;
    JanusGraphManagement mgmt;
    int count = 1;

    public JavaExample () throws Exception {
        graph = JanusGraphFactory.open("conf/janusgraph-cassandra.properties");
        tx = graph.newTransaction();
        mgmt = graph.openManagement();
        //phone_number index
        PropertyKey name = mgmt.makePropertyKey("phone_number1").dataType(String.class).make();
        JanusGraphIndex namei = mgmt.buildIndex("phone_number1", Vertex.class).addKey(name).buildCompositeIndex();
        mgmt.setConsistency(namei, ConsistencyModifier.DEFAULT);
        //name label
        mgmt.makeEdgeLabel("user").make();
        mgmt.commit();

    }


    class Phone {
        public Phone(String phoneNumber) {
            this.phoneNumber = phoneNumber;
        }

        @lombok.Getter
        private String phoneNumber;
    }


    public static void main(String[] args) throws Exception {
        Path path = Paths.get("/Users/nchauhan/junk/query_result_sweden.csv");
        if(args.length > 0) {
            path = Paths.get(args[0]);
        }
        JavaExample exp = new JavaExample();
        exp.ingestion(path);
    }

    public void ingestion(Path path) {
        GraphTraversalSource g = graph.traversal();
        iteratePhoneFile(path);
        //Get me all my contacts

        Long numberOfContact = g.V().has("phone_number1", "23276420376").out("contact").count().next();
        Long count = g.V().count().next();
    }


    public  Vertex addVertexIfNotExist(JanusGraphTransaction graph, Phone phone) {
        GraphTraversalSource g = graph.traversal();
        if(g.V().has("phone_number1", phone.getPhoneNumber()).count().next() == 0) {
            Vertex v =  graph.addVertex((new Object[]{T.label, "user", "phone_number1", phone.getPhoneNumber() }));
            //v.property(VertexProperty.Cardinality.single,"name", phone.getName());
            return v;
        } else {
            return g.V().has("phone_number1", phone.getPhoneNumber()).next();
        }
    }

    private void populateGraph(Phone srcPhone, Phone dstPhone) {

        //add phone as vertex, check if it exist if not create one
        System.out.print("Adding connection" + srcPhone.phoneNumber + " contact " + dstPhone.phoneNumber);
        Vertex sourceUser = addVertexIfNotExist(tx, srcPhone);
        Vertex destUser = addVertexIfNotExist(tx, dstPhone);
        sourceUser.addEdge("contact",destUser, new Object[0]);
    }

    private void parseLine(String line)  {
        if(count%1000 == 0) {
            tx.commit();
            tx = graph.newTransaction();
        } else {
            count++;
        }

        long startTime = System.nanoTime();
        List<String> list = Arrays.asList(line.split("\\s*,\\s*"));
        populateGraph(new Phone(list.get(0)), new Phone(list.get(1)));
        long endTime = System.nanoTime();
        long duration = (endTime - startTime);
        System.out.println("duration :" + duration);
    }


    private void iteratePhoneFile(Path path) {
        try (Stream<String> stream = Files.lines(path)) {
            stream.forEach(line -> parseLine(line));
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
