package io.mhmtonrn;

import io.mhmtonrn.event.CDCEvent;
import org.json.JSONObject;
import org.postgresql.PGConnection;
import org.postgresql.PGProperty;
import org.postgresql.replication.LogSequenceNumber;
import org.postgresql.replication.PGReplicationStream;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.CommandLineRunner;
import org.springframework.context.ApplicationEventPublisher;
import org.springframework.stereotype.Service;

import java.io.ByteArrayOutputStream;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.*;
import java.util.concurrent.TimeUnit;

@Service
public class ReplicationListener implements CommandLineRunner {
    @Value("${replication.db.url}")
    private String url;

    @Value("${replication.db.username}")
    private String username;

    @Value("${replication.db.password}")
    private String password;

    @Value("${replication.db.slot}")
    private String slot;

    @Value("${replication.db.publication}")
    private String publication;
    private final ApplicationEventPublisher applicationEventPublisher;

    private final List<ReplicationEventHandler> handlers = new ArrayList<>();

    public ReplicationListener(ApplicationEventPublisher applicationEventPublisher) {
        this.applicationEventPublisher = applicationEventPublisher;
        Map<Integer, RelationInfo> relationMap = new HashMap<>();
        handlers.add(new RelationHandler(relationMap));
        handlers.add(new InsertHandler(relationMap));
        handlers.add(new UpdateHandler(relationMap));
        handlers.add(new DeleteHandler(relationMap));
        handlers.add(new CommitHandler());
        handlers.add(new BeginHandler());
    }

    @Override
    public void run(String... args) {
        new Thread(() -> {
            try {
                listenLoop();
            } catch (SQLException e) {
                throw new RuntimeException(e);
            }
        }).start();

    }

    private void listenLoop() throws SQLException {
        PGReplicationStream stream = createStream();
        int errorCount = 0;

        while (true) {
            try {
                ByteBuffer buffer = stream.readPending();
                if (buffer == null) {
                    Thread.sleep(10);
                    continue;
                }
                while (buffer.hasRemaining()) {
                    char tag = (char) buffer.get();
                    for (ReplicationEventHandler handler : handlers) {
                        if (handler.canHandle(tag)) {
                            JSONObject event = handler.handle(buffer);
                            if (event != null) {
                                applicationEventPublisher.publishEvent(new CDCEvent(this,event.toString()));
                            }
                            break;
                        }
                    }
                }
                updateLSN(stream);
                errorCount = 0; // reset on success
            } catch (Exception e) {
                e.printStackTrace();
                errorCount++;
                if (errorCount >= 3) {
                    System.err.println("Error threshold reached. Restarting replication stream...");
                    try {
                        stream.close();
                    } catch (Exception ex) {
                        ex.printStackTrace();
                    }
                    stream = createStream();
                    errorCount = 0;
                }
            }
        }
    }

    private PGReplicationStream createStream() throws SQLException {
        Properties props = new Properties();
        PGProperty.USER.set(props, username);
        PGProperty.PASSWORD.set(props, password);
        PGProperty.ASSUME_MIN_SERVER_VERSION.set(props, "15.4");
        PGProperty.REPLICATION.set(props, "postgres");
        PGProperty.PREFER_QUERY_MODE.set(props, "simple");
        props.setProperty("characterEncoding", "UTF-8");
        Connection conn = DriverManager.getConnection(url, props);
        PGConnection pgConn = conn.unwrap(PGConnection.class);
        return pgConn.getReplicationAPI()
                .replicationStream()
                .logical()
                .withSlotName(slot)
                .withSlotOption("proto_version", 1)
                .withSlotOption("publication_names", publication)
                .withStatusInterval(120, TimeUnit.SECONDS)
                .start();
    }

    private void updateLSN(PGReplicationStream stream) throws SQLException {
        LogSequenceNumber lsn = stream.getLastReceiveLSN();
        stream.setAppliedLSN(lsn);
        stream.setFlushedLSN(lsn);
        stream.forceUpdateStatus();
    }

    // Common utilities
    static String readString(ByteBuffer buffer) {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        byte b;
        while ((b = buffer.get()) != 0) out.write(b);
        return out.toString(StandardCharsets.UTF_8);
    }

    static Map<String, String> parseTuple(ByteBuffer buffer, List<ColumnInfo> columns) {
        short colCount = buffer.getShort();
        Map<String, String> row = new LinkedHashMap<>();
        for (int i = 0; i < colCount; i++) {
            byte fmt = buffer.get();
            String col = columns.get(i).name();
            if (fmt == 'n') {
                row.put(col, null);
            } else {
                int len = buffer.getInt();
                byte[] data = new byte[len];
                buffer.get(data);
                row.put(col, new String(data, StandardCharsets.UTF_8));
            }
        }
        return row;
    }
}

interface ReplicationEventHandler {
    boolean canHandle(char tag);
    JSONObject handle(ByteBuffer buffer);
}

class RelationHandler implements ReplicationEventHandler {
    private final Map<Integer, RelationInfo> relationMap;
    RelationHandler(Map<Integer, RelationInfo> map) { this.relationMap = map; }
    public boolean canHandle(char tag) { return tag == 'R'; }
    public JSONObject handle(ByteBuffer buffer) {
        int relId = buffer.getInt();
        String ns = ReplicationListener.readString(buffer);
        String name = ReplicationListener.readString(buffer);
        buffer.get();
        short colCount = buffer.getShort();
        List<ColumnInfo> cols = new ArrayList<>();
        for (int i = 0; i < colCount; i++) {
            buffer.get();
            String colName = ReplicationListener.readString(buffer);
            int oid = buffer.getInt();
            buffer.getInt();
            cols.add(new ColumnInfo(colName, oid));
        }
        relationMap.put(relId, new RelationInfo(relId, ns, name, cols));
        return null;
    }
}

class InsertHandler implements ReplicationEventHandler {
    private final Map<Integer, RelationInfo> relationMap;
    InsertHandler(Map<Integer, RelationInfo> map) { this.relationMap = map; }
    public boolean canHandle(char tag) { return tag == 'I'; }
    public JSONObject handle(ByteBuffer buffer) {
        int relId = buffer.getInt();
        RelationInfo rel = relationMap.get(relId);
        buffer.get();
        JSONObject data = new JSONObject(ReplicationListener.parseTuple(buffer, rel.columns()));
        return new JSONObject().put("type","insert").put("table",rel.name()).put("data",data);
    }
}

class UpdateHandler implements ReplicationEventHandler {
    private final Map<Integer, RelationInfo> relationMap;
    UpdateHandler(Map<Integer, RelationInfo> map) { this.relationMap = map; }
    public boolean canHandle(char tag) { return tag == 'U'; }
    public JSONObject handle(ByteBuffer buffer) {
        int relId = buffer.getInt();
        RelationInfo rel = relationMap.get(relId);
        byte m = buffer.get(); if (m=='K') { skip(buffer); m = buffer.get(); }
        Map<String,String> oldR = null;
        if (m=='O') { oldR = ReplicationListener.parseTuple(buffer, rel.columns()); m = buffer.get(); }
        if (m!='N') throw new IllegalStateException();
        Map<String,String> newR = ReplicationListener.parseTuple(buffer, rel.columns());
        return new JSONObject().put("type","update").put("table",rel.name())
                .put("old", oldR==null? JSONObject.NULL : new JSONObject(oldR))
                .put("new", new JSONObject(newR));
    }
    private void skip(ByteBuffer b) {
        short c = b.getShort(); for(int i=0;i<c;i++){ if(b.get()!='n'){int l=b.getInt();b.position(b.position()+l);} }
    }
}

class DeleteHandler implements ReplicationEventHandler {
    private final Map<Integer, RelationInfo> relationMap;
    DeleteHandler(Map<Integer, RelationInfo> map) { this.relationMap = map; }
    public boolean canHandle(char tag) { return tag == 'D'; }
    public JSONObject handle(ByteBuffer buffer) {
        int relId = buffer.getInt();
        RelationInfo rel = relationMap.get(relId);
        buffer.get();
        Map<String,String> oldR = ReplicationListener.parseTuple(buffer, rel.columns());
        return new JSONObject().put("type","delete").put("table",rel.name())
                .put("old", new JSONObject(oldR));
    }
}

class BeginHandler implements ReplicationEventHandler {
    public boolean canHandle(char tag) { return tag == 'B'; }
    public JSONObject handle(ByteBuffer buffer) {
        buffer.getLong(); return null;
    }
}

class CommitHandler implements ReplicationEventHandler {
    public boolean canHandle(char tag) { return tag == 'C'; }
    public JSONObject handle(ByteBuffer buffer) {
        buffer.get(); long lsn=buffer.getLong(); buffer.getLong();
        long ts=buffer.getLong();
        return new JSONObject().put("type","commit").put("lsn",lsn).put("timestamp",ts);
    }
}

