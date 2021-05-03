package com.scylladb.cdc.printer;

import com.google.common.io.BaseEncoding;
import com.scylladb.cdc.lib.CDCConsumer;
import com.scylladb.cdc.lib.RawChangeConsumerProvider;
import com.scylladb.cdc.model.StreamId;
import com.scylladb.cdc.model.TableName;
import com.scylladb.cdc.model.worker.*;
import com.scylladb.cdc.model.worker.cql.Cell;
import com.scylladb.cdc.model.worker.cql.ProcessedUpdateCell;
import net.sourceforge.argparse4j.ArgumentParsers;
import net.sourceforge.argparse4j.inf.ArgumentParser;
import net.sourceforge.argparse4j.inf.ArgumentParserException;
import net.sourceforge.argparse4j.inf.Namespace;

import java.text.SimpleDateFormat;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;

public class Main {
    public static void main(String[] args) {
        // Get connection info and desired table
        // from command line arguments.
        Namespace parsedArguments = parseArguments(args);
        String source = parsedArguments.getString("source");
        String keyspace = parsedArguments.getString("keyspace"), table = parsedArguments.getString("table");

        // Build a provider of consumers. The CDCConsumer instance
        // can be run in multi-thread setting and a separate
        // RawChangeConsumer is used by each thread.
        RawChangeConsumerProvider changeConsumerProvider = threadId -> {
            // Build a consumer of changes. You should provide
            // a class that implements the RawChangeConsumer
            // interface.
            //
            // Here, we use a lambda for simplicity.
            //
            // The consume() method of RawChangeConsumer returns
            // a CompletableFuture, so your code can perform
            // some I/O responding to the change.
            //
            // The RawChange name alludes to the fact that
            // changes represented by this class correspond
            // 1:1 to rows in *_scylla_cdc_log table.
            RawChangeConsumer changeConsumer = change -> {
                // Print the change. See printChange()
                // for more information on how to
                // access its details.
                printChange(change);
                return CompletableFuture.completedFuture(null);
            };
            return changeConsumer;
        };

        // Build a CDCConsumer, which is single-threaded
        // (workersCount(1)), reads changes
        // from [keyspace].[table] and passes them
        // to consumers created by changeConsumerProvider.
        try (CDCConsumer consumer = CDCConsumer.builder()
                .addContactPoint(source)
                .addTable(new TableName(keyspace, table))
                .withConsumerProvider(changeConsumerProvider)
                .withWorkersCount(1)
                .build()) {

            // Start a consumer. You can stop it by using .stop() method
            // or it can be automatically stopped when created in a
            // try-with-resources (as shown above).
            consumer.start();

            // The consumer is started in background threads.
            // It is consuming the CDC log and providing read changes
            // to the consumers.

            // Wait for SIGINT:
            CountDownLatch terminationLatch = new CountDownLatch(1);
            Thread.sleep(100000);
        } catch (InterruptedException ex) {
            System.err.println("Exception occurred while running the Printer: "
                + ex.getMessage());
        }

        // The CDCConsumer is gracefully stopped after try-with-resources.
    }

    private static void printChange(RawChange change) {
        if (change.getOperationType() == RawChange.OperationType.ROW_UPDATE) {
            UpdateChange uc = new UpdateChange(null, change, null);
            uc.getPrimaryKeyStream().forEach(pk -> {
                System.out.println("PrimK | " + pk.toString());
            });
            uc.getPartitionKeyStream().forEach(pk -> {
                System.out.println("PK | " + pk.toString());
            });
            uc.getPartitionKeyStream().forEach(pk -> {
                System.out.println("CK | " + pk.toString());
            });
            uc.getCellsStream().forEach(c -> {
                System.out.println("Cell | " + c.toString());
            });
            ProcessedUpdateCell theCell = uc.getCell("v");
            System.out.println(theCell);
            System.out.println(theCell.getUDT().getMutationType());
            switch (theCell.getUDT().getMutationType()) {
                case UPDATE:
                    System.out.println(theCell.getUDT().getUpdatedFields());
                    break;
                case OVERWRITE:
                    System.out.println(theCell.getUDT().getOverwriteFields());
                    break;
            }
            System.out.println("===========");
        }
    }

    // Some pretty printing helpers:

    private static void prettyPrintChangeHeader(StreamId streamId, ChangeTime changeTime,
                                                RawChange.OperationType operationType) {
        byte[] buf = new byte[16];
        streamId.getValue().duplicate().get(buf, 0, 16);

        System.out.println("┌────────────────── Scylla CDC log row ──────────────────┐");
        prettyPrintField("Stream id:", BaseEncoding.base16().encode(buf, 0, 16));
        prettyPrintField("Timestamp:", new SimpleDateFormat("dd/MM/yyyy, HH:mm:ss.SSS").format(changeTime.getDate()));
        prettyPrintField("Operation type:", operationType.name());
        System.out.println("├────────────────────────────────────────────────────────┤");
    }

    private static void prettyPrintCell(String columnName, ChangeSchema.ColumnType baseTableColumnType, ChangeSchema.DataType logDataType, Object cellValue, boolean isLast) {
        prettyPrintField(columnName + ":", Objects.toString(cellValue));
        prettyPrintField(columnName + " (schema):", columnName + ", " + logDataType.toString() + ", " + baseTableColumnType.name());
        if (!isLast) {
            prettyPrintField("", "");
        }
    }

    private static void prettyPrintEnd() {
        System.out.println("└────────────────────────────────────────────────────────┘");
        System.out.println();
    }

    private static void prettyPrintField(String fieldName, String fieldValue) {
        System.out.print("│ " + fieldName + " ");

        int leftSpace = 53 - fieldName.length();

        if (fieldValue.length() > leftSpace) {
            fieldValue = fieldValue.substring(0, leftSpace - 3) + "...";
        }

        fieldValue = String.format("%1$" + leftSpace + "s", fieldValue);
        System.out.println(fieldValue + " │");
    }

    // Parsing the command-line arguments:

    private static Namespace parseArguments(String[] args) {
        ArgumentParser parser = ArgumentParsers.newFor("./scylla-cdc-printer").build().defaultHelp(true);
        parser.addArgument("-k", "--keyspace").required(true).help("Keyspace name");
        parser.addArgument("-t", "--table").required(true).help("Table name");
        parser.addArgument("-s", "--source").required(true)
                .setDefault("127.0.0.1").help("Address of a node in source cluster");

        try {
            return parser.parseArgs(args);
        } catch (ArgumentParserException e) {
            parser.handleError(e);
            System.exit(-1);
            return null;
        }
    }
}
