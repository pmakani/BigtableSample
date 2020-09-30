package com.qlogic;

import com.google.api.gax.rpc.NotFoundException;
import com.google.api.gax.rpc.ServerStream;
import com.google.cloud.ServiceOptions;
import com.google.cloud.bigtable.admin.v2.BigtableTableAdminClient;
import com.google.cloud.bigtable.admin.v2.BigtableTableAdminSettings;
import com.google.cloud.bigtable.admin.v2.models.CreateTableRequest;
import com.google.cloud.bigtable.data.v2.BigtableDataClient;
import com.google.cloud.bigtable.data.v2.BigtableDataSettings;
import com.google.cloud.bigtable.data.v2.models.Query;
import com.google.cloud.bigtable.data.v2.models.Row;
import com.google.cloud.bigtable.data.v2.models.RowCell;
import com.google.cloud.bigtable.data.v2.models.RowMutation;

import java.io.IOException;

public class BigtableSample {

  private static final String PROJECT_ID = ServiceOptions.getDefaultProjectId();
  private static final String INSTANCE_ID = "bigquery-sample-instance";
  private static final String FAMILY = "us-states";
  private static final String COLUMN_NAME = "name";
  private static final String COLUMN_POST_ABBR = "post_abbr";
  private static final String ROW_KEY_PREFIX = "rowKey";
  private final String tableId = "test-bigquery-sample";
  private BigtableDataClient dataClient;
  private BigtableTableAdminClient adminClient;

  public static void main(String[] args) throws IOException {
    BigtableSample sample = new BigtableSample();
    sample.createTable();
    sample.writeToTable();
    sample.readTable();
  }

  public BigtableSample() throws IOException {
    BigtableDataSettings settings =
        BigtableDataSettings.newBuilder()
            .setProjectId(PROJECT_ID)
            .setInstanceId(INSTANCE_ID)
            .build();

    // Creates a bigtable data client.
    this.dataClient = BigtableDataClient.create(settings);

    // Creates the settings to configure a bigtable table admin client.
    BigtableTableAdminSettings adminSettings =
        BigtableTableAdminSettings.newBuilder()
            .setProjectId(PROJECT_ID)
            .setInstanceId(INSTANCE_ID)
            .build();

    // Creates a bigtable table admin client.
    this.adminClient = BigtableTableAdminClient.create(adminSettings);
  }

  public void createTable() {
    if (!adminClient.exists(tableId)) {
      System.out.println("Creating table: " + tableId);
      CreateTableRequest createTableRequest = CreateTableRequest.of(tableId).addFamily(FAMILY);
      adminClient.createTable(createTableRequest);
      System.out.printf("Table %s created successfully%n", tableId);
    }
  }

  public void writeToTable() {
    try {
      System.out.println("\nWriting some greetings to the table");
      String[] names = {
        "Alabama",
        "Alaska",
        "Arizona",
        "Arkansas",
        "California",
        "Colorado",
        "Connecticut",
        "Delaware",
        "Florida",
        "Georgia",
        "Hawaii",
        "Idaho",
        "Illinois",
        "Indiana",
        "Iowa",
        "Kansas",
        "Kentucky",
        "Louisiana",
        "Maine",
        "Maryland",
        "Massachusetts",
        "Michigan",
        "Minnesota",
        "Mississippi",
        "Missouri",
        "Montana",
        "Nebraska",
        "Nevada",
        "New Hampshire",
        "New Jersey",
        "New Mexico",
        "New York",
        "North Carolina",
        "North Dakota",
        "Ohio",
        "Oklahoma",
        "Oregon",
        "Pennsylvania",
        "Rhode Island",
        "South Carolina",
        "SouthDakota",
        "Tennessee",
        "Texas",
        "Utah",
        "Vermont",
        "Virginia",
        "Washington",
        "West Virginia",
        "Wisconsin",
        "Wyoming"
      };
      String[] post_abbr = {
        "AL", "AK", "AZ", "AR", "CA", "CO", "CT", "DE", "FL", "GA", "HI", "ID", "IL", "IN", "IA",
        "KS", "KY", "LA", "ME", "MD", "MA", "MI", "MN", "MS", "MO", "MT", "NE", "NV", "NH", "NJ",
        "NM", "NY", "NC", "ND", "OH", "OK", "OR", "PA", "RI", "SC", "SD", "TN", "TX", "UT", "VT",
        "VA", "WA", "WV", "WI", "WY"
      };
      for (int i = 0; i < names.length; i++) {
        RowMutation rowMutation =
            RowMutation.create(tableId, ROW_KEY_PREFIX + i)
                .setCell(FAMILY, COLUMN_NAME, names[i])
                .setCell(FAMILY, COLUMN_POST_ABBR, post_abbr[i]);
        dataClient.mutateRow(rowMutation);
        System.out.printf("Name :%s and State:%s\n", names[i], post_abbr[i]);
      }
    } catch (NotFoundException e) {
      System.err.println("Failed to write to non-existent table: " + e.getMessage());
    }
  }

  public void readTable() {
    try {
      System.out.println("\nReading the entire table");
      Query query = Query.create(tableId);
      ServerStream<Row> rowStream = dataClient.readRows(query);
      for (Row r : rowStream) {
        System.out.println("Row Key: " + r.getKey().toStringUtf8());
        for (RowCell cell : r.getCells()) {
          System.out.printf(
              "Family: %s    Qualifier: %s    Value: %s%n",
              cell.getFamily(), cell.getQualifier().toStringUtf8(), cell.getValue().toStringUtf8());
        }
      }
    } catch (NotFoundException e) {
      System.err.println("Failed to read a non-existent table: " + e.getMessage());
    }
  }
}
