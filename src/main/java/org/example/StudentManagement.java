package org.example;

import java.io.IOException;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.List;
import java.util.Scanner;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

public class StudentManagement {
    public static void main(String[] args) throws IOException {
        Scanner scanner = new Scanner(System.in);
        int input = 0;
        do {
            System.out.println("\n\n Hello in our students management application using Java and HBase \n" +
                    "Choose your action\n" +
                    "1- Get all students \n" +
                    "2- Add new student\n" +
                    "3- Edit student\n" +
                    "4- Delete student\n" +
                    "5- Exit from the app\n\n" +
                    "Please Enter the number of your action");

            input = scanner.nextInt();

            switch (input) {
                case 1:
                    getAllStudents();
                    break;
                case 2:
                    addNewStudent();
                    break;
                case 3:
                    editStudent();
                    break;
                case 4:
                    deleteStudent();
                    break;
                default:
                    if (input != 5) System.out.println("Please choose a valid option!!");
                    break;
            }

        } while (input != 5);

        System.out.println("See you soon!!");
    }

    private static void addNewStudent() throws IOException {
        boolean insertion = true;
        String family_column;
        List<List<String>> data = new ArrayList<>();
        Scanner scanner = new Scanner(System.in);

        System.out.println("\n\n\n ====================== Welcome in our insertion form ==================== \n\n\n");
        System.out.println("We have two family columns 1-personal and 2-academic");

        while (insertion) {
            System.out.println("choose the family columns \n 1-personal \n 2-academic");
            int fc = scanner.nextInt();
            if (fc == 1) {
                family_column = "personal";
            }else{
                family_column = "academic";
            }
            // System.out.println("entre the family column");
            scanner.nextLine();
            System.out.println("entre the column name");
            String column_name = scanner.nextLine();
            System.out.println("entre the value");
            String value = scanner.nextLine();
            List<String> row1 = new ArrayList<>();
            row1.add(family_column);
            row1.add(column_name);
            row1.add(value);
            data.add(row1);
            System.out.println("Continue insertion [Y/N]");
            String isInsertion = scanner.nextLine();

            if (isInsertion.equalsIgnoreCase("Y")) {
                insertion = true;
            } else {
                System.out.println("Insertion canceled.");
                insertion = false;
            }
        }
        // Instantiating Configuration class
        Configuration config = HBaseConfiguration.create();
        //
        Connection connection = ConnectionFactory.createConnection(config);
        System.out.println("connected");

        Table hTable = connection.getTable(TableName.valueOf("student"));

        // Instantiating Put class
        // accepts a row name.
        Put p = new Put(Bytes.toBytes(String.valueOf(LocalDateTime.now())));
        for (int i = 0; i <
             data.size(); i++) {
            p.addColumn(
                    Bytes.toBytes(data.get(i).get(0)),
                    Bytes.toBytes(data.get(i).get(1)),
                    Bytes.toBytes(data.get(i).get(2))
            );
        }
        // Saving the put Instance to the HTable.
        hTable.put(p);
        System.out.println("\n\n\n ============= data inserted ============== \n");
        // closing HTable
        hTable.close();
        connection.close();

    }

    private static void getAllStudents() throws IOException {
        // Instantiating Configuration class
        Configuration config = HBaseConfiguration.create();

        // Creating a connection
        Connection connection = ConnectionFactory.createConnection(config);

        // Creating a table object
        Table table = connection.getTable(TableName.valueOf("student"));

        // Creating a Scan object
        Scan scan = new Scan();

        // Getting the scan result
        ResultScanner scanner = table.getScanner(scan);

        // Iterating over the scan result and printing each row
        System.out.println("\n================= All Students =================");

        for (Result result : scanner) {
            System.out.println("Row key: " + Bytes.toString(result.getRow()));
            for (Cell cell : result.rawCells()) {
                System.out.println("  Family: " + Bytes.toString(CellUtil.cloneFamily(cell)) +
                        "  Qualifier: " + Bytes.toString(CellUtil.cloneQualifier(cell)) +
                        "  Value: " + Bytes.toString(CellUtil.cloneValue(cell)));
            }
            System.out.println("---------------------------------------------");
        }

        // Closing the ResultScanner
        scanner.close();

        // Closing the table and connection
        table.close();
        connection.close();
    }


    private static void editStudent() throws IOException {
        Scanner scanner = new Scanner(System.in);

        // Prompt the user for the row key of the student to edit
        System.out.println("Enter the row key of the student to edit:");
        String rowKey = scanner.nextLine();

        // Instantiating Configuration class
        Configuration config = HBaseConfiguration.create();

        // Creating a connection
        Connection connection = ConnectionFactory.createConnection(config);

        // Creating a table object
        Table table = connection.getTable(TableName.valueOf("student"));

        // Creating a Get object to retrieve the existing student data
        Get get = new Get(Bytes.toBytes(rowKey));

        // Getting the existing student data
        Result result = table.get(get);

        // Check if the student exists
        if (result.isEmpty()) {
            System.out.println("Student not found with the provided row key.");
        } else {
            // Display the existing student data
            System.out.println("\n================= Existing Student Data =================");
            System.out.println("Row key: " + Bytes.toString(result.getRow()));
            for (Cell cell : result.rawCells()) {
                System.out.println("  Family: " + Bytes.toString(CellUtil.cloneFamily(cell)) +
                        "  Qualifier: " + Bytes.toString(CellUtil.cloneQualifier(cell)) +
                        "  Value: " + Bytes.toString(CellUtil.cloneValue(cell)));
            }

            // Prompt the user for the column family, column qualifier, and new value to update
            System.out.println("\nEnter the new data for the student:");

            System.out.print("Family column: ");
            String familyColumn = scanner.nextLine();

            System.out.print("Column qualifier: ");
            String columnQualifier = scanner.nextLine();

            System.out.print("New value: ");
            String newValue = scanner.nextLine();

            // Creating a Put object to update the student data
            Put put = new Put(Bytes.toBytes(rowKey));
            put.addColumn(Bytes.toBytes(familyColumn), Bytes.toBytes(columnQualifier), Bytes.toBytes(newValue));

            // Updating the student data
            table.put(put);

            System.out.println("\n================= Student Updated Successfully =================");
        }

        // Closing the table and connection
        table.close();
        connection.close();
    }


    private static void deleteStudent() throws IOException {
        Scanner scanner = new Scanner(System.in);

        // Prompt the user for the row key of the student to delete
        System.out.println("Enter the row key of the student to delete:");
        String rowKey = scanner.nextLine();

        // Ask the user whether to delete the entire row or a specific column
        System.out.println("Do you want to delete the entire row? (Y/N)");
        String deleteOption = scanner.nextLine();

        // Instantiating Configuration class
        Configuration config = HBaseConfiguration.create();

        // Creating a connection
        Connection connection = ConnectionFactory.createConnection(config);

        // Creating a table object
        Table table = connection.getTable(TableName.valueOf("student"));

        if (deleteOption.equalsIgnoreCase("Y")) {
            // Creating a Delete object to remove the entire row
            Delete delete = new Delete(Bytes.toBytes(rowKey));

            // Deleting the entire row
            table.delete(delete);
            System.out.println("\n================= Entire Row Deleted Successfully =================");
        } else {
            // Prompt the user for the column family and column qualifier
            System.out.println("Enter the column family:");
            String columnFamily = scanner.nextLine();

            System.out.println("Enter the column qualifier:");
            String columnQualifier = scanner.nextLine();

            // Creating a Delete object to remove a specific column
            Delete delete = new Delete(Bytes.toBytes(rowKey));
            delete.addColumn(Bytes.toBytes(columnFamily), Bytes.toBytes(columnQualifier));

            // Deleting the specific column
            table.delete(delete);
            System.out.println("\n================= Specific Column Deleted Successfully =================");
        }

        // Closing the table and connection
        table.close();
        connection.close();
    }



    private static void DynamicDataInsertion(List<List<String>> Data) throws IOException {
        //row1,col1,,,,,

        // Instantiating Configuration class
        Configuration config = HBaseConfiguration.create();
        //
        Connection connection = ConnectionFactory.createConnection(config);
        System.out.println("connected");

        Table hTable = connection.getTable(TableName.valueOf("student"));

        // Instantiating Put class
        // accepts a row name.
        Put p = new Put(Bytes.toBytes(String.valueOf(LocalDateTime.now())));
        for (int i = 0; i < Data.size(); i++) {
            System.out.println("===========================hhhhh===================");
            p.addColumn(
                    Bytes.toBytes(Data.get(i).get(0)),
                    Bytes.toBytes(Data.get(i).get(1)),
                    Bytes.toBytes(Data.get(i).get(2))
            );
        }
        // Saving the put Instance to the HTable.
        hTable.put(p);
        System.out.println("\n\n\n ============= data inserted ============== \n");
        // closing HTable
        hTable.close();
        connection.close();
    }

    private static void NewinsertData(String first_name, String last_name, String filier, String _bac_year) throws IOException {
        //row1,col1,,,,,

        // Instantiating Configuration class
        Configuration config = HBaseConfiguration.create();
        //
        Connection connection = ConnectionFactory.createConnection(config);
        System.out.println("connected");

        Table hTable = connection.getTable(TableName.valueOf("commande"));

        // Instantiating HTable class
        //HTable hTable = new HTable(config, "emp");

        // Instantiating Put class
        // accepts a row name.
        Put p = new Put(Bytes.toBytes(last_name + _bac_year + filier));

        // adding values using add() method
        // accepts column family name, qualifier/row name ,value
        p.addColumn(
                Bytes.toBytes("clients"),
                Bytes.toBytes("First_Name"),
                Bytes.toBytes(first_name)
        );

        p.addColumn(
                Bytes.toBytes("clients"),
                Bytes.toBytes("Second_Name"),
                Bytes.toBytes(last_name)
        );


        p.addColumn(
                Bytes.toBytes("produits"),
                Bytes.toBytes("Fileire"),
                Bytes.toBytes(filier)
        );

        p.addColumn(
                Bytes.toBytes("produits"),
                Bytes.toBytes("Annee_Bac"),
                Bytes.toBytes(_bac_year)
        );


        // Saving the put Instance to the HTable.
        hTable.put(p);
        System.out.println("\n\n\n ============= inserted data ============== \n");
        System.out.println("First name: " + first_name + "\n" + "Last name: " + last_name + "\n" + "Filier: " + filier + "\n" + "BAC year: " + _bac_year);
        System.out.println("\n\n\n");
        // closing HTable
        hTable.close();
        connection.close();
    }

    private static void insertData() throws IOException {
        //row1,col1,,,,,

        // Instantiating Configuration class
        Configuration config = HBaseConfiguration.create();

        //
        Connection connection = ConnectionFactory.createConnection(config);
        System.out.println("connected");

        Table hTable = connection.getTable(TableName.valueOf("commande"));

        // Instantiating HTable class
        //HTable hTable = new HTable(config, "emp");

        // Instantiating Put class
        // accepts a row name.
        Put p = new Put(Bytes.toBytes("row3"));

        // adding values using add() method
        // accepts column family name, qualifier/row name ,value
        p.addColumn(
                Bytes.toBytes("clients"),
                Bytes.toBytes("First_Name"),
                Bytes.toBytes("Sabado")
        );

        p.addColumn(
                Bytes.toBytes("clients"),
                Bytes.toBytes("Second_Name"),
                Bytes.toBytes("GREAT")
        );


        p.addColumn(
                Bytes.toBytes("produits"),
                Bytes.toBytes("Fileire"),
                Bytes.toBytes("GLAASRI")
        );

        p.addColumn(
                Bytes.toBytes("produits"),
                Bytes.toBytes("Annee_Bac"),
                Bytes.toBytes("2014")
        );


        // Saving the put Instance to the HTable.
        hTable.put(p);
        System.out.println("data inserted");

        // closing HTable
        hTable.close();
        connection.close();
    }
}