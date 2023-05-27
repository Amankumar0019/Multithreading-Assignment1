package Assignment;
import java.io.*;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class Main {
    // Calculating the number of lines in the sales.csv file
    public static int count(String filename) throws IOException {
        InputStream is = new BufferedInputStream(new FileInputStream(filename));
        try {
            byte[] c = new byte[1024];
            int count = 0;
            int readChars = 0;
            boolean empty = true;
            while ((readChars = is.read(c)) != -1) {
                empty = false;
                for (int i = 0; i < readChars; ++i) {
                    if (c[i] == '\n') {
                        ++count;
                    }
                }
            }
            return (count == 0 && !empty) ? 1 : count;
        } finally {
            is.close();
        }
    }
    public static void main(String[] arg) throws IOException, InterruptedException {
        long start=System.currentTimeMillis();
        int sales_size=count("C:\\Users\\aman.kumar1\\Desktop\\assignment1\\sales1.csv")-1;
        int batch_size=sales_size/10;
        System.out.println(batch_size+" "+sales_size);
      //  CountDownLatch latch=new CountDownLatch(10);

        // Creating a thread pool with 10 threads
        ExecutorService executorService = Executors.newFixedThreadPool(10);

        for( int i=0;i<sales_size;i=i+batch_size) {
            executorService.execute(new Runnable() {

                @Override
                public void run() {

                    // Printing the name of the current thread


                    BufferedReader reader = null;
                    try {
                        reader = new BufferedReader(new FileReader("C:\\Users\\aman.kumar1\\Desktop\\assignment1\\salesman1.csv"));
                    } catch (FileNotFoundException e) {
                        throw new RuntimeException(e);
                    }
                    try {
                        String headerRow = reader.readLine();
                    } catch (IOException e) {
                        throw new RuntimeException(e);
                    }
                    String row;
                    PrintWriter writer1 = null;
                    try {
                        // Initialize a PrintWriter object to write the product report file
                        writer1 = new PrintWriter(new File("filePathproductReport.csv"));
                    } catch (FileNotFoundException e) {
                        throw new RuntimeException(e);
                    }
                    writer1.write("Salesman_id,Product,Quantity,Sale_Amount,SalesmanName,SalesmanCommission,Area" + "\n");
                    writer1.flush();

                    PrintWriter writer2 = null;
                    try {
                        // Initialize a PrintWriter object to write the SalesmanCommissionReport file
                        writer2 = new PrintWriter(new File("fileSalesmanCommissionReport.csv"));
                    } catch (FileNotFoundException e) {
                        throw new RuntimeException(e);
                    }
                    writer2.write("Salesman_id,SalesmanName,Total_sales,Total_commission,Area" + "\n");
                    writer2.flush();
                    while (true) {
                        try {
                            if (!((row = reader.readLine()) != null)) break;
                        } catch (IOException e) {
                            throw new RuntimeException(e);
                        }
                        double total_commission = 0;
                        int total_sales = 0;
                        String[] values = row.split(",");
                        int salesmanId = Integer.parseInt(values[0]);
                        String salesmanName = values[1];
                        String salesmanArea = values[2];
                        double commissionRate = Double.parseDouble(values[3]);
                        BufferedReader reader2 = null;
                        try {
                            reader2 = new BufferedReader(new FileReader("C:\\Users\\aman.kumar1\\Desktop\\assignment1\\sales1.csv"));
                        } catch (FileNotFoundException e) {
                            throw new RuntimeException(e);
                        }
                        try {
                            String headerRow2 = reader2.readLine();
                        } catch (IOException e) {
                            throw new RuntimeException(e);
                        }

                        String row2;
                        while (true) {
                            try {
                                if (!((row2 = reader2.readLine()) != null)) break;
                            } catch (IOException e) {
                                throw new RuntimeException(e);
                            }
                            String[] values2 = row2.split(",");

                            String product = values2[0];
                            int quantity = Integer.parseInt(values2[1]);
                            int Mrp_per_unit = Integer.parseInt(values2[2]);
                            double salesmanId2 = Double.parseDouble(values2[3]);
                            double SalesCommission = 0;
                            if (salesmanId == salesmanId2) {
                                SalesCommission = commissionRate * quantity * Mrp_per_unit;

                                String st = salesmanId + "," + product + "," + quantity + "," + quantity * Mrp_per_unit + ", " + salesmanName + "," + SalesCommission + "," + salesmanArea + "\n";
                                System.out.println(Thread.currentThread().getName()+"-->"+st);
                                writer1.write(st);
                                total_commission = total_commission + SalesCommission;
                                total_sales++;
                            }

                        }

                        String st2 = salesmanId + "," + salesmanName + "," + total_sales + "," + total_commission + "," + salesmanArea + "\n";
                        writer2.write(st2);
                    }
                    writer1.close();
                    writer2.close();
                    System.out.println(Thread.currentThread().getName());
                  //  latch.countDown();
                    }

                });

        }
     //  latch.await();
        executorService.shutdown();
        long end=System.currentTimeMillis();
        System.out.println("current running time"+(end-start));

    }
}