package transactions;

import java.sql.Connection;

import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Timestamp;
import java.text.DateFormat;
import java.text.FieldPosition;
import java.text.ParseException;
import java.text.ParsePosition;
import java.text.SimpleDateFormat;
import java.util.Scanner;
import java.sql.Date;

public class Transactions {

    private Connection connect() {
        // SQLite connection string
        String url = "jdbc:sqlite:/Users/florapierre/Desktop/Main Folder/College/Data Eng/ATMProject/Database/ATM_Management.db";
        Connection conn = null;
        try {
            conn = DriverManager.getConnection(url);
        } catch (SQLException e) {
            System.out.println(e.getMessage());
        }
        return conn;
    }


    public void insertMem(String mem_fname, String mem_lname, int ssn, Long phone,
        String email, String address, java.util.Date birthdate) {
        String insertMember = "INSERT INTO MEMBER(mem_id,acct_id,mem_fname,"
            + "mem_lname,ssn,phone,email,address,birthdate) values(?,?,?,?,?,?,?,?,?)" ;

        Connection conn = null;
        PreparedStatement ps = null;

        try {
            conn = this.connect();
            if (conn == null)
                return;
            conn.setAutoCommit(false);
            //sets values for statement
            ps = conn.prepareStatement(insertMember);
            ps.setString(3,mem_fname);
            ps.setString(4,mem_lname);
            ps.setInt(5,ssn);
            ps.setLong(6,phone);
            ps.setString(7,email);
            ps.setString(8,address);
            ps.setDate(9,new java.sql.Date(birthdate.getTime()));
            //generates random numbers for ids
            Integer mem_id = (int ) (Math.random() * 125 +
                Math.random() * 50 + 1000);
            Integer acct_id = (int ) (Math.random() * 125 +
                Math.random() * 50 + 1000);
            ps.setInt(1, mem_id);
            ps.setInt(2, acct_id);
            int rows = ps.executeUpdate();
            //if nothing was changed start over
            if(rows != 1)
                conn.rollback();
            //commit to queries
            conn.commit();
            System.out.println("Done!");

        } catch (SQLException e) {
            try {
                if (conn != null) {
                    conn.rollback();
                }
            } catch (SQLException e2) {
                System.out.println(e2.getMessage());
            }
            System.out.println(e.getMessage());
        }
        finally {
            try {
                if (ps != null) {
                    ps.close();
                }
                if (conn != null) {
                    conn.close();
                }
            } catch (SQLException e3) {
                System.out.println(e3.getMessage());
            }
        }

    }
    public void transaction(int acctID, int withdrawal, int atmID) {
        Connection conn = null;
        Statement stmt = null;
        ResultSet rs = null;
        PreparedStatement ps = null;
        ResultSet rs1 = null;
        ResultSet rs2 = null;
        int tranID = 0;
        int memID = 0;
        int bankBalance = 0;
        //string to get balance
        String getBal = "select balance from account where acct_id=" + acctID;
        //string to insert transactions
        String insertTran = "INSERT INTO atm_transaction(tran_id,atm_id,mem_id,"
            + "tran_amount,tran_time) values(?,?,?,?,?)" ;
        //get member ids
        String selectMemID = "Select mem_id from member where acct_id ="+acctID;
        //gets bank balance
        String bankBal = "select balance from atm where atm_id =" +atmID;


        try {
            conn = this.connect();
            if (conn == null)
                return;
            conn.setAutoCommit(false);
            stmt = conn.createStatement();
            rs = stmt.executeQuery(getBal);
            //shows balance
            if(rs.next())
                System.out.println(rs.getInt("balance"));
            //gets member id
            rs1 = stmt.executeQuery(selectMemID);
            if(rs1.next())
                memID = rs.getInt("mem_id");
            //gets bank balance
            rs2 = stmt.executeQuery(bankBal);
            if(rs2.next())
                bankBalance = rs2.getInt("balance");

            tranID = (int ) (Math.random() * 125 +
                Math.random() * 50 + 1000);
            //withdrawals money from bank and acct balance
            String bankWithd = "Update atm set balance = " +(rs.getInt("balance")-withdrawal)
                +"where atm_id = " + atmID;
            String acctWithd = "Update acct set balance = " +(bankBalance-withdrawal)
                +"where acct_id = " + acctID;
            stmt.executeUpdate(bankWithd);
            stmt.executeUpdate(acctWithd);

            ps = conn.prepareStatement(insertTran);
            ps.setInt(1,tranID);
            ps.setInt(2,atmID);
            ps.setInt(3,memID);
            ps.setInt(4,withdrawal);
            ps.setTimestamp(5, new Timestamp(System.currentTimeMillis()));
            int rows = ps.executeUpdate();
            if(rows == 0)
                conn.rollback();
            conn.commit();
            System.out.println(rs.getInt("balance"));
            System.out.println(bankBalance);


        } catch (SQLException e) {
            try {
                if (conn != null) {
                    conn.rollback();
                }
            } catch (SQLException e2) {
                System.out.println(e2.getMessage());
            }
            System.out.println(e.getMessage());
        }
        finally {
            try {
                if (rs != null) {
                    rs.close();
                }
                if (stmt != null) {
                    stmt.close();
                }
                if (conn != null) {
                    conn.close();
                }
            } catch (SQLException e3) {
                System.out.println(e3.getMessage());
            }
        }


    }

    public static void main(String[] args) throws SQLException {
        String uri = "jdbc:sqlite:/Users/florapierre/Desktop/Main Folder/College/Data Eng/ATMProject/Database/ATM_Management.db";
        /*
         * If you need your home directory to build the path (on both Win/*nix)
         * String uri = "dbc:sqlite:" + System.getProperty("user.home") + "Path/to/DB.db";
         */

        String createView ="create view balances as select "
            + "sum(balance) from account  where account.bank_id = bank.bank_id;";

        String insertATMSQL = "INSERT INTO ATM(atm_id, bank_id, atm_location," +
            " location_name, balance, num_of_tran)" +
            " VALUES (?, ?, ?, ?, ?, 0);";

        String firstName;
        String lastName;
        java.util.Date birthdate = null;
        int ssn;
        Long phone;
        String email;
        String address;
//        
        Transactions tran = new Transactions();



        Integer[] BANK_IDS = new Integer[] {1235, 1452, 1466, 1958, 2456,
            2778, 3522, 3588, 4123, 4222, 4555, 4888, 4891, 5489,
            7245, 9875};
        // could also do a sub-select in query but lets assume these are recorded
        String bankchoices = "Please Enter the number of bank from the following:\n";
        for (int i=1; i<BANK_IDS.length; i++) {
            bankchoices += i + ": " + BANK_IDS[i] + "\n";
        }

        int ctr = 0;

        Scanner userInput = new Scanner(System.in);

        /*
         * Transactional processing is also called "Batch Processing" so
         * that's what we are going to do with this. Batch processing even
         * has it's own method with java.sql.
         *
         * Make sure to setAutoCommit to false (to allow for transactions)
         */

        Connection con = DriverManager.getConnection(uri);
        con.setAutoCommit(false);
        PreparedStatement insertATMStmt = con.prepareStatement(insertATMSQL);

        while(true) {
            Integer atmid = (int ) (Math.random() * 125 +
                Math.random() * 50 + 1000);
            try {
                insertATMStmt.setInt(1, atmid);
                // Typically would use UUID.randomUUID() for this

                System.out.println(bankchoices);
                // would typically add error checking based on length
                insertATMStmt.setInt(2, BANK_IDS[userInput.nextInt()]);

                insertATMStmt.setInt(3, atmid + 700);
                System.out.println("Enter the name of the location: \n");
                userInput.nextLine();
                String name = userInput.nextLine();
                insertATMStmt.setString(4, name);

                System.out.println("Enter the starting balance of the ATM: \n");
                int bal = Integer.parseInt(userInput.nextLine());
                insertATMStmt.setInt(5, (bal * 100));

                // here is our new statement which builds a Batch set to run
                insertATMStmt.addBatch();
                ctr++;
            } catch(SQLException e) {
                System.out.println("The batch could not be processed - rolled back to start");
                con.rollback();
                con.close();
                break;
            }
            System.out.println("Do you want to add any other ATMs? (Y/N)");
            if (userInput.next().toLowerCase().startsWith("n")) {
                break;
            }
        }
        if (!con.isClosed()) {
            // Now a final "Hey, are you sure?" message
            System.out.println("There are " + ctr + " rolls waiting to insert. Do you wish to continue? (Y/N)\n");
            String crInput = userInput.next().toLowerCase();

            if (crInput.startsWith("y")) {
                insertATMStmt.executeBatch();
                con.commit();
            } else {
                con.rollback();
            }
            con.close();
        }

        //gets user information

        System.out.println("Enter first name");
        userInput.nextLine();
        firstName = userInput.nextLine();
        System.out.println("Enter last name");

        lastName = userInput.nextLine();
        System.out.println("Enter social security number");

        ssn = userInput.nextInt();
        System.out.println("Enter phone number");

        phone = userInput.nextLong();
        System.out.println("Enter email");
        userInput.nextLine();
        email = userInput.nextLine();
        System.out.println("Enter address");

        address = userInput.nextLine();
        System.out.println("Enter birthdate");

        DateFormat df = new SimpleDateFormat("yyyy-MM-DD");
        try {
            birthdate = df.parse(userInput.nextLine());
        } catch (ParseException e) {
            e.printStackTrace();
            System.out.println("Wrong format");
        }
        System.out.println("Would you like to insert? (Y/N)");
        //commits insert member
        if(userInput.nextLine().toLowerCase().equals("y")) {
            tran.insertMem(firstName,lastName,ssn,phone,email,address,birthdate);
        }
        //gets information for withdrawal
        System.out.println("Enter your account id");
        int accid = userInput.nextInt();
        userInput.nextLine();
        System.out.println("Enter the atm id");
        int atmId = userInput.nextInt();

        System.out.println("Enter withdrawal amount");
        int withdr = userInput.nextInt();

        System.out.println("Would you like to withdrawal? (Y/N)");
        userInput.nextLine();
        //commits withdrawal
        if(userInput.nextLine().toLowerCase().equals("y")) {
            tran.transaction(accid,withdr,atmId);
        }
        userInput.close();
    }

}
