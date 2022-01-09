package ceng.ceng351.cengvacdb;

import java.sql.*;
import java.util.ArrayList;

public class CENGVACDB implements ICENGVACDB{
    private static final String url = "jdbc:mysql://144.122.71.121:8080/db2380806?useSSL=false";
    private static final String username = "e2380806";
    private static final String password = "kt3cbG4K#gjv";

    private Connection conn;

    @Override
    public void initialize() {
        try {
            this.conn = DriverManager.getConnection(url, username, password);
        }
        catch (SQLException e) {
            e.printStackTrace();
        }
    }

    @Override
    public int createTables() {
        String TABLE_User = "CREATE TABLE User(" +
                "userID INT," +
                "userName VARCHAR(30)," +
                "age INT," +
                "address VARCHAR(150)," +
                "password VARCHAR(30)," +
                "status VARCHAR(15)," +
                "PRIMARY KEY(userID)" +
                ");";
        String TABLE_Vaccine = "CREATE TABLE Vaccine(" +
                "code INT," +
                "vaccinename VARCHAR(30)," +
                "type VARCHAR(30)," +
                "PRIMARY KEY(code)" +
                ");";

        String TABLE_Vaccination = "CREATE TABLE Vaccination(" +
                "code INT," +
                "userID INT," +
                "dose INT," +
                "vacdate DATE," +
                "PRIMARY KEY (code, userID)," +
                "FOREIGN KEY (code) REFERENCES Vaccine(code) ON DELETE CASCADE," +
                "FOREIGN KEY (userID) REFERENCES User(userID) ON DELETE CASCADE" +
                ");";
        String TABLE_AllergicSideEffect = "CREATE TABLE AllergicSideEffect(" +
                "effectcode INT," +
                "effectname VARCHAR(50)," +
                "PRIMARY KEY (effectcode)" +
                ")";
        String TABLE_Seen = "CREATE TABLE Seen(" +
                "effectcode INT," +
                "code INT," +
                "userID INT," +
                "date DATE," +
                "degree VARCHAR(30)," +
                "PRIMARY KEY (effectcode, code, userID)," +
                "FOREIGN KEY (effectcode) REFERENCES AllergicSideEffect(effectcode) ON DELETE CASCADE," +
                "FOREIGN KEY (code) REFERENCES Vaccine(code) ON DELETE CASCADE," +
                "FOREIGN KEY (userID) REFERENCES User(userID) ON DELETE CASCADE" +
                ");";

        String[] listOfTables = new String[]{TABLE_User, TABLE_Vaccine, TABLE_Vaccination,
                TABLE_AllergicSideEffect, TABLE_Seen};
        int numberOfTables = 0;
        for(int i=0; i<5;i++){
            String request = listOfTables[i];
            try {
                this.conn.prepareStatement(request).execute();
                numberOfTables+=1;
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }

        return numberOfTables;
    }

    @Override
    public int dropTables() {
        int dropped = 0;
        String[] listOfTables = new String[]{"Seen", "AllergicSideEffect", "Vaccination", "Vaccine", "User"};
        for(int i=0; i<5; i++){
            String request = "DROP TABLE " + listOfTables[i] + ";";
            try {
                this.conn.prepareStatement(request).execute();
                dropped+=1;
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }

        return dropped;
    }

    @Override
    public int insertUser(User[] users) {
        int inserted = 0;
        for(int i=0;i<users.length;i++){
            try {
                User temp = users[i];
                String request = "INSERT IGNORE INTO User (userID, userName, age, address, password, status)" +
                        "VALUES (?, ?, ?, ?, ?, ?);";
                PreparedStatement st = this.conn.prepareStatement(request);
                st.setInt(1,temp.getUserID());
                st.setString(2,temp.getUserName());
                st.setInt(3,temp.getAge());
                st.setString(4,temp.getAddress());
                st.setString(5,temp.getPassword());
                st.setString(6,temp.getStatus());
                st.execute();
                inserted+=1;
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
        return inserted;
    }

    @Override
    public int insertAllergicSideEffect(AllergicSideEffect[] sideEffects) {
        int inserted = 0;
        for(int i=0;i<sideEffects.length;i++){
            try {
                AllergicSideEffect temp = sideEffects[i];
                String request = "INSERT IGNORE INTO AllergicSideEffect (effectcode, effectname) VALUES (?, ?);";
                PreparedStatement st = this.conn.prepareStatement(request);
                st.setInt(1,temp.getEffectCode());
                st.setString(2,temp.getEffectName());
                st.execute();
                inserted+=1;
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
        return inserted;
    }

    @Override
    public int insertVaccine(Vaccine[] vaccines) {
        int inserted = 0;
        for(int i=0;i<vaccines.length;i++){
            try {
                Vaccine temp = vaccines[i];
                String request = "INSERT IGNORE INTO Vaccine (code, vaccineName, type) VALUES (?, ?, ?);";
                PreparedStatement st = this.conn.prepareStatement(request);
                st.setInt(1,temp.getCode());
                st.setString(2,temp.getVaccineName());
                st.setString(3,temp.getType());
                st.execute();
                inserted+=1;
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
        return inserted;
    }

    @Override
    public int insertVaccination(Vaccination[] vaccinations) {
        int inserted = 0;
        for(int i=0;i<vaccinations.length;i++){
            try {
                Vaccination temp = vaccinations[i];
                String request = "INSERT IGNORE INTO Vaccination (code, userID, dose, vacdate) VALUES (?, ?, ?, ?);";
                PreparedStatement st = this.conn.prepareStatement(request);
                st.setInt(1,temp.getCode());
                st.setInt(2,temp.getUserID());
                st.setInt(3,temp.getDose());
                st.setString(4, temp.getVacdate());

                st.execute();
                inserted+=1;
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
        return inserted;
    }

    @Override
    public int insertSeen(Seen[] seens) {
        int inserted = 0;
        for(int i=0;i<seens.length;i++){
            try {
                Seen temp = seens[i];
                String request = "INSERT IGNORE INTO Seen (effectcode, code, userID, date, degree) VALUES (?, ?, ?, ?, ?);";
                PreparedStatement st = this.conn.prepareStatement(request);
                st.setInt(1,temp.getEffectcode());
                st.setInt(2,temp.getCode());
                st.setString(3,temp.getUserID());
                st.setString(4, temp.getDate());
                st.setString(5, temp.getDegree());

                st.execute();
                inserted+=1;
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
        return inserted;
    }

    @Override
    public Vaccine[] getVaccinesNotAppliedAnyUser() {
        String query = "SELECT V.code, V.vaccinename, V.type " +
                "FROM Vaccine V " +
                "WHERE V.code NOT IN (SELECT AV.code " +
                "FROM Vaccination AV)";

        try {
            PreparedStatement st = this.conn.prepareStatement(query);
            ResultSet rs = st.executeQuery();
            ArrayList<Vaccine> answer = new ArrayList<>();

            while(rs.next()){
                answer.add(new Vaccine(rs.getInt("code"),
                        rs.getString("vaccinename"),
                        rs.getString("type")));
            }
            return answer.toArray(new Vaccine[0]);
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return new Vaccine[0];
    }

    @Override
    public QueryResult.UserIDuserNameAddressResult[] getVaccinatedUsersforTwoDosesByDate(String vacdate) {
        return new QueryResult.UserIDuserNameAddressResult[0];
    }

    @Override
    public Vaccine[] getTwoRecentVaccinesDoNotContainVac() {
        return new Vaccine[0];
    }

    @Override
    public QueryResult.UserIDuserNameAddressResult[] getUsersAtHasLeastTwoDoseAtMostOneSideEffect() {
        return new QueryResult.UserIDuserNameAddressResult[0];
    }

    @Override
    public QueryResult.UserIDuserNameAddressResult[] getVaccinatedUsersWithAllVaccinesCanCauseGivenSideEffect(String effectname) {
        return new QueryResult.UserIDuserNameAddressResult[0];
    }

    @Override
    public QueryResult.UserIDuserNameAddressResult[] getUsersWithAtLeastTwoDifferentVaccineTypeByGivenInterval(String startdate, String enddate) {
        return new QueryResult.UserIDuserNameAddressResult[0];
    }

    @Override
    public AllergicSideEffect[] getSideEffectsOfUserWhoHaveTwoDosesInLessThanTwentyDays() {
        return new AllergicSideEffect[0];
    }

    @Override
    public double averageNumberofDosesofVaccinatedUserOverSixtyFiveYearsOld() {
        return 0;
    }

    @Override
    public int updateStatusToEligible(String givendate) {
        return 0;
    }

    @Override
    public Vaccine deleteVaccine(String vaccineName) {
        return null;
    }
}
