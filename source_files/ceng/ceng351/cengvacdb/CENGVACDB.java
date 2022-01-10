package ceng.ceng351.cengvacdb;

import com.mysql.cj.protocol.Resultset;

import java.sql.*;
import java.util.ArrayList;
import java.util.Arrays;

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
                "PRIMARY KEY (code, userID, dose)," +
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
                String request = "INSERT IGNORE INTO AllergicSideEffect (effectcode, effectname) " +
                        "VALUES (?, ?);";
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
                String request = "INSERT IGNORE INTO Vaccine (code, vaccineName, type) " +
                        "VALUES (?, ?, ?);";
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
                String request = "INSERT IGNORE INTO Vaccination (code, userID, dose, vacdate) " +
                        "VALUES (?, ?, ?, ?);";
                PreparedStatement st = this.conn.prepareStatement(request);
                st.setInt(1,temp.getCode());
                st.setInt(2,temp.getUserID());
                st.setInt(3,temp.getDose());
                st.setDate(4, Date.valueOf(temp.getVacdate()));

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
                String request = "INSERT IGNORE INTO Seen (effectcode, code, userID, date, degree) " +
                        "VALUES (?, ?, ?, ?, ?);";
                PreparedStatement st = this.conn.prepareStatement(request);
                st.setInt(1,temp.getEffectcode());
                st.setInt(2,temp.getCode());
                st.setString(3,temp.getUserID());
                st.setDate(4, Date.valueOf(temp.getDate()));
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
        String query = "SELECT DISTINCT V.code, V.vaccinename, V.type " +
                "FROM Vaccine V " +
                "WHERE V.code NOT IN (SELECT AV.code " +
                "FROM Vaccination AV " +
                "ORDER BY V.code)";

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
        String query = "SELECT U.userID, U.username, U.address " +
                "FROM User U, Vaccination AV " +
                "WHERE U.userID = AV.userID AND AV.vacdate >= ? " +
                "GROUP BY U.userID " +
                "HAVING COUNT(AV.code) >= 2 " +
                "ORDER BY U.userID asc";

        try {
            PreparedStatement st = this.conn.prepareStatement(query);
            st.setString(1, vacdate);
            ResultSet rs = st.executeQuery();
            ArrayList<QueryResult.UserIDuserNameAddressResult> answer = new ArrayList<>();
            while (rs.next()){
                answer.add(new QueryResult.UserIDuserNameAddressResult(
                        rs.getString("userID"),
                        rs.getString("username"),
                        rs.getString("address")));
            }
            return answer.toArray(new QueryResult.UserIDuserNameAddressResult[0]);

        } catch (SQLException e) {
            e.printStackTrace();
        }

        return new QueryResult.UserIDuserNameAddressResult[0];
    }

    @Override
    public Vaccine[] getTwoRecentVaccinesDoNotContainVac() {
        String query = "SELECT DISTINCT V.code, V.vaccinename, V.type " +
                "FROM Vaccine V, Vaccination AV " +
                "WHERE V.vaccinename NOT LIKE '%vac%' AND V.code = AV.code " +
                "ORDER BY V.code";

        try {
            PreparedStatement st = this.conn.prepareStatement(query);
            ResultSet rs = st.executeQuery();
            ArrayList<Vaccine> answer = new ArrayList<>();
            while(rs.next()){
                answer.add(new Vaccine(
                        rs.getInt("code"),
                        rs.getString("vaccinename"),
                        rs.getString("type")
                ));
            }
            return Arrays.copyOfRange(answer.toArray(new Vaccine[0]), 0, 2);
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return new Vaccine[0];
    }

    @Override
    public QueryResult.UserIDuserNameAddressResult[] getUsersAtHasLeastTwoDoseAtMostOneSideEffect() {
        String query = "SELECT DISTINCT U.userID, U.username, U.address" +
                " FROM User U, Vaccination AV " +
                "WHERE U.userID = AV.userID AND U.userID IN(SELECT U2.userID " +
                                                        "FROM User U2, Seen S " +
                                                        "WHERE S.userID = U2.userID " +
                                                        "GROUP BY U2.userID " +
                                                        "HAVING COUNT(U.userID) <= 1) " +
                "GROUP BY U.userID " +
                "HAVING COUNT(U.userID) >= 2 " +
                "ORDER BY U.userID";
        try {
            PreparedStatement st = this.conn.prepareStatement(query);
            ResultSet rs = st.executeQuery();
            ArrayList<QueryResult.UserIDuserNameAddressResult> answer = new ArrayList<>();
            while (rs.next()){
                answer.add(new QueryResult.UserIDuserNameAddressResult(
                        rs.getString("userID"),
                        rs.getString("username"),
                        rs.getString("address")));
            }
            return answer.toArray(new QueryResult.UserIDuserNameAddressResult[0]);
        } catch (SQLException e) {
            e.printStackTrace();
        }

        return new QueryResult.UserIDuserNameAddressResult[0];
    }

    @Override
    public QueryResult.UserIDuserNameAddressResult[] getVaccinatedUsersWithAllVaccinesCanCauseGivenSideEffect(String effectname) {
        String query = "SELECT U.userID, U.username, U.address " +
                "FROM User U " +
                "WHERE NOT EXISTS (SELECT AV.code " +
                                "FROM Vaccination AV " +
                                "WHERE AV.userID = U.userID AND " +
                                "NOT EXISTS (SELECT S.code " +
                                            "FROM Seen S, Vaccine V, AllergicSideEffect E " +
                                            "WHERE S.code = AV.code AND S.userID = U.userID AND " +
                                            "E.effectname = ?)) " +
                "GROUP BY U.userID";
        try {
            PreparedStatement st = this.conn.prepareStatement(query);
            st.setString(1, effectname);
            ResultSet rs = st.executeQuery();
            ArrayList<QueryResult.UserIDuserNameAddressResult> answer = new ArrayList<>();
            while (rs.next()){
                answer.add(new QueryResult.UserIDuserNameAddressResult(
                        rs.getString("userID"),
                        rs.getString("username"),
                        rs.getString("address")));
            }
            return answer.toArray(new QueryResult.UserIDuserNameAddressResult[0]);
        } catch (SQLException e) {
            e.printStackTrace();
        }


        return new QueryResult.UserIDuserNameAddressResult[0];

    }

    @Override
    public QueryResult.UserIDuserNameAddressResult[] getUsersWithAtLeastTwoDifferentVaccineTypeByGivenInterval(String startdate, String enddate) {
        String query = "SELECT U.userID, U.username, U.address " +
                "FROM User U, Vaccination AV, Vaccine V " +
                "WHERE V.code = AV.code AND U.userID = AV.userID AND " +
                "AV.vacdate >= ? AND AV.vacdate <= ? " +
                "GROUP BY U.userID " +
                "HAVING COUNT(DISTINCT V.code) >=2 " +
                "ORDER BY U.userID";

        try {
            PreparedStatement st = this.conn.prepareStatement(query);
            st.setString(1, startdate);
            st.setString(2, enddate);
            ResultSet rs = st.executeQuery();
            ArrayList<QueryResult.UserIDuserNameAddressResult> answer = new ArrayList<>();
            while (rs.next()){
                answer.add(new QueryResult.UserIDuserNameAddressResult(
                        rs.getString("userID"),
                        rs.getString("username"),
                        rs.getString("address")));
            }
            return answer.toArray(new QueryResult.UserIDuserNameAddressResult[0]);
        } catch (SQLException e) {
            e.printStackTrace();
        }

        return new QueryResult.UserIDuserNameAddressResult[0];
    }

    @Override
    public AllergicSideEffect[] getSideEffectsOfUserWhoHaveTwoDosesInLessThanTwentyDays() {
        String query = "SELECT A.effectcode, A.effectname " +
                "FROM Seen S, AllergicSideEffect A, User U " +
                "WHERE S.effectcode = A.effectcode AND " +
                "U.userID = S.userID AND U.userID IN " +
                "(SELECT DISTINCT U2.userID " +
                "FROM Seen S2, User U2, Vaccination AV, Vaccination AV2 " +
                "WHERE S2.userID = U2.userID AND U2.userID = AV.userID AND " +
                "U2.userID = AV2.userID AND AV.vacdate <> AV2.vacdate AND " +
                "((DATEDIFF(AV.vacdate, AV2.vacdate) < 20 AND DATEDIFF(AV.vacdate, AV2.vacdate) >=0) OR " +
                "(DATEDIFF(AV2.vacdate, AV.vacdate) < 20 AND DATEDIFF(AV2.vacdate, AV.vacdate >= 0)))) " +
                "ORDER BY A.effectcode";

        try {
            PreparedStatement st = this.conn.prepareStatement(query);
            ResultSet rs = st.executeQuery();
            ArrayList<AllergicSideEffect> answer = new ArrayList<>();
            while (rs.next()){
                answer.add(new AllergicSideEffect(
                        rs.getInt("effectcode"),
                        rs.getString("effectname")));
            }
            return answer.toArray(new AllergicSideEffect[0]);
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return new AllergicSideEffect[0];
    }

    @Override
    public double averageNumberofDosesofVaccinatedUserOverSixtyFiveYearsOld() {
        String query = "SELECT AVG(Temp.Result) AS Value " +
                "FROM (SELECT AV.userID, MAX(AV.dose) as Result " +
                "FROM Vaccination AV, User U " +
                "Where AV.userID = U.userID AND U.age > 65 " +
                "GROUP BY U.userID) as Temp";
        try {
            PreparedStatement st = this.conn.prepareStatement(query);
            ResultSet rs = st.executeQuery();
            if(rs.next()){
                return rs.getDouble("Value");
            }

        } catch (SQLException e) {
            e.printStackTrace();
        }
        return 0;
    }

    @Override
    public int updateStatusToEligible(String givendate) {
        int updated = 0;
        String request = "UPDATE User U, " +
                "(SELECT MAX(AV.vacdate) as vacdate, U2.userID FROM User U2, Vaccination AV " +
                "WHERE U2.userID = AV.userID " +
                "GROUP BY U2.userID) as Target " +
                "SET U.status = 'Eligible' " +
                "WHERE U.userID = Target.userID AND U.status <> 'Eligible' AND " +
                "DATEDIFF(?, Target.vacdate) > 120";
        try {
            PreparedStatement st = this.conn.prepareStatement(request);
            st.setString(1, givendate);
            updated = st.executeUpdate();

        } catch (SQLException e) {
            e.printStackTrace();
        }

        return updated;
    }

    @Override
    public Vaccine deleteVaccine(String vaccineName) {
        String target = "SELECT * FROM Vaccine WHERE vaccineName = ?";
        String query = "DELETE FROM Vaccine WHERE vaccineName = ?";
        ArrayList<Vaccine> answer = new ArrayList<>();

        try {
            PreparedStatement st = this.conn.prepareStatement(target);
            st.setString(1, vaccineName);
            ResultSet rs = st.executeQuery();

            while(rs.next()){
                answer.add(new Vaccine(rs.getInt("code"),
                        rs.getString("vaccinename"),
                        rs.getString("type")));
                return answer.get(0);
            }
            try {
                st = this.conn.prepareStatement(query);
                st.setString(1, vaccineName);
                st.executeUpdate();
                Vaccine temp;
            } catch (SQLException e) {
                e.printStackTrace();
            }

        } catch (SQLException e) {
            e.printStackTrace();
        }
        return answer.get(0);
    }
}
