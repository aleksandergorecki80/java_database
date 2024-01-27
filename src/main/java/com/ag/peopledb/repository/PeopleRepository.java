package com.ag.peopledb.repository;

import com.ag.peopledb.exeption.UnableToSaveException;
import com.ag.peopledb.model.Person;

import java.math.BigDecimal;
import java.sql.*;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.util.Arrays;
import java.util.Optional;

import static java.util.stream.Collectors.joining;

public class PeopleRepository extends CRUDRepository<Person> {
    public static final String SAVE_PERSON_SQL = "INSERT INTO PEOPLE (FIRST_NAME, LAST_NAME, DOB) VALUES(?, ?, ?) ";
    public static final String FIND_BY_ID_SQL = "SELECT ID, FIRST_NAME, LAST_NAME, DOB, SALARY FROM PEOPLE WHERE ID=?";
    public PeopleRepository(Connection connection) {
        super(connection);
    }

    @Override
    protected String getSaveSQL() {
        return SAVE_PERSON_SQL;
    }

    @Override
    void mapForSave(Person entity, PreparedStatement preparedStatement) throws SQLException {
        preparedStatement.setString(1, entity.getFirstName());
        preparedStatement.setString(2, entity.getLastName());
        preparedStatement.setTimestamp(3, convertDateOfBirthToTimestamp(entity.getDob()));
    }

    @Override
    Person extractEntityFromResultSet(ResultSet resultSet) throws SQLException{
        long personId = resultSet.getLong("ID");
        String personFirstName = resultSet.getString("FIRST_NAME");
        String personLastName = resultSet.getString("LAST_NAME");
        ZonedDateTime personDateOfBirth = ZonedDateTime.of(resultSet.getTimestamp("DOB").toLocalDateTime(), ZoneId.of("+0"));
        BigDecimal personSalary = resultSet.getBigDecimal("SALARY");
        System.out.println(personDateOfBirth);
        return new Person(personId, personFirstName, personLastName, personDateOfBirth, personSalary);
    }

    @Override
    protected String getFindByIdSQL() {
        return FIND_BY_ID_SQL;
    }

//    public Optional<Person> findPersonById(Long id) {
//        Person person = null;
//
//        try {
//            PreparedStatement preparedStatement = connection.prepareStatement(FIND_BY_ID_SQL);
//            preparedStatement.setLong(1, id);
//            ResultSet resultSet = preparedStatement.executeQuery();
//            while (resultSet.next()) {
//                person = extractEntityFromResultSet(resultSet);
//            }
//        } catch (SQLException e) {
//            e.printStackTrace();
//            throw new UnableToSaveException("Nothing has been found. Try again later");
//        }
//        return Optional.ofNullable(person);
//    }

//    private static Person extractPersonFromResultSet(ResultSet resultSet) throws SQLException {
//        Person person;
//        long personId = resultSet.getLong("ID");
//        String personFirstName = resultSet.getString("FIRST_NAME");
//        String personLastName = resultSet.getString("LAST_NAME");
//        ZonedDateTime personDateOfBirth = ZonedDateTime.of(resultSet.getTimestamp("DOB").toLocalDateTime(), ZoneId.of("+0"));
//        BigDecimal personSalary = resultSet.getBigDecimal("SALARY");
//        System.out.println(personDateOfBirth);
//
//        person = new Person(personFirstName, personLastName, personDateOfBirth, personSalary);
//        person.setId(personId);
//        return person;
//    }

    public long count(){
        long count = 0;
        try {
            PreparedStatement preparedStatement = connection.prepareStatement("SELECT COUNT(*) FROM PEOPLE");
            ResultSet resultSet = preparedStatement.executeQuery();
            if (resultSet.next()){
                count = resultSet.getLong(1);
            }
            return count;
        } catch (SQLException e) {
            e.printStackTrace();
            throw new UnableToSaveException("Nothing to count. Try again later");
        }

    }

    public void delete(Person person) {
        try {
            PreparedStatement preparedStatement = connection.prepareStatement("DELETE FROM PEOPLE WHERE ID=?");
            preparedStatement.setLong(1, person.getId());
            int result = preparedStatement.executeUpdate();
            System.out.println(result + " - Deleted person");
        } catch (SQLException e) {
            e.printStackTrace();
            throw new UnableToSaveException("Deleting failed. Try again later");
        }
    }

    // DELETING A GROUP OF PEOPLE IN A LOOP
//    public void delete(Person...people) {
//        for (Person person : people){
//            delete(person);
//        }
//    }

    public void delete(Person...people){
        try {
            String ids = Arrays.stream(people).map(Person::getId).map(String::valueOf).collect(joining(","));
            Statement statement = connection.createStatement();
            int deletedRecordsCount = statement.executeUpdate("DELETE FROM PEOPLE WHERE ID IN (:ids)".replace(":ids", ids));
            System.out.println(deletedRecordsCount + " === deletedRecordsCount");
        } catch (SQLException e) {
            e.printStackTrace();
            throw new UnableToSaveException("Multiple deleting failed. Try again later");
        }
    }

    public void update(Person person) {
        try {
            PreparedStatement preparedStatement = connection.prepareStatement("UPDATE PEOPLE SET FIRST_NAME=?, LAST_NAME=?, DOB=?, SALARY=? WHERE ID=?");
            preparedStatement.setString(1, person.getFirstName());
            preparedStatement.setString(2, person.getLastName());
            preparedStatement.setTimestamp(3, convertDateOfBirthToTimestamp(person.getDob()));
            preparedStatement.setBigDecimal(4, person.getSalary());
            preparedStatement.setLong(5, person.getId());
            preparedStatement.executeUpdate();
        } catch (SQLException e) {
            e.printStackTrace();
            throw new UnableToSaveException("Updating failed. Try again later");
        }
    }

    private static Timestamp convertDateOfBirthToTimestamp(ZonedDateTime dateOfBirth) {
        return Timestamp.valueOf(dateOfBirth.withZoneSameInstant(ZoneId.of("+0")).toLocalDateTime());
    }
}
