package com.ag.peopledb.repository;

import com.ag.peopledb.anotation.SQL;
import com.ag.peopledb.exeption.UnableToSaveException;
import com.ag.peopledb.model.CrudOperation;
import com.ag.peopledb.model.Person;

import java.math.BigDecimal;
import java.sql.*;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.util.Arrays;

import static java.util.stream.Collectors.joining;

public class PeopleRepository extends CRUDRepository<Person> {
    public static final String SAVE_PERSON_SQL = "INSERT INTO PEOPLE (FIRST_NAME, LAST_NAME, DOB) VALUES(?, ?, ?) ";
    public static final String FIND_BY_ID_SQL = "SELECT ID, FIRST_NAME, LAST_NAME, DOB, SALARY FROM PEOPLE WHERE ID=?";
    public static final String FIND_ALL_SQL = "SELECT ID, FIRST_NAME, LAST_NAME, DOB, SALARY FROM PEOPLE";
    public static final String SELECT_COUNT_SQL = "SELECT COUNT(*) FROM PEOPLE";
    public static final String DELETE_BY_ID_SQL = "DELETE FROM PEOPLE WHERE ID=?";
    public static final String DELETE_BY_ID_IN_SQL = "DELETE FROM PEOPLE WHERE ID IN (:ids)";
    public static final String UPDATE_SQL = "UPDATE PEOPLE SET FIRST_NAME=?, LAST_NAME=?, DOB=?, SALARY=? WHERE ID=?";

    public PeopleRepository(Connection connection) {
        super(connection);
    }

    @Override
    @SQL(value = SAVE_PERSON_SQL, operationType = CrudOperation.SAVE)
    void mapForSave(Person entity, PreparedStatement preparedStatement) throws SQLException {
        preparedStatement.setString(1, entity.getFirstName());
        preparedStatement.setString(2, entity.getLastName());
        preparedStatement.setTimestamp(3, convertDateOfBirthToTimestamp(entity.getDob()));
    }

    @Override
    @SQL(value = "SELECT ID, FIRST_NAME, LAST_NAME, DOB, SALARY FROM PEOPLE WHERE ID=?", operationType = CrudOperation.FIND_BY_ID)
    @SQL(value = FIND_ALL_SQL, operationType = CrudOperation.FIND_ALL)
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
    @SQL(value = UPDATE_SQL, operationType = CrudOperation.UPDATE)
    void mapForUpdate(Person entity, PreparedStatement preparedStatement) throws SQLException {
        preparedStatement.setString(1, entity.getFirstName());
        preparedStatement.setString(2, entity.getLastName());
        preparedStatement.setTimestamp(3, convertDateOfBirthToTimestamp(entity.getDob()));
        preparedStatement.setBigDecimal(4, entity.getSalary());
    }

    private static Timestamp convertDateOfBirthToTimestamp(ZonedDateTime dateOfBirth) {
        return Timestamp.valueOf(dateOfBirth.withZoneSameInstant(ZoneId.of("+0")).toLocalDateTime());
    }

    @Override
    protected String getFindByIdSQL() {
        return FIND_BY_ID_SQL;
    }

    @Override
    protected String getFindAllSQL() {
        return FIND_ALL_SQL;
    }

    @Override
    protected String getCountSQL() {
        return SELECT_COUNT_SQL;
    }

    @Override
    protected String getDeleteSQL() {
        return DELETE_BY_ID_SQL;
    }

    @Override
    protected String getDeleteInSQL() {
        return DELETE_BY_ID_IN_SQL;
    }

//    @Override
//    protected String getUpdateSQL() {
//        return UPDATE_SQL;
//    }


}

// DELETING A GROUP OF PEOPLE IN A LOOP
//    public void delete(Person...people) {
//        for (Person person : people){
//            delete(person);
//        }
//    }
