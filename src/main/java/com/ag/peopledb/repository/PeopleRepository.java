package com.ag.peopledb.repository;

import com.ag.peopledb.anotation.SQL;
import com.ag.peopledb.model.Address;
import com.ag.peopledb.model.CrudOperation;
import com.ag.peopledb.model.Person;
import com.ag.peopledb.model.Region;

import java.math.BigDecimal;
import java.sql.*;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.util.Optional;

import static java.util.stream.Collectors.joining;

public class PeopleRepository extends CRUDRepository<Person> {

    private AddressRepository addressRepository = null;

    public static final String SAVE_PERSON_SQL = """
            INSERT INTO PEOPLE (FIRST_NAME, LAST_NAME, DOB, SALARY, EMAIL, HOME_ADDRESS) 
            VALUES(?, ?, ?, ?, ?, ?) 
            """;
    public static final String FIND_BY_ID_SQL = """
            SELECT 
            P.ID, P.FIRST_NAME, P.LAST_NAME, P.DOB, P.SALARY, P.HOME_ADDRESS,
            A.ID AS A_ID, A.STREET_ADDRESS, A.ADDRESS2, A.CITY, A.STATE, A.POSTCODE, A.COUNTY, A.REGION, A.COUNTRY 
            FROM PEOPLE AS P
            LEFT OUTER JOIN ADDRESSES AS A ON P.HOME_ADDRESS = A.ID
            WHERE P.ID=?
            """;
    public static final String FIND_ALL_SQL = "SELECT ID, FIRST_NAME, LAST_NAME, DOB, SALARY FROM PEOPLE";
    public static final String SELECT_COUNT_SQL = "SELECT COUNT(*) FROM PEOPLE";
    public static final String DELETE_BY_ID_SQL = "DELETE FROM PEOPLE WHERE ID=?";
    public static final String DELETE_BY_ID_IN_SQL = "DELETE FROM PEOPLE WHERE ID IN (:ids)";
    public static final String UPDATE_SQL = "UPDATE PEOPLE SET FIRST_NAME=?, LAST_NAME=?, DOB=?, SALARY=? WHERE ID=?";

    public PeopleRepository(Connection connection) {

        super(connection);
        addressRepository = new AddressRepository(connection);
    }

    @Override
    @SQL(value = SAVE_PERSON_SQL, operationType = CrudOperation.SAVE)
    void mapForSave(Person entity, PreparedStatement preparedStatement) throws SQLException {
        Address savedAddress = null;

        preparedStatement.setString(1, entity.getFirstName());
        preparedStatement.setString(2, entity.getLastName());
        preparedStatement.setTimestamp(3, convertDateOfBirthToTimestamp(entity.getDob()));
        preparedStatement.setBigDecimal(4, entity.getSalary());
        preparedStatement.setString(5, entity.getEmail());

        if (entity.getHomeAddress().isPresent()) {
            savedAddress = addressRepository.save(entity.getHomeAddress().get());
            preparedStatement.setLong(6, savedAddress.id());
        } else {
            preparedStatement.setObject(6, null);
        }

    }

    @Override
    @SQL(value = FIND_BY_ID_SQL, operationType = CrudOperation.FIND_BY_ID)
    @SQL(value = FIND_ALL_SQL, operationType = CrudOperation.FIND_ALL)
    @SQL(value = SELECT_COUNT_SQL, operationType = CrudOperation.COUNT)
    @SQL(value = DELETE_BY_ID_SQL, operationType = CrudOperation.DELETE_ONE)
    @SQL(value = DELETE_BY_ID_IN_SQL, operationType = CrudOperation.DELETE_MANY)
    Person extractEntityFromResultSet(ResultSet resultSet) throws SQLException{
        long personId = resultSet.getLong("ID");
        String personFirstName = resultSet.getString("FIRST_NAME");
        String personLastName = resultSet.getString("LAST_NAME");
        ZonedDateTime personDateOfBirth = ZonedDateTime.of(resultSet.getTimestamp("DOB").toLocalDateTime(), ZoneId.of("+0"));
        BigDecimal personSalary = resultSet.getBigDecimal("SALARY");

//        long homeAddressId = resultSet.getLong("HOME_ADDRESS");

        Address address = extractAddress(resultSet);


        Person person = new Person(personId, personFirstName, personLastName, personDateOfBirth, personSalary);
        person.setHomeAddress(address);
        return person;
    }

    private Address extractAddress(ResultSet resultSet) throws SQLException {
        if(resultSet.getObject("A_ID") == null) return null;

        long addressId = resultSet.getLong("A_ID");

        String streetAddress = resultSet.getString("STREET_ADDRESS");
        String address2 = resultSet.getString("ADDRESS2");
        String city = resultSet.getString("CITY");
        String state = resultSet.getString("STATE");
        String postcode = resultSet.getString("POSTCODE");
        String county = resultSet.getString("COUNTY");
        Region region = Region.valueOf(resultSet.getString("REGION").toUpperCase());
        String country = resultSet.getString("COUNTRY");
        Address address = new Address(addressId, streetAddress, address2, city, state, postcode, country, county, region);
        return address;
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
}

// DELETING A GROUP OF PEOPLE IN A LOOP
//    public void delete(Person...people) {
//        for (Person person : people){
//            delete(person);
//        }
//    }
