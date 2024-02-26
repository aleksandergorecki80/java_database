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
            INSERT INTO PEOPLE (FIRST_NAME, LAST_NAME, DOB, SALARY, EMAIL, HOME_ADDRESS, BUSINESS_ADDRESS, PARENT_ID) 
            VALUES(?, ?, ?, ?, ?, ?, ?, ?) 
            """;
    public static final String FIND_BY_ID_SQL = """
            SELECT
            PARENT.ID AS PARENT_ID, PARENT.FIRST_NAME AS PARENT_FIRST_NAME, PARENT.LAST_NAME AS PARENT_LAST_NAME, PARENT.DOB AS PARENT_DOB, PARENT.SALARY AS PARENT_SALARY, PARENT.EMAIL AS PARENT_EMAIL, 
            CHILD.ID AS CHILD_ID, CHILD.FIRST_NAME AS CHILD_FIRST_NAME, CHILD.LAST_NAME AS CHILD_LAST_NAME, CHILD.DOB AS CHILD_DOB, CHILD.SALARY AS CHILD_SALARY, CHILD.EMAIL AS CHILD_EMAIL, 
            HOME.ID AS HOME_ID, HOME.STREET_ADDRESS AS HOME_STREET_ADDRESS, HOME.ADDRESS2 AS HOME_ADDRESS2, HOME.CITY AS HOME_CITY, HOME.STATE AS HOME_STATE, HOME.POSTCODE AS HOME_POSTCODE, HOME.COUNTY AS HOME_COUNTY, HOME.REGION AS HOME_REGION, HOME.COUNTRY AS HOME_COUNTRY,
            BUSINESS.ID AS BUSINESS_ID, BUSINESS.STREET_ADDRESS AS BUSINESS_STREET_ADDRESS, BUSINESS.ADDRESS2 AS BUSINESS_ADDRESS2, BUSINESS.CITY AS BUSINESS_CITY, BUSINESS.STATE AS BUSINESS_STATE, BUSINESS.POSTCODE AS BUSINESS_POSTCODE, BUSINESS.COUNTY AS BUSINESS_COUNTY, BUSINESS.REGION AS BUSINESS_REGION, BUSINESS.COUNTRY AS BUSINESS_COUNTRY, 
            FROM PEOPLE AS PARENT
            LEFT OUTER JOIN PEOPLE AS CHILD ON PARENT.ID = CHILD.PARENT_ID
            LEFT OUTER JOIN ADDRESSES AS HOME ON PARENT.HOME_ADDRESS = HOME.ID
            LEFT OUTER JOIN ADDRESSES AS BUSINESS ON PARENT.BUSINESS_ADDRESS = BUSINESS.ID
            WHERE PARENT.ID = ?
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

        associateAddressWithPerson(preparedStatement, entity.getHomeAddress(), 6);
        associateAddressWithPerson(preparedStatement, entity.getBusinessAddress(), 7);

        associateChildWithPerson(entity, preparedStatement);

    }

    private static void associateChildWithPerson(Person entity, PreparedStatement preparedStatement) throws SQLException {
        Optional<Person> parent = entity.getParent();
        if (parent.isPresent()){
            preparedStatement.setLong(8, parent.get().getId());
        } else {
            preparedStatement.setObject(8, null);
        }
    }

    @Override
    protected void postSave(Person entity, long id) {
        entity.getChildren().stream()
                .forEach(this::save);
    }

    private void associateAddressWithPerson(PreparedStatement preparedStatement, Optional<Address> address, int parameterIndex) throws SQLException {
        Address savedAddress;
        if (address.isPresent()) {
            savedAddress = addressRepository.save(address.get());
            preparedStatement.setLong(parameterIndex, savedAddress.id());
        } else {
            preparedStatement.setObject(parameterIndex, null);
        }
    }

    @Override
    @SQL(value = FIND_BY_ID_SQL, operationType = CrudOperation.FIND_BY_ID)
    @SQL(value = FIND_ALL_SQL, operationType = CrudOperation.FIND_ALL)
    @SQL(value = SELECT_COUNT_SQL, operationType = CrudOperation.COUNT)
    @SQL(value = DELETE_BY_ID_SQL, operationType = CrudOperation.DELETE_ONE)
    @SQL(value = DELETE_BY_ID_IN_SQL, operationType = CrudOperation.DELETE_MANY)
    Person extractEntityFromResultSet(ResultSet resultSet) throws SQLException{
        Person finalParent = null;
        do {
            Person currentParent = extractPerson(resultSet, "PARENT_");

            if(finalParent == null) {
                finalParent = currentParent;
            }

            if(finalParent.equals(currentParent)){

            }

            Person child = extractPerson(resultSet, "CHILD_");

            Address homeAddress = extractAddress(resultSet, "HOME_");
            Address businessAddress = extractAddress(resultSet, "BUSINESS_");

            finalParent.setHomeAddress(homeAddress);
            finalParent.setBusinessAddress(businessAddress);
            finalParent.addChild(child);

        } while (resultSet.next());

        return finalParent;
    }

    private static Person extractPerson(ResultSet resultSet, String aliasPrefix) throws SQLException {
        long personId = getValueByAlias(aliasPrefix + "ID", resultSet, Long.class);
        String personFirstName = getValueByAlias(aliasPrefix + "FIRST_NAME", resultSet, String.class);
        String personLastName = getValueByAlias(aliasPrefix + "LAST_NAME", resultSet, String.class);
        ZonedDateTime personDateOfBirth = ZonedDateTime.of(getValueByAlias(aliasPrefix + "DOB", resultSet, Timestamp.class).toLocalDateTime(), ZoneId.of("+0"));
        BigDecimal personSalary = getValueByAlias(aliasPrefix + "SALARY", resultSet, BigDecimal.class);
        Person person = new Person(personId, personFirstName, personLastName, personDateOfBirth, personSalary);
        return person;
    }

    private Address extractAddress(ResultSet resultSet, String aliasPrefix) throws SQLException {
        Long addressId = getValueByAlias(aliasPrefix + "ID", resultSet, Long.class);

        if(addressId == null) return null;
//        long addressId = resultSet.getLong("A_ID");
//        String streetAddress2 = getValueByAlias("STREET_ADDRESS", resultSet, String.class);

        String streetAddress = getValueByAlias(aliasPrefix + "STREET_ADDRESS", resultSet, String.class);
        String address2 = getValueByAlias(aliasPrefix + "ADDRESS2", resultSet, String.class);
        String city = getValueByAlias(aliasPrefix + "CITY", resultSet, String.class);
        String state = getValueByAlias(aliasPrefix + "STATE", resultSet, String.class);
        String postcode = getValueByAlias(aliasPrefix + "POSTCODE", resultSet, String.class);
        String county = getValueByAlias(aliasPrefix + "COUNTY", resultSet, String.class);
        Region region = Region.valueOf(getValueByAlias(aliasPrefix + "REGION", resultSet, String.class).toUpperCase());
        String country = getValueByAlias(aliasPrefix + "COUNTRY", resultSet, String.class);
        Address address = new Address(addressId, streetAddress, address2, city, state, postcode, country, county, region);
        return address;
    }

    private static <T> T getValueByAlias(String alias, ResultSet resultSet, Class<T> clazz) throws SQLException {
        int columnCount = resultSet.getMetaData().getColumnCount();
        for(int columnIndex = 1; columnIndex <= columnCount; columnIndex++) {
            if(alias.equals(resultSet.getMetaData().getColumnLabel(columnIndex))){
             return (T) resultSet.getObject(columnIndex);
            };
        }
        throw new SQLException(String.format("Column not found for alias: '%s'", alias));
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
