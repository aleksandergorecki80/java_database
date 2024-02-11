package com.ag.peopledb.repository;

import com.ag.peopledb.anotation.SQL;
import com.ag.peopledb.model.Address;
import com.ag.peopledb.model.CrudOperation;
import com.ag.peopledb.model.Region;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

public class AddressRepository extends CRUDRepository<Address> {

    public AddressRepository(Connection connection) {
        super(connection);
    }

    @Override
    @SQL(operationType = CrudOperation.FIND_BY_ID, value = """
            SELECT ID, STREET_ADDRESS, ADDRESS2, CITY, STATE, POSTCODE, COUNTY, REGION, COUNTRY
            FROM ADDRESSES
            WHERE ID = ?
            """)
    Address extractEntityFromResultSet(ResultSet resultSet) throws SQLException {
        long id = resultSet.getLong("ID");
        String streetAddress = resultSet.getString("STREET_ADDRESS");
        String address2 = resultSet.getString("ADDRESS2");
        String city = resultSet.getString("CITY");
        String state = resultSet.getString("STATE");
        String postcode = resultSet.getString("POSTCODE");
        String county = resultSet.getString("COUNTY");
        Region region = Region.valueOf(resultSet.getString("REGION").toUpperCase());
        String country = resultSet.getString("COUNTRY");

        Address address = new Address(id, streetAddress, address2, city, state, postcode, country, county, region);

        return address;
    }

    @Override
    @SQL(operationType = CrudOperation.SAVE, value = """
             INSERT INTO ADDRESSES (STREET_ADDRESS, ADDRESS2, CITY, STATE, POSTCODE, COUNTY, REGION, COUNTRY)
             VALUES(?, ?, ?, ?, ?, ?, ?, ?)
            """)
    void mapForSave(Address entity, PreparedStatement preparedStatement) throws SQLException {
            preparedStatement.setString(1, entity.streetAddress());
            preparedStatement.setString(2, entity.address2());
            preparedStatement.setString(3, entity.city());
            preparedStatement.setString(4, entity.state());
            preparedStatement.setString(5, entity.postcode());
            preparedStatement.setString(6, entity.county());
            preparedStatement.setString(7, entity.region().toString());
            preparedStatement.setString(8, entity.county());
    }

    @Override
    void mapForUpdate(Address entity, PreparedStatement preparedStatement) throws SQLException {

    }
}
