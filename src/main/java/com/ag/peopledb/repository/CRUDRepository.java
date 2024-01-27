package com.ag.peopledb.repository;

import com.ag.peopledb.exeption.UnableToSaveException;
import com.ag.peopledb.model.Entity;
import com.ag.peopledb.model.Person;

import java.sql.*;
import java.util.Optional;

abstract class CRUDRepository<T extends Entity> {

    protected Connection connection;

    public CRUDRepository(Connection connection) {
        this.connection = connection;
    }

    public T save(T entity) throws UnableToSaveException {
        try {
            PreparedStatement preparedStatement = connection.prepareStatement(getSaveSQL(), Statement.RETURN_GENERATED_KEYS);

            mapForSave(entity, preparedStatement);
            int update = preparedStatement.executeUpdate();
            ResultSet generatedKeys = preparedStatement.getGeneratedKeys();
            while (generatedKeys.next()){
                long id = generatedKeys.getLong(1);
                entity.setId(id);
                System.out.println(entity);
            }
            System.out.printf("Records affected: %d%n", update );
        } catch (SQLException e) {
            e.printStackTrace();
            throw new UnableToSaveException("Saving new person failed: " + entity);
        }
        return entity;
    }

    public Optional<T> findPersonById(Long id) {
        T entity = null;

        try {
            PreparedStatement preparedStatement = connection.prepareStatement(getFindByIdSQL());
            preparedStatement.setLong(1, id);
            ResultSet resultSet = preparedStatement.executeQuery();
            while (resultSet.next()) {
                entity = extractEntityFromResultSet(resultSet);
            }
        } catch (SQLException e) {
            e.printStackTrace();
            throw new UnableToSaveException("Nothing has been found. Try again later");
        }
        return Optional.ofNullable(entity);
    }

    abstract T extractEntityFromResultSet(ResultSet resultSet) throws SQLException;

    /**
     * Returns a string that represents a SQL needed to retrieve one entity
     * The SQL must contain one SQL parameter i.e. "?" that would bind to entity's ID
     */

    protected abstract String getFindByIdSQL();

    abstract void mapForSave(T entity, PreparedStatement preparedStatement) throws SQLException;

    protected abstract String getSaveSQL();
}
