package com.ag.peopledb.repository;

import com.ag.peopledb.exeption.UnableToSaveException;
import com.ag.peopledb.model.Entity;

import java.sql.*;

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

    abstract void mapForSave(T entity, PreparedStatement preparedStatement) throws SQLException;

    abstract String getSaveSQL();
}
