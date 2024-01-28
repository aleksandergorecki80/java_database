package com.ag.peopledb.repository;

import com.ag.peopledb.anotation.SQL;
import com.ag.peopledb.exeption.UnableToSaveException;
import com.ag.peopledb.model.Entity;

import java.sql.*;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.function.Supplier;

import static java.util.stream.Collectors.joining;

abstract class CRUDRepository<T extends Entity> {

    protected Connection connection;

    public CRUDRepository(Connection connection) {
        this.connection = connection;
    }


    private String getSQLByAnnotation(String methodName, Supplier<String> sqlGetter){
     return Arrays.stream(this.getClass().getDeclaredMethods())
              .filter(m -> methodName.contentEquals(m.getName()))
              .map(m -> m.getAnnotation(SQL.class))
              .map(SQL::value)
              .findFirst().orElseGet(sqlGetter);
    };

    public T save(T entity) throws UnableToSaveException {
        try {
            PreparedStatement preparedStatement = connection.prepareStatement(getSQLByAnnotation("mapForSave", this::getSaveSQL), Statement.RETURN_GENERATED_KEYS);

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

    public List<T> findAll(){
        List<T> entities = new ArrayList<>();
        try {
            PreparedStatement preparedStatement = connection.prepareStatement(getFindAllSQL());
            ResultSet resultSet = preparedStatement.executeQuery();
            while (resultSet.next()){
                entities.add(extractEntityFromResultSet(resultSet));
            }

        } catch (SQLException e) {
            e.printStackTrace();
            throw new UnableToSaveException("Nothing has been found. Try again later");
        }
        return entities;
    };

    public long count(){
        long count = 0;
        try {
            PreparedStatement preparedStatement = connection.prepareStatement(getCountSQL());
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

    public void delete(T entity) {
        try {
            PreparedStatement preparedStatement = connection.prepareStatement(getDeleteSQL());
            preparedStatement.setLong(1, entity.getId());
            int result = preparedStatement.executeUpdate();
            System.out.println(result + " - Deleted entity");
        } catch (SQLException e) {
            e.printStackTrace();
            throw new UnableToSaveException("Deleting failed. Try again later");
        }
    }

    public void delete(T...entities){
        try {
            String ids = Arrays.stream(entities).map(T::getId).map(String::valueOf).collect(joining(","));
            Statement statement = connection.createStatement();
            int deletedRecordsCount = statement.executeUpdate(getDeleteInSQL().replace(":ids", ids));
            System.out.println(deletedRecordsCount + " === deletedRecordsCount");
        } catch (SQLException e) {
            e.printStackTrace();
            throw new UnableToSaveException("Multiple deleting failed. Try again later");
        }
    }

    public void update(T entity) {
        try {
            PreparedStatement preparedStatement = connection.prepareStatement(getSQLByAnnotation("mapForUpdate", this::getUpdateSQL));
            mapForUpdate(entity, preparedStatement);
            preparedStatement.setLong(5, entity.getId());
            preparedStatement.executeUpdate();
        } catch (SQLException e) {
            e.printStackTrace();
            throw new UnableToSaveException("Updating failed. Try again later");
        }
    }




    abstract T extractEntityFromResultSet(ResultSet resultSet) throws SQLException;

    /**
     * Returns a string that represents a SQL needed to retrieve one entity
     * The SQL must contain one SQL parameter i.e. "?" that would bind to entity's ID
     */


    abstract void mapForSave(T entity, PreparedStatement preparedStatement) throws SQLException;

    abstract void mapForUpdate(T entity, PreparedStatement preparedStatement) throws SQLException;
//    {
//        preparedStatement.setString(1, entity.getFirstName());
//        preparedStatement.setString(2, entity.getLastName());
//        preparedStatement.setTimestamp(3, convertDateOfBirthToTimestamp(entity.getDob()));
//        preparedStatement.setBigDecimal(4, entity.getSalary());
//    }
    
    protected abstract String getDeleteInSQL();

    /**
     * should return a string like "DELETE FROM PEOPLE WHERE ID IN (:ids)"
     */

    protected abstract String getCountSQL();

    protected abstract String getFindAllSQL();

    protected abstract String getFindByIdSQL();

    String getSaveSQL(){
          return "";
      };

    String getUpdateSQL(){
        return "";
    };

    protected abstract String getDeleteSQL();


}
