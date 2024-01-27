package com.ag.peopledb.repository;

import com.ag.peopledb.model.Person;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;

public class PeopleRepositoryTest {

    private Connection connection;
    private PeopleRepository repo;

    @BeforeEach
    void setUp() throws SQLException {
        connection = DriverManager.getConnection("jdbc:h2:C:/Users/justy/Desktop/JAVA/DB/peopledb");
        connection.setAutoCommit(false);
        repo = new PeopleRepository(connection);
    }

    @AfterEach
    void tearDown() throws SQLException {
        if(connection != null){
            connection.close();
        }
    }

    @Test
    public void canSaveOnePerson() {
        Person john = new Person("John", "Connor", ZonedDateTime.of(1980, 11, 01, 21, 05, 10, 0, ZoneId.of("-7")));
        Person savedPerson = repo.save(john);
        assertThat(savedPerson.getId()).isGreaterThan(0);
    }

    @Test
    public void canSaveTwoPeople(){
        Person john = new Person("John", "Connor", ZonedDateTime.of(1980, 11, 01, 21, 05, 10, 0, ZoneId.of("-7")));
        Person bob = new Person("Bob", "Smith", ZonedDateTime.of(1985, 05, 04, 01, 05, 10, 0, ZoneId.of("+4")));
        Person savedPerson1 = repo.save(john);
        Person savedPerson2 = repo.save(bob);
        assertThat(savedPerson1.getId()).isNotEqualTo(savedPerson2.getId());
    }

    @Test
    public void canFindPersonById(){
        Person savedPerson = repo.save(new Person("test", "ofSaving", ZonedDateTime.now()));
        Person foundPerson = repo.findPersonById(savedPerson.getId()).get();
        assertThat(foundPerson).isEqualTo(savedPerson);
    }

    @Test
    public void testPersonIdNotFound(){
        Optional<Person> personById = repo.findPersonById(-1L);
        assertThat(personById).isEmpty();
    }

    @Test
    public void canGetCount(){
        long startCount = repo.count();
        repo.save(new Person("test", "ofSaving", ZonedDateTime.now()));
        repo.save(new Person("test", "ofSaving", ZonedDateTime.now()));
        long endCount = repo.count();

        assertThat(endCount).isEqualTo(startCount + 2);
    }

    @Test
    public void canDelete(){
        Person savedPerson = repo.save(new Person("test", "ofSaving", ZonedDateTime.now()));
        long startCount = repo.count();

        repo.delete(savedPerson);
        long endCount = repo.count();

        assertThat(startCount).isEqualTo(endCount + 1);
    }

    @Test
    public void canDeleteMultiplePeople(){
        Person person = repo.save(new Person("John", "Doe", ZonedDateTime.now()));
        Person person1 = repo.save(new Person("Jan", "Nowak", ZonedDateTime.now()));

        repo.delete(person, person1);
    }

    @Test
    public void canUpdate(){
        String salary = "74587.21";

        Person savedPerson = repo.save(new Person("John", "Doe", ZonedDateTime.now()));
        Person foundPerson = repo.findPersonById(savedPerson.getId()).get();

        savedPerson.setSalary(new BigDecimal(salary));
        repo.update(savedPerson);

        Person updatedPerson = repo.findPersonById(savedPerson.getId()).get();

//        assertThat(foundPerson.getSalary()).isEqualTo("0");
//        assertThat(updatedPerson.getSalary()).isEqualTo(salary);
        assertThat(updatedPerson.getSalary()).isNotEqualTo(foundPerson.getSalary());
    }

}
