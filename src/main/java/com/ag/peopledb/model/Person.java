package com.ag.peopledb.model;

import java.math.BigDecimal;
import java.time.ZonedDateTime;
import java.util.Objects;

public class Person implements Entity {
    private Long id;
    private String firstName;
    private String lastName;
    private  ZonedDateTime dob;
    private BigDecimal salary = new BigDecimal("0");

    public Person(String firstName, String lastName, ZonedDateTime dob) {
        this.firstName = firstName;
        this.lastName = lastName;
        this.dob = dob;
    }

    public Person(String personFirstName, String personLastName, ZonedDateTime personDateOfBirth, BigDecimal personSalary) {
        this(personFirstName, personLastName, personDateOfBirth);
        this.salary = personSalary;
    }

    public String getFirstName() {
        return firstName;
    }

    public String getLastName() {
        return lastName;
    }

    public ZonedDateTime getDob() {
        return dob;
    }

    @Override
    public Long getId() {
        return id;
    }

    @Override
    public void setId(Long id) {
        this.id = id;
    }

    public BigDecimal getSalary() {
        return salary;
    }

    public void setSalary(BigDecimal salary) {
        this.salary = salary;
    }

    @Override
    public String toString() {
        return "Person{" +
                "id=" + id +
                ", firstName='" + firstName + '\'' +
                ", lastName='" + lastName + '\'' +
                ", dob=" + dob +
                '}';
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        Person person = (Person) o;
        return Objects.equals(id, person.id) && firstName.equals(person.firstName) && lastName.equals(person.lastName);

//        TODO: go back to this condition. It is a part of lesson no. 212 Finding a person by ID
//                && dob.withZoneSameInstant(ZoneId.of("+0")).equals(person.dob.withZoneSameInstant(ZoneId.of("+0")));
    }

    @Override
    public int hashCode() {
        return Objects.hash(id, firstName, lastName, dob);
    }
}
