package com.efimchick.ifmo.web.jdbc.dao;

import com.efimchick.ifmo.web.jdbc.ConnectionSource;
import com.efimchick.ifmo.web.jdbc.domain.Department;
import com.efimchick.ifmo.web.jdbc.domain.Employee;
import com.efimchick.ifmo.web.jdbc.domain.FullName;
import com.efimchick.ifmo.web.jdbc.domain.Position;

import java.math.BigInteger;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.LocalDate;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

public class DaoFactory {
    private static final ConnectionSource connectionSource = ConnectionSource.instance();

    public EmployeeDao employeeDAO() {
        return new EmployeeDao() {
            @Override
            public List<Employee> getByDepartment(Department department) {
                try (Connection connection = connectionSource.createConnection();
                     Statement statement = connection.createStatement()) {
                    final ResultSet resultSet = statement.executeQuery(
                            "SELECT * FROM EMPLOYEE WHERE DEPARTMENT=" + department.getId());
                    return toEmployees(resultSet);
                } catch (SQLException e) {
                    e.printStackTrace();
                    return null;
                }
            }

            @Override
            public List<Employee> getByManager(Employee employee) {
                try (Connection connection = connectionSource.createConnection();
                     Statement statement = connection.createStatement()) {
                    final ResultSet resultSet = statement.executeQuery(
                            "SELECT * FROM EMPLOYEE WHERE MANAGER=" + employee.getId());
                    return toEmployees(resultSet);
                } catch (SQLException e) {
                    e.printStackTrace();
                    return null;
                }
            }

            @Override
            public Optional<Employee> getById(BigInteger Id) {
                try (Connection connection = connectionSource.createConnection();
                     Statement statement = connection.createStatement()) {
                    final ResultSet resultSet = statement.executeQuery("SELECT * FROM EMPLOYEE WHERE ID=" + Id);
                    if (resultSet.next()) {
                        return Optional.of(toEmployee(resultSet));
                    } else return Optional.empty();
                } catch (SQLException e) {
                    e.printStackTrace();
                    return Optional.empty();
                }
            }

            @Override
            public List<Employee> getAll() {
                try (Connection connection = connectionSource.createConnection();
                     Statement statement = connection.createStatement()) {
                    final ResultSet resultSet = statement.executeQuery("SELECT * FROM EMPLOYEE");
                    return toEmployees(resultSet);
                } catch (SQLException e) {
                    e.printStackTrace();
                    return null;
                }
            }

            @Override
            public Employee save(Employee employee) {
                try (Connection connection = connectionSource.createConnection();
                     Statement statement = connection.createStatement()) {
                    String sqlQuery = String.format(
                            "INSERT INTO EMPLOYEE VALUES (%d, '%s', '%s', '%s', '%s', '%d', '%s', '%d', '%d')",
                            employee.getId(),
                            employee.getFullName().getFirstName(),
                            employee.getFullName().getLastName(),
                            employee.getFullName().getMiddleName(),
                            employee.getPosition().toString(),
                            employee.getManagerId(),
                            employee.getHired().toString(),
                            employee.getSalary().toBigInteger(),
                            employee.getDepartmentId());
                    statement.executeUpdate(sqlQuery);
                    return employee;
                } catch (SQLException e) {
                    e.printStackTrace();
                    return null;
                }
            }

            @Override
            public void delete(Employee employee) {
                try (Connection connection = connectionSource.createConnection();
                     Statement statement = connection.createStatement()) {
                    String sqlQuery = String.format("DELETE FROM EMPLOYEE WHERE ID=%s", employee.getId());
                    statement.executeUpdate(sqlQuery);
                } catch (SQLException e) {
                    e.printStackTrace();
                }
            }
        };
    }

    public DepartmentDao departmentDAO() {
        return new DepartmentDao() {
            @Override
            public Optional<Department> getById(BigInteger Id) {
                try (Connection connection = connectionSource.createConnection();
                     Statement statement = connection.createStatement()) {
                    final ResultSet resultSet = statement.executeQuery("SELECT * FROM DEPARTMENT WHERE ID=" + Id);
                    if (resultSet.next()) {
                        return Optional.of(toDepartment(resultSet));
                    } else return Optional.empty();
                } catch (SQLException e) {
                    return Optional.empty();
                }
            }

            @Override
            public List<Department> getAll() {
                try (Connection connection = connectionSource.createConnection();
                     Statement statement = connection.createStatement()) {
                    ResultSet resultSet = statement.executeQuery("SELECT * FROM DEPARTMENT");
                    return toDepartments(resultSet);
                } catch (SQLException e) {
                    return null;
                }
            }

            @Override
            public Department save(Department department) {
                try (Connection connection = connectionSource.createConnection();
                     Statement statement = connection.createStatement()) {
                    ResultSet resultSet = statement.executeQuery(
                            "SELECT * FROM DEPARTMENT WHERE ID=" + department.getId());
                    String sqlQuery;
                    if (resultSet.next()) { //update
                        sqlQuery = String.format("UPDATE DEPARTMENT SET NAME='%s', LOCATION='%s' WHERE ID=%d",
                                department.getName(),
                                department.getLocation(),
                                department.getId());
                    } else { //create
                        sqlQuery = String.format("INSERT INTO DEPARTMENT VALUES (%d, '%s', '%s')",
                                department.getId(),
                                department.getName(),
                                department.getLocation());
                    }
                    statement.executeUpdate(sqlQuery);
                    return department;
                } catch (SQLException e) {
                    e.printStackTrace();
                    return null;
                }
            }

            @Override
            public void delete(Department department) {
                try (Connection connection = connectionSource.createConnection();
                     Statement statement = connection.createStatement()) {
                    String sqlQuery = String.format("DELETE FROM DEPARTMENT WHERE ID = %s", department.getId());
                    statement.executeUpdate(sqlQuery);
                } catch (SQLException e) {
                    e.printStackTrace();
                }
            }
        };
    }

    private Employee toEmployee(ResultSet rs) throws SQLException {
        return new Employee(
                rs.getBigDecimal("ID").toBigInteger(),
                new FullName(
                        rs.getString("FIRSTNAME"),
                        rs.getString("LASTNAME"),
                        rs.getString("MIDDLENAME")
                ),
                Position.valueOf(rs.getString("POSITION")),
                LocalDate.parse(rs.getString("HIREDATE")),
                rs.getBigDecimal("SALARY"),
                (rs.getBigDecimal("MANAGER") != null) ?
                        rs.getBigDecimal("MANAGER").toBigInteger() : //if employee has manager
                        BigInteger.ZERO, //otherwise
                (rs.getBigDecimal("DEPARTMENT") != null) ?
                        rs.getBigDecimal("DEPARTMENT").toBigInteger() : //if employee has department
                        BigInteger.ZERO //otherwise
        );
    }

    private List<Employee> toEmployees(ResultSet rs) throws SQLException {
        List<Employee> employees = new ArrayList<>();
        while (rs.next()) {
            employees.add(toEmployee(rs));
        }
        return employees;
    }

    private Department toDepartment(ResultSet rs) throws SQLException {
        return new Department(
                BigInteger.valueOf(rs.getLong("ID")),
                rs.getString("NAME"),
                rs.getString("LOCATION")
        );
    }

    private List<Department> toDepartments(ResultSet rs) throws SQLException {
        List<Department> departments = new ArrayList<>();
        while (rs.next()) {
            departments.add(toDepartment(rs));
        }
        return departments;
    }
}
