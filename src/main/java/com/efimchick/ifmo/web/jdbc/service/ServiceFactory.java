package com.efimchick.ifmo.web.jdbc.service;

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

public class ServiceFactory {
    private static final ConnectionSource CONNECTION_SOURCE = ConnectionSource.instance();

    public EmployeeService employeeService() {
        return new EmployeeService() {
            @Override
            public List<Employee> getAllSortByHireDate(Paging paging) {
                try (Connection connection = CONNECTION_SOURCE.createConnection();
                     Statement statement = connection.createStatement()) {
                    ResultSet resultSet = statement.executeQuery("SELECT * FROM EMPLOYEE ORDER BY HIREDATE");
                    List<Employee> employees = toEmployees(resultSet);
                    return obtainPage(paging, employees);
                } catch (SQLException e) {
                    return null;
                }
            }

            @Override
            public List<Employee> getAllSortByLastname(Paging paging) {
                try (Connection connection = CONNECTION_SOURCE.createConnection();
                     Statement statement = connection.createStatement()) {
                    ResultSet resultSet = statement.executeQuery("SELECT * FROM EMPLOYEE ORDER BY LASTNAME");
                    List<Employee> employees = toEmployees(resultSet);
                    return obtainPage(paging, employees);
                } catch (SQLException e) {
                    return null;
                }
            }

            @Override
            public List<Employee> getAllSortBySalary(Paging paging) {
                try (Connection connection = CONNECTION_SOURCE.createConnection();
                     Statement statement = connection.createStatement()) {
                    ResultSet resultSet = statement.executeQuery("SELECT * FROM EMPLOYEE ORDER BY SALARY");
                    List<Employee> employees = toEmployees(resultSet);
                    return obtainPage(paging, employees);
                } catch (SQLException e) {
                    return null;
                }
            }

            @Override
            public List<Employee> getAllSortByDepartmentNameAndLastname(Paging paging) {
                try (Connection connection = CONNECTION_SOURCE.createConnection();
                     Statement statement = connection.createStatement()) {
                    ResultSet resultSet = statement.executeQuery(
                            "SELECT E.* FROM EMPLOYEE E " +
                                    "LEFT JOIN DEPARTMENT D on E.DEPARTMENT = D.ID " +
                                    "ORDER BY D.NAME, E.LASTNAME");
                    List<Employee> employees = toEmployees(resultSet);
                    return obtainPage(paging, employees);
                } catch (SQLException e) {
                    return null;
                }
            }

            @Override
            public List<Employee> getByDepartmentSortByHireDate(Department department, Paging paging) {
                try (Connection connection = CONNECTION_SOURCE.createConnection();
                     Statement statement = connection.createStatement()) {
                    ResultSet resultSet = statement.executeQuery("SELECT * FROM EMPLOYEE " +
                            "WHERE DEPARTMENT = " + department.getId() +
                            " ORDER BY HIREDATE");
                    List<Employee> employees = toEmployees(resultSet);
                    return obtainPage(paging, employees);
                } catch (SQLException e) {
                    return null;
                }
            }

            @Override
            public List<Employee> getByDepartmentSortBySalary(Department department, Paging paging) {
                try (Connection connection = CONNECTION_SOURCE.createConnection();
                     Statement statement = connection.createStatement()) {
                    ResultSet resultSet = statement.executeQuery("SELECT * FROM EMPLOYEE " +
                            "WHERE DEPARTMENT = " + department.getId() +
                            " ORDER BY SALARY");
                    List<Employee> employees = toEmployees(resultSet);
                    return obtainPage(paging, employees);
                } catch (Exception e) {
                    e.printStackTrace();
                    return null;
                }
            }

            @Override
            public List<Employee> getByDepartmentSortByLastname(Department department, Paging paging) {
                try (Connection connection = CONNECTION_SOURCE.createConnection();
                     Statement statement = connection.createStatement()) {
                    ResultSet resultSet = statement.executeQuery("SELECT * FROM EMPLOYEE " +
                            "WHERE DEPARTMENT = " + department.getId() +
                            " ORDER BY LASTNAME");
                    List<Employee> employees = toEmployees(resultSet);
                    return obtainPage(paging, employees);
                } catch (SQLException e) {
                    e.printStackTrace();
                    return null;
                }
            }

            @Override
            public List<Employee> getByManagerSortByLastname(Employee manager, Paging paging) {
                try (Connection connection = CONNECTION_SOURCE.createConnection();
                     Statement statement = connection.createStatement()) {
                    ResultSet resultSet = statement.executeQuery("SELECT * FROM EMPLOYEE " +
                            "WHERE MANAGER = " + manager.getId() +
                            " ORDER BY LASTNAME");
                    List<Employee> employees = toEmployees(resultSet);
                    return obtainPage(paging, employees);
                } catch (SQLException e) {
                    return null;
                }
            }

            @Override
            public List<Employee> getByManagerSortByHireDate(Employee manager, Paging paging) {
                try (Connection connection = CONNECTION_SOURCE.createConnection();
                     Statement statement = connection.createStatement()) {
                    ResultSet resultSet = statement.executeQuery("SELECT * FROM EMPLOYEE " +
                            "WHERE MANAGER = " + manager.getId() +
                            " ORDER BY HIREDATE");
                    List<Employee> employees = toEmployees(resultSet);
                    return obtainPage(paging, employees);
                } catch (SQLException e) {
                    return null;
                }
            }

            @Override
            public List<Employee> getByManagerSortBySalary(Employee manager, Paging paging) {
                try (Connection connection = CONNECTION_SOURCE.createConnection();
                     Statement statement = connection.createStatement()) {
                    ResultSet resultSet = statement.executeQuery("SELECT * FROM EMPLOYEE " +
                            "WHERE MANAGER = " + manager.getId() +
                            " ORDER BY SALARY");
                    List<Employee> employees = toEmployees(resultSet);
                    return obtainPage(paging, employees);
                } catch (SQLException e) {
                    return null;
                }
            }

            @Override
            public Employee getWithDepartmentAndFullManagerChain(Employee employee) {
                try (Connection connection = CONNECTION_SOURCE.createConnection();
                     Statement statement = connection.createStatement()) {
                    ResultSet resultSet = statement.executeQuery(
                            "SELECT * FROM EMPLOYEE WHERE ID = " + employee.getId());
                    return resultSet.next() ? toEmployee(resultSet, true) : null;
                } catch (SQLException e) {
                    e.printStackTrace();
                    return null;
                }
            }

            @Override
            public Employee getTopNthBySalaryByDepartment(int salaryRank, Department department) {
                try (Connection connection = CONNECTION_SOURCE.createConnection();
                     Statement statement = connection.createStatement()) {
                    ResultSet resultSet = statement.executeQuery("SELECT * FROM EMPLOYEE " +
                            "WHERE DEPARTMENT = " + department.getId() +
                            " ORDER BY SALARY DESC LIMIT 1 OFFSET " + (salaryRank - 1));
                    return resultSet.next() ? toEmployee(resultSet) : null;
                } catch (SQLException e) {
                    e.printStackTrace();
                    return null;
                }
            }
        };
    }

    private Employee toEmployee(ResultSet rs) throws SQLException {
        return toEmployee(rs, false);
    }

    private Employee toEmployee(ResultSet rs, boolean chain) throws SQLException {
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
                (rs.getObject("MANAGER") != null) ?
                        retrieveManager(rs.getBigDecimal("MANAGER").toBigInteger(), chain) :
                        null,
                (rs.getBigDecimal("DEPARTMENT") != null) ?
                        retrieveDepartment(rs.getBigDecimal("DEPARTMENT").toBigInteger()) :
                        null
        );
    }

    private List<Employee> toEmployees(ResultSet rs) throws SQLException {
        List<Employee> employees = new ArrayList<>();
        while (rs.next()) {
            employees.add(toEmployee(rs));
        }
        return employees;
    }

    private Employee retrieveManager(BigInteger id, boolean chain) throws SQLException {
        try (Connection connection = CONNECTION_SOURCE.createConnection();
             Statement statement = connection.createStatement(ResultSet.TYPE_SCROLL_SENSITIVE,
                     ResultSet.CONCUR_UPDATABLE)) {
            ResultSet resultSet = statement.executeQuery("SELECT * FROM EMPLOYEE WHERE ID = " + id.toString());
            if (resultSet.next()) {
                if (chain) {
                    resultSet = statement.executeQuery("SELECT  * FROM EMPLOYEE");
                    while (resultSet.next()) {
                        if (resultSet.getBigDecimal("ID").toBigInteger().equals(id)) {
                            return getManagerChain(resultSet, resultSet.getRow());
                        }
                    }
                    return null;
                } else {
                    return new Employee(
                            resultSet.getBigDecimal("ID").toBigInteger(),
                            new FullName(
                                    resultSet.getString("FIRSTNAME"),
                                    resultSet.getString("LASTNAME"),
                                    resultSet.getString("MIDDLENAME")
                            ),
                            Position.valueOf(resultSet.getString("POSITION")),
                            LocalDate.parse(resultSet.getString("HIREDATE")),
                            resultSet.getBigDecimal("SALARY"),
                            null,
                            (resultSet.getBigDecimal("DEPARTMENT") != null) ?
                                    retrieveDepartment(resultSet.getBigDecimal("DEPARTMENT").toBigInteger()) :
                                    null
                    );
                }
            } else {
                return null;
            }
        }
    }

    private Employee getManagerChain(ResultSet rs, int pos) throws SQLException {
        Employee manager = null;
        int managerId = rs.getInt("MANAGER");
        if (managerId != 0) {
            rs.beforeFirst();
            while (rs.next()) {
                if (rs.getInt("ID") == managerId) { //find manager
                    manager = getManagerChain(rs, rs.getRow()); //convert it to employee obj
                    break;
                }
            }
            rs.absolute(pos);
        }
        return new Employee(
                BigInteger.valueOf(rs.getLong("ID")),
                new FullName(
                        rs.getString("FIRSTNAME"),
                        rs.getString("LASTNAME"),
                        rs.getString("MIDDLENAME")
                ),
                Position.valueOf(rs.getString("POSITION")),
                LocalDate.parse(rs.getString("HIREDATE")),
                rs.getBigDecimal("SALARY"),
                manager,
                (rs.getBigDecimal("DEPARTMENT") != null) ?
                        retrieveDepartment(rs.getBigDecimal("DEPARTMENT").toBigInteger()) : //if employee has department
                        null //otherwise// if employee has manager, else null
        );
    }

    private Department toDepartment(ResultSet rs) throws SQLException {
        return new Department(
                BigInteger.valueOf(rs.getLong("ID")),
                rs.getString("NAME"),
                rs.getString("LOCATION")
        );
    }

    private Department retrieveDepartment(BigInteger id) throws SQLException {
        try (Connection connection = CONNECTION_SOURCE.createConnection();
             Statement statement = connection.createStatement()) {
            ResultSet resultSet = statement.executeQuery("SELECT * FROM DEPARTMENT WHERE ID = " + id.toString());
            if (resultSet.next()) {
                return toDepartment(resultSet);
            } else {
                return null;
            }
        }
    }

    private List<Employee> obtainPage(Paging paging, List<Employee> book) {
        int fromIndex = (paging.page - 1) * paging.itemPerPage;
        int toIndex = Math.min(paging.itemPerPage * paging.page, book.size());
        return book.subList(fromIndex, toIndex);
    }
}
