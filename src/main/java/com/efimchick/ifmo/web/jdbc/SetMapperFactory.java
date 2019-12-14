package com.efimchick.ifmo.web.jdbc;

import java.math.BigInteger;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.LocalDate;
import java.util.HashSet;
import java.util.Set;

import com.efimchick.ifmo.web.jdbc.domain.Employee;
import com.efimchick.ifmo.web.jdbc.domain.FullName;
import com.efimchick.ifmo.web.jdbc.domain.Position;

public class SetMapperFactory {

    public SetMapper<Set<Employee>> employeesSetMapper() {
        return resultSet -> {
            Set<Employee> emps = null;
            try {
                emps = new HashSet<>();
                while (resultSet.next()) {
                    emps.add(toEmployee(resultSet, resultSet.getRow()));
                }
            } catch (SQLException ignored) {
            }
            return emps;
        };
    }


    private Employee toEmployee(ResultSet rs, int pos) throws SQLException {
        Employee manager = null;
        int managerId = rs.getInt("MANAGER");
        if(managerId != 0) {
            rs.beforeFirst();
            while(rs.next()) {
                if(rs.getInt("ID") == managerId) { //find manager
                    manager = toEmployee(rs, rs.getRow()); //convert it to employee obj
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
                manager // if employee has manager, else null
        );
    }
}
