package src.model.dao.impl;

import src.db.DB;
import src.db.DbException;
import src.model.dao.SellerDao;
import src.model.entities.Department;
import src.model.entities.Seller;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class SellerDaoJDBC implements SellerDao {
    private Connection connection;

    public SellerDaoJDBC (Connection connection){
        this.connection = connection;
    }

    @Override
    public void insert(Seller obj) {

    }

    @Override
    public void update(Seller obj) {

    }

    @Override
    public void deleteById(Integer id) {

    }

    @Override
    public Seller findById(Integer id) {
        PreparedStatement statement = null;
        ResultSet resultSet = null;
        try {
            statement = connection.prepareStatement(
                    "SELECT seller.*,department.Name as DepName "
                    + "FROM seller INNER JOIN department "
                    + "ON seller.DepartmentId = department.Id "
                    + "WHERE seller.Id = ? "
            );
            statement.setInt(1, id);
            resultSet = statement.executeQuery();
            if (resultSet.next()){
                Department department = instantiateDepartment (resultSet);
                Seller obj = instantiateSeller (resultSet, department);
                return obj;
            }
            return null;
        } catch (SQLException e) {
            throw new DbException(e.getMessage());
        }
        finally {
            DB.closeStatement(statement);
            DB.closeResultSet(resultSet);
        }
    }

    private Seller instantiateSeller(ResultSet resultSet, Department department) throws SQLException {
        Seller obj =new Seller();
        obj.setId(resultSet.getInt("id"));
        obj.setName(resultSet.getString("name"));
        obj.setEmail(resultSet.getString("email"));
        obj.setBaseSalary(resultSet.getDouble("basesalary"));
        obj.setBirthDate(resultSet.getDate("birthdate"));
        obj.setDepartment(department);

        return obj;
    }

    private Department instantiateDepartment(ResultSet resultSet) throws SQLException {
        Department department = new Department();
        department.setId(resultSet.getInt("departmentid"));
        department.setName(resultSet.getString("depname"));

        return department;
    }

    @Override
    public List<Seller> findAll() {
        PreparedStatement statement = null;
        ResultSet resultSet = null;
        try {
            statement = connection.prepareStatement(
                    "SELECT seller.*, department.name AS depname "
                            + "FROM seller INNER JOIN department "
                            + "ON seller.departmentid = department.id "
                            + "ORDER BY seller.name");

            resultSet = statement.executeQuery();

            List <Seller> list = new ArrayList<>();
            Map<Integer, Department> map = new HashMap();

            while (resultSet.next()){
                Department department = map.get(resultSet.getInt("DepartmentId"));

                if (department == null){
                    department = instantiateDepartment(resultSet);
                    map.put(resultSet.getInt("DepartmentId"), department);
                }

                Seller obj = instantiateSeller (resultSet, department);
                list.add(obj);
            }
            return list;
        } catch (SQLException e) {
            throw new DbException(e.getMessage());
        }
        finally {
            DB.closeStatement(statement);
            DB.closeResultSet(resultSet);
        }
    }

    @Override
    public List<Seller> findByDepartment(Department department) {
        PreparedStatement statement = null;
        ResultSet resultSet = null;
        try {
            statement = connection.prepareStatement(
                    "SELECT seller.*, department.name AS depname "
                    + "FROM seller INNER JOIN department "
                    + "ON seller.departmentid = department.id "
                    + "WHERE seller.departmentid = ? "
                    + "ORDER BY seller.name");

            statement.setInt(1, department.getId());

            resultSet = statement.executeQuery();

            List <Seller> list = new ArrayList<>();
            Map<Integer, Department> map = new HashMap();

            while (resultSet.next()){
                department = map.get(resultSet.getInt("DepartmentId"));

                if (department == null){
                    department = instantiateDepartment(resultSet);
                    map.put(resultSet.getInt("DepartmentId"), department);
                }

                Seller obj = instantiateSeller (resultSet, department);
                list.add(obj);
            }
            return list;
        } catch (SQLException e) {
            throw new DbException(e.getMessage());
        }
        finally {
            DB.closeStatement(statement);
            DB.closeResultSet(resultSet);
        }
    }
}
