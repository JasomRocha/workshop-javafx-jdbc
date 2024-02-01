package model.dao.impl;

import db.DB;
import db.DbException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import model.dao.DepartmentDao;
import model.entities.Department;



public class DepartmentDaoJDBC implements DepartmentDao {

    Connection conn;
    
    public DepartmentDaoJDBC(Connection conn) {
        this.conn = conn;
    }
    
    @Override
    public void insert(Department obj) {
       PreparedStatement st = null;
       
       try{
           st =  conn.prepareStatement("INSERT INTO department (Name) VALUES (?)", Statement.RETURN_GENERATED_KEYS);
           st.setString(1, obj.getName());
           
           int rowsAffected = st.executeUpdate();
           
           if(rowsAffected > 0){
               ResultSet rs = st.getGeneratedKeys();
               if(rs.next()){
                   int id = rs.getInt(1);
                   obj.setId(id);
               }
               DB.closeResultSet(rs);
           }
           else{
               throw new DbException("Unexpected Error no rows are affected");
           }
       }
       catch(SQLException e){
           throw new DbException(e.getMessage());
       }  
       finally{
           DB.closeStatement(st);
       }
    }

    @Override
    public void update(Department obj) {
        PreparedStatement st = null;
       
       try{
           st =  conn.prepareStatement("UPDATE department SET Name = ? WHERE Id=?");
           st.setString(1, obj.getName());
           st.setInt(2, obj.getId());
           st.executeUpdate();
       }
       catch(SQLException e){
           throw new DbException(e.getMessage());
       }  
       finally{
           DB.closeStatement(st);
       }
    }

    @Override
    public void deleteById(Integer id) {
        PreparedStatement st = null;
        try{
            st = conn.prepareStatement("DELETE FROM department WHERE Id = ?");
            st.setInt(1, id);
            st.executeUpdate();
        }
         catch(SQLException e){
           throw new DbException(e.getMessage());
       }
       finally{
           DB.closeStatement(st);
       }
    }

    @Override
    public Department findById(Integer id) {
        PreparedStatement st = null; //Instanciando o prparedStatement iniciando com null 
        ResultSet rs = null; //Instanciando o resultSet iniciando com null
        
        try{
          st = conn.prepareStatement("SELECT * FROM department WHERE (Id = ?)"); //Passo o comando sql que desejo que seja executado
          st.setInt(1, id);
          rs = st.executeQuery(); //variavel que armazena o retorno da função execute, no formato de tabela
          
          
          if(rs.next()){
            Department dep = instantiateDepartment(rs); //recebe o retorno da função que instancia o departamento 
            return dep; //retorno o departamento encontrado pelo id informado
          }
          
          return null; // retorno null caso nao exista
        }
        catch(SQLException e){
            throw new DbException(e.getMessage()); //uso a exceção personalizada, pegando a msg da sql exception
        }
        finally{
            DB.closeStatement(st); //fecho meus recursos externos
            DB.closeResultSet(rs); //fecho meus recursos externos
        }
    }

    @Override
    public List<Department> findAll() {
        PreparedStatement st = null; //Instanciando o prparedStatement iniciando com null 
        ResultSet rs = null; //Instanciando o resultSet iniciando com null
        
        try{
          st = conn.prepareStatement("SELECT * FROM department"); //Passo o comando sql que desejo que seja executado
          rs = st.executeQuery(); //variavel que armazena o retorno da função execute, no formato de tabela
          
          List<Department> list = new ArrayList<>(); //List que ir[a receber os dados lidos da tabela
           
          while(rs.next()){
              Department dep = instantiateDepartment(rs); //recebe o retorno da função que instancia o departamento 
              list.add(dep); // adciono na lista
          }
          
          return list; // retorno a lista
        }
        catch(SQLException e){
            throw new DbException(e.getMessage()); //uso a exceção personalizada, pegando a msg da sql exception
        }
        finally{
            DB.closeStatement(st); //fecho meus recursos externos
            DB.closeResultSet(rs); //fecho meus recursos externos
        }
    }
    
    
    private Department instantiateDepartment(ResultSet rs) throws SQLException {
        Department dep = new Department(); //instancio minha dep 
        
        dep.setId(rs.getInt("Id")); //seto o id com o id ttrazido da tabela no bd
        dep.setName(rs.getString("Name")); //seto o nome igualmente
        
        return dep; //retorno a variavel dep tipo Department
    }
    
}
