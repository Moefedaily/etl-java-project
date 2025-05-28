import javax.swing.*;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;

public class ETLClientImporter {
    public static void main(String[] args) {
        JFileChooser fileChooser = new JFileChooser();
        fileChooser.setCurrentDirectory(new File("./data"));
        
        int result = fileChooser.showOpenDialog(null);
        if (result != JFileChooser.APPROVE_OPTION) {
            System.out.println("Aucun fichier sélectionné.");
            return;
        }
        
        File csvFile = fileChooser.getSelectedFile();
        
        String url = "jdbc:postgresql://localhost:5432/etl_database";
        String user = "postgres";
        String password = "root";
        
        try (
            Connection connection = DriverManager.getConnection(url, user, password);
            BufferedReader br = new BufferedReader(new FileReader(csvFile))
        ) {
            
            String dropTable = "DROP TABLE IF EXISTS clients";
            connection.createStatement().execute(dropTable);
            
            String createTable = """
                CREATE TABLE clients (
                    id SERIAL PRIMARY KEY,
                    nom VARCHAR(100),
                    prenom VARCHAR(100),
                    ville VARCHAR(100),
                    address VARCHAR(100),
                    email VARCHAR(150)
                )
            """;
            connection.createStatement().execute(createTable);
            
            String insertSQL = "INSERT INTO clients (nom, prenom, ville, address, email) VALUES (?, ?, ?, ?, ?)";
            PreparedStatement pstmt = connection.prepareStatement(insertSQL);
            
            
            String line;
            int count = 0;
            
            while ((line = br.readLine()) != null) {
                String[] values = line.split(",");
                
                if (values.length >= 11) {
                    String nom = values[1].trim();          
                    String prenom = values[2].trim();       
                    String ville = values[5].trim();        
                    String address = values[3].trim();      
                    String email = values[10].trim();                           
                    pstmt.setString(1, nom);
                    pstmt.setString(2, prenom);
                    pstmt.setString(3, ville);
                    pstmt.setString(4, address);
                    pstmt.setString(5, email);
                    
                    pstmt.executeUpdate();
                    count++;
                }
            }
            
            System.out.println("Total importé: " + count + " enregistrements");
            System.out.println("Importation terminée.");
            
        } catch (Exception e) {
            System.out.println("Erreur : " + e);
        }
    }
}