import io.github.cdimascio.dotenv.Dotenv;
import java.io.FileWriter;
import java.io.PrintWriter;
import java.sql.*;

public class ClientExporter {
    public static void main(String[] args) {
        System.out.println("=== Client Exporter ===");
        
        Dotenv dotenv = null;
        try {
            dotenv = Dotenv.load();
            System.out.println("Fichier .env chargé avec succès");
        } catch (Exception e) {
            System.out.println("Fichier .env non trouvé, utilisation des valeurs par défaut");
        }
        
        String dbUrl = (dotenv != null) ? dotenv.get("DB_URL") : "jdbc:postgresql://localhost:5432/etl_database";
        String dbUser = (dotenv != null) ? dotenv.get("DB_USER") : "postgres";
        String dbPassword = (dotenv != null) ? dotenv.get("DB_PASSWORD") : "root";
        
        try (
            Connection connection = DriverManager.getConnection(dbUrl, dbUser, dbPassword);
            
            Statement statement = connection.createStatement();
            ResultSet resultSet = statement.executeQuery("SELECT nom, prenom, ville, address, email FROM clients ORDER BY id");
            
            FileWriter fileWriter = new FileWriter("output/clients_export.csv");
            PrintWriter printWriter = new PrintWriter(fileWriter);
        ) {
            System.out.println("Connexion à PostgreSQL réussie !");
            
            printWriter.println("nom,prenom,ville,address,email");
            
            int count = 0;
            while (resultSet.next()) {
                count++;
                
                String nom = escape(resultSet.getString("nom"));
                String prenom = escape(resultSet.getString("prenom"));
                String ville = escape(resultSet.getString("ville"));
                String address = escape(resultSet.getString("address"));
                String email = escape(resultSet.getString("email"));
                
                printWriter.printf("%s,%s,%s,%s,%s%n", 
                    nom, prenom, ville, address, email);
                
                if (count % 1000 == 0) {
                    System.out.println("Exporté : " + count + " enregistrements");
                }
            }
            
            System.out.println("\n=== EXPORT TERMINÉ ===");
            System.out.println("Fichier créé : output/clients_export.csv");
            System.out.println("Total enregistrements exportés : " + count);
            
        } catch (Exception e) {
            System.out.println("Erreur :" + e);
            e.printStackTrace();
        }
    }
    
    private static String escape(String field) {
        if (field == null) return "";
        if (field.contains(",") || field.contains("\"")) {
            return "\"" + field.replace("\"", "\"\"") + "\"";
        }
        return field;
    }
}