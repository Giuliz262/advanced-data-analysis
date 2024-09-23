import sys
import MySQLdb
import pandas as pd
import matplotlib.pyplot as plt

class DbManager:
    def __init__(self, host, user, password, dbname):
        self.host = host
        self.user = user
        self.password = password
        self.dbname = dbname
        self.connection = None
        self.cursor = None
    def connect(self):
        try:
            self.connection = MySQLdb.connect(self.host, self.user, self.password, self.dbname)
            self.cursor = self.connection.cursor()
            print("Conexión correcta.")
        except MySQLdb.Error as e:
            print("No se pudo conectar a la base de datos:", e)
            sys.exit(1)
    def close(self):
        if self.connection:
            self.connection.close()
    def execute_query(self, query, data=None, many=False):
        try:
            if many:
                self.cursor.executemany(query, data)
            else:
                self.cursor.execute(query)
            self.connection.commit()
        except MySQLdb.Error as e:
            print("Error ejecutando la consulta:", e)
            self.close()
            sys.exit(1)
    def drop_table(self, table_name):
        self.execute_query(f"DROP TABLE IF EXISTS {table_name}")
        print(f"Tabla '{table_name}' eliminada (si existía).")
    def create_table(self, create_table_query):
        self.execute_query(create_table_query)
        print("Tabla creada exitosamente.")

class DProcessor:
    def __init__(self, file_path):
        self.df = self.load_data(file_path)

    def load_data(self, file_path):
        try:
            df = pd.read_json(file_path)
            print("Datos cargados en DataFrame.")
            return df
        except FileNotFoundError as e:
            print("El archivo JSON no se encontró:", e)
            sys.exit(1)
        except ValueError as e:
            print("Error al leer el archivo JSON:", e)
            sys.exit(1)

    def insert_data_to_db(self, db_manager, table_name):
        data_tuples = list(self.df.itertuples(index=False, name=None))
        insert_query = f"""
            INSERT INTO {table_name} (id, employee__id, department, performance_score, year_with_company, salary)
            VALUES (%s, %s, %s, %s, %s, %s)
        """
        db_manager.execute_query(insert_query, data_tuples, many=True)
        print("Datos insertados exitosamente.")

    def generate_statistics(self):
        print('Media, mediana y desviación estándar de rendimiento:')
        print(self.df['performance_score'].mean())
        print(self.df['performance_score'].median())
        print(self.df['performance_score'].std())

        print('Media, mediana y desviación estándar de salario:')
        print(self.df['salary'].mean())
        print(self.df['salary'].median())
        print(self.df['salary'].std())

        print('Número total de empleados por departamento:')
        print(self.df.groupby('department').size())

        print('Correlación entre años en la empresa y rendimiento:')
        print(self.df['year_with_company'].corr(self.df['performance_score']))

        print('Correlación entre salario y rendimiento:')
        print(self.df['salary'].corr(self.df['performance_score']))

    def plot_histogram(self, department):
        df_dept = self.df[self.df['department'] == department]
        plt.hist(df_dept['performance_score'], bins=10, edgecolor='black', alpha=0.7)
        plt.title(f'Histograma de rendimiento para el departamento de {department}')
        plt.xlabel('Rendimiento')
        plt.ylabel('Frecuencia')
        plt.show()

    def plot_scatter(self, x_col, y_col, x_label, y_label, title):
        plt.scatter(self.df[x_col], self.df[y_col], alpha=0.7)
        plt.title(title)
        plt.xlabel(x_label)
        plt.ylabel(y_label)
        plt.show();

def main():
    db_manager = DbManager("localhost", "root", "", "companydata")
    db_manager.connect()

    db_manager.drop_table('EmployeePerformance')
    
    create_table_query = """
        CREATE TABLE EmployeePerformance (
        id INT AUTO_INCREMENT PRIMARY KEY,
        employee__id INT,
        department VARCHAR(50),
        performance_score DECIMAL(10, 2),
        year_with_company INT,
        salary DECIMAL(10, 2)
    )
    """
    db_manager.create_table(create_table_query)
    data_processor = DProcessor('Data.json')
    data_processor.insert_data_to_db(db_manager, 'EmployeePerformance')
    data_processor.generate_statistics()
    data_processor.plot_histogram('Marketing')
    data_processor.plot_scatter('year_with_company', 'performance_score', 'Años con la empresa', 'Rendimiento', 'Años con la empresa vs. Rendimiento')
    data_processor.plot_scatter('salary', 'performance_score', 'Salario', 'Rendimiento', 'Salario vs. Rendimiento')

    db_manager.close()
if __name__ == "__main__":
    main()