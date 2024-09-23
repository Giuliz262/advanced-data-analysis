import sys
import MySQLdb
import pandas as pd
import matplotlib.pyplot as plt

try:
    db = MySQLdb.connect("localhost", "root", "", "companydata")
except MySQLdb.Error as e:
    print("No se pudo conectar a la base de datos:", e)
    sys.exit(1)
print("Conexión correcta.")

cursor = db.cursor()
try:
    cursor.execute("DROP TABLE IF EXISTS EmployeePerformance")
    print("Tabla 'EmployeePerformance' eliminada (si existía).")
    
    cursor.execute("""
        CREATE TABLE EmployeePerformance (
        id INT AUTO_INCREMENT PRIMARY KEY,
        employee__id INT,
        department VARCHAR(50),
        performance_score DECIMAL(10, 2),
        year_with_company INT,
        salary DECIMAL(10, 2)
    )
    """)
    print("Tabla 'EmployeePerformance' creada exitosamente.")
except MySQLdb.Error as e:
    print("Error al crear o eliminar la tabla:", e)
    db.close()
    sys.exit(1)

try:
    df = pd.read_json('Data.json')
    print("Datos cargados en DataFrame.")
except FileNotFoundError as e:
    print("El archivo JSON no se encontró:", e)
    db.close()
    sys.exit(1)
except ValueError as e:
    print("Error al leer el archivo JSON:", e)
    db.close()
    sys.exit(1)

try:
    data_tuples = list(df.itertuples(index=False, name=None))
    
    insert_query = """
        INSERT INTO EmployeePerformance (id, employee__id, department, performance_score, year_with_company, salary)
        VALUES (%s, %s, %s, %s, %s, %s)
    """

    cursor.executemany(insert_query, data_tuples) 
    print("Datos insertados exitosamente.")
except MySQLdb.Error as e:
    print("Error al insertar los datos:", e)

db.commit()
db.close()

print('Media, mediana y desviación estándar de rendimiento:')
print(df['performance_score'].mean())
print(df['performance_score'].median())
print(df['performance_score'].std())

print('Media, mediana y desviación estándar de salario:')
print(df['salary'].mean())
print(df['salary'].median())
print(df['salary'].std())

print('Número total de empleados por departamento:')
print(df.groupby('department').size())

print('Correlación entre años en la empresa y rendimiento:')
print(df['year_with_company'].corr(df['performance_score']))

print('Correlación entre salario y rendimiento:')
print(df['salary'].corr(df['performance_score']))

df_MK = df[df['department'] == 'Marketing']

plt.hist(df_MK['performance_score'], bins=10, edgecolor='black', alpha=0.7)
plt.title('Histograma de rendimiento para el departamento de Marketing')
plt.xlabel('Rendimiento')
plt.ylabel('Frecuencia')
plt.show()


plt.scatter(df['year_with_company'], df['performance_score'], alpha=0.7)
plt.title('Años con la empresa vs. Rendimiento')
plt.xlabel('Años con la empresa')
plt.ylabel('Rendimiento')
plt.show()

plt.scatter(df['salary'], df['performance_score'], alpha=0.7)
plt.title('Salario vs. Rendimiento')
plt.xlabel('Salario')
plt.ylabel('Rendimiento')
plt.show()