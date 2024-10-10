import pandas as pd
import os

def makeCSV(data: str, reportName: str) -> str:
    with open(f"{reportName}.csv", "w", encoding="utf-8") as outputCSV: 
        outputCSV.write(data)
        return os.path.abspath(f"{reportName}.csv")
    
def csvCombination(archivos, outputName, keyFild):
    outputName = outputName + ".csv"
    # Leer el primer archivo para iniciar el DataFrame combinado
    df_combinado = pd.read_csv(f'{archivos[0]}.csv')

    # Iterar sobre los archivos restantes y combinarlos
    for archivo in archivos[1:]:
        # Leer el archivo actual
        df_actual = pd.read_csv(f'{archivo}.csv')
        
        # Combinar con el DataFrame principal
        # Usamos 'outer' para asegurarnos de incluir todas las filas, incluso si no hay coincidencia en 'columna_identificadora'
        df_combinado = pd.merge(df_combinado, df_actual, on=keyFild, how='outer')

    # Guardar el DataFrame combinado en un nuevo archivo CSV
    df_combinado.to_csv(outputName, index=False)

    print("Archivos combinados con éxito.")