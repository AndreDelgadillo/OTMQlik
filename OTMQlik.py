import os
import json
import OTMrunReport
import CSV_
import time
from andonFlex import andonFlex
import concurrent.futures as ft
from colorama import Fore, Style, init
from datetime import datetime
from Tenant import Qlik, Script
from diesel import diesel

init()
ExceptionList: list[str] = list()
statusReportList: list[str] = ["EJECUTANDO", "SUBIENDO", "ERROR", "TERMINADO"]
statusLen: int = max([len(n) for n in statusReportList])

def imprimir_encabezado_tabla():
    """Imprimir el encabezado de la tabla."""
    print(f'{"#":<{indiceLen}} {"Reporte":<{reportLen}} {"Estatus":<{statusLen}} {"Fecha/Hora"}')

def mover_cursor(fila: int):
    """Mueve el cursor a una fila específica."""
    print(f"\033[{fila};0H", end='')

def actualizar_fila_tabla(fila: int, indice: int, reporte: str, estatus: str, color: str):
    """Actualizar la fila con el estado actual en una línea específica."""
    ahora = datetime.now()
    mover_cursor(fila)  # Mueve el cursor a la fila correspondiente
    print(f'{indice:<{indiceLen}} {reporte:<{reportLen}} {color + estatus + Style.RESET_ALL:<{statusLen}} {str(ahora): >{statusLen+len(str(ahora))}}', flush=True)

def run_report_and_upload(
                        i: int
                        , fila: int
                        , paths_list: list[str]
                        , names_list: list[str]
                        , server: str
                        , header: dict[str, str]
                        , nQlik: Qlik) -> None:
    # Imprimir el estado "Ejecutando"
    actualizar_fila_tabla(fila, i+1, names_list[i], "EJECUTANDO", Fore.GREEN)

    try:
        report_result = OTMrunReport.runReport(paths_list[i], server, header, 'csv')
        reportPath = CSV_.makeCSV(report_result, names_list[i])
    except Exception as e:
        ExceptionList.append(e)
        actualizar_fila_tabla(fila, i+1, names_list[i], "ERROR", Fore.RED)
        with open(configuration['excepcions_log'], 'w') as f:
            for item in ExceptionList: f.write('%s\n' % item)
        return

    # Actualizar el estado a "Subiendo"
    actualizar_fila_tabla(fila, i+1, names_list[i], "SUBIENDO", Fore.YELLOW)

    try:
        nQlik.updateFile(file_name=names_list[i], file_extension='.csv')
    except Exception as e:
        ExceptionList.append(e)
        actualizar_fila_tabla(fila, i+1, names_list[i], "ERROR", Fore.RED)
        with open(configuration['excepcions_log'], 'w') as f:
            for item in ExceptionList: f.write('%s\n' % item)
        return

    # Actualizar el estado a "Terminado"
    actualizar_fila_tabla(fila, i+1, names_list[i], "TERMINADO", Fore.BLUE)


jsonFile = 'config.json'
for x in range(len(json.load(open(jsonFile, 'r'))['systems'])):
    os.system('cls' if os.name == 'nt' else 'clear')
    configuration = json.load(open(jsonFile, 'r'))
    print(f'{Fore.MAGENTA}Iniciando sistema {configuration["systems"][x]}{Style.RESET_ALL}')
    seconds = int(configuration['seconds'])
    minutes = int(configuration['minutes'])
    hours = int(configuration['hours'])

    nQlik = Qlik(config=jsonFile)
    server = configuration['Oracle_server'][x]   
    header = OTMrunReport.headers(configuration['Oracle_user'][x], 
                                  configuration['Oracle_password'][x])

    paths_list, names_list = OTMrunReport.getFolderContents(configuration['Oracle_folder'][x], header, server)
    reportCount = len(paths_list)

    maxLenName: int = max([len(n) for n in names_list])
    indiceLen: int = len(str(reportCount))  # Longitud de los índices
    reportLen: int = maxLenName + 5  # Ajuste de longitud del nombre del reporte
    statusLen: int = max([len(n) for n in statusReportList])  # Longitud máxima del estatus

    # Imprimir encabezado de la tabla
    imprimir_encabezado_tabla()

    # Imprimir filas iniciales
    for i in range(reportCount):
        print(f'{i+1:<{indiceLen}} {names_list[i]:<{reportLen}} {"Esperando":<{statusLen}} {"":<20}')

    # Utilizar ThreadPoolExecutor para manejar múltiples hilos
    with ft.ThreadPoolExecutor() as executor:
        futures: list = []
        for i in range(reportCount):
            # La fila donde cada reporte será actualizado es i + 3 (ya que las primeras 3 líneas son el encabezado)
            futures.append(executor.submit(run_report_and_upload
                                           , i
                                           , i + 3  # Fila donde imprimir cada reporte
                                           , paths_list
                                           , names_list
                                           , server
                                           , header
                                           , nQlik))
            time.sleep(.1)
        
        # Esperar a que todos los hilos terminen
        ft.wait(futures)

    # Evitar repetición de reportes
    # Imprimir solo una vez al final la operación global para Qlik
    try:
        space_info = nQlik.space_info(configuration['Qlik_space'])
        app_info = nQlik.app_info(configuration['Qlik_app'][x], space_info['id'])
        print(nQlik.reload_App(app_info)[0])
        nScript = Script('Master', None, None)
        nScript.mkMaster(names_list)
    except Exception as e:
        ExceptionList.append(e)
        with open(configuration['excepcions_log'], 'w') as f:
            for item in ExceptionList: f.write('%s\n' % item)
        print(ExceptionList)

    time.sleep(10)


diesel(nQlik, configuration['diesel'])
andonFlex(nQlik, configuration['andonFlex']) 