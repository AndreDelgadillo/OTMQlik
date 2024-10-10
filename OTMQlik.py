import os
import json
import OTMrunReport
import CSV_
import time
import concurrent.futures as ft
from colorama import Fore, Style, init
from datetime import datetime
from Tenant import Qlik, Script
from diesel import diesel
from andonFlex import andonFlex

init()
ExceptionList: list[str] = list()
dateFormat: str = r"^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}\.\d{3}\+00:00$"

def run_report_and_upload(
                        i: int
                        #, reportNumber: Generator[int, None, None]
                        , paths_list: list[str]
                        , names_list: list[str]
                        , server: str
                        , header: dict[str: str]
                        , nQlik: Qlik) -> None:
    print(f'Ejecutando: {Fore.GREEN + names_list[i] + Style.RESET_ALL} {datetime.now()}')
    try:
        report_result = OTMrunReport.runReport(paths_list[i], server, header, 'csv')
        CSV_.makeCSV(report_result, names_list[i])
    except Exception as e:
        ExceptionList.append(e)
        print(e)
        with open(configuration['excepcions_log'], 'w') as f:
            for item in ExceptionList: f.write('%s\n' % item)

    print(f'Subiendo: {Fore.YELLOW + names_list[i] + Style.RESET_ALL}: {datetime.now()}')
    try:
        nQlik.updateFile(file_name=names_list[i], file_extension='.csv')
    except Exception as e:
        ExceptionList.append(e)
        print(e)
        with open(configuration['excepcions_log'], 'w') as f:
            for item in ExceptionList: f.write('%s\n' % item)
    print(f'{Fore.BLUE + names_list[i] + Style.RESET_ALL} terminado | Faltan {Fore.YELLOW}{i}{Style.RESET_ALL} reportes por descargar {datetime.now()}')

jsonFile = 'config.json'
configuration = json.load(open(jsonFile, 'r'))

for x in range(len(json.load(open(jsonFile, 'r'))['systems'])):
    os.system('cls' if os.name == 'nt' else 'clear')
    print(f'Iniciando sistema {configuration["systems"][x]}')
    seconds = int(configuration['seconds'])
    minutes = int(configuration['minutes'])
    hours = int(configuration['hours'])
    nQlik = Qlik(config=jsonFile)
    server = configuration['Oracle_server'][x]
    header = OTMrunReport.headers(configuration['Oracle_user'][x], 
                                  configuration['Oracle_password'][x])
    paths_list, names_list = OTMrunReport.getFolderContents(configuration['Oracle_folder'][x],
                                header,
                                server)
    reportCount = len(paths_list)

    print(f'Reportes a Descargar:')
    for i in range(reportCount): print(f'{i+1}: {Fore.YELLOW + names_list[i] + Style.RESET_ALL}')   
    print(f'Se descargarán {Fore.MAGENTA}{reportCount}{Style.RESET_ALL} reportes')
    with ft.ThreadPoolExecutor() as executor:
        futures: list = []
        for i in range(reportCount):
                futures.append(executor.submit(run_report_and_upload
                                                , i
                                                , paths_list
                                                , names_list
                                                , server
                                                , header
                                                , nQlik))
                time.sleep(.1)
        time.sleep(.5)
        ft.wait(futures)
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
    now = datetime.now()
    time.sleep(10)

andonFlex(nQlik, configuration['andonFlex'])
diesel(configuration['diesel'])