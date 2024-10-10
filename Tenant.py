from qsaas.qsaas import Tenant
import json
import os
import re
import time
from pandas import DataFrame, read_csv
from datetime import datetime
from colorama import Fore, Style, init
try: from OTMrunReport import *
except: pass

init()

class Script:
    def __init__(self
                , name: str
                , fileLocalSource: str
                , extension: str
                , qlikStorage: str = None) -> None:
        self.extension = extension
        self.name = name
        self.data: DataFrame = read_csv(fileLocalSource)
        self.qlikStorage = qlikStorage if qlikStorage else r'lib://DataFiles/'
        self.strFields = list()
    
    def dateFile(self, field: str) -> str:
        return f"Timestamp(Timestamp#({field}, '$(TimestampFormat)')) as {field}"
    
    def mkQvs(self, dateFormat: str) -> None:
        for field in self.tmpFields:
            if self.isDate(field, dateFormat): self.strFields.append(self.dateFile(field))
            else: self.strFields.append(field)
        script = f"""
        [{self.name}]:
        LOAD
        {self.mkFields}
        {self.mkFrom}
        (txt, utf8, embedded labels, delimiter is ',', msq);"""
        with open(f"{self.name}.qvs", "w", encoding="utf-8") as outputCSV:
            outputCSV.write(script)
    
    @property
    def mkFields(self) -> str:
        return ',\n\t'.join(self.strFields)
    
    @property
    def mkFrom(self) -> str:
        return f'FROM [{self.qlikStorage}{self.name}{self.extension}]'
    @property
    def tmpFields(self) -> list[str]:
        return list(self.data.columns)
    
    def masterScript(self, scriptNames: list[str]) -> str:
        scriptList: list[str] = [f"$(Must_Include={self.qlikStorage}{scriptName}.qvs);\n" for scriptName in scriptNames]
        scriptStores: list[str] = [f"Store {scriptName} into {self.qlikStorage}{scriptName}.qvd] (qvd);\n" for scriptName in scriptNames]
        scriptDrops: list[str] = [f"Drop Table {scriptName};\n" for scriptName in scriptNames]
        masterScripts: list[str] = list()
        for i in range(0, len(scriptNames)):
            masterScripts.append(scriptList[i])
            masterScripts.append(scriptStores[i])
            masterScripts.append(scriptDrops[i])
        return f"\n".join(masterScripts)
    
    def mkMaster(self, scriptNames: list[str]) -> None:
        with open(f"{self.name}.qvs", "w", encoding="utf-8") as outputCSV: outputCSV.write(self.masterScript(scriptNames))
    
    def isDate(self, field: str, dateFormat: str) -> bool:
        data: DataFrame = self.data.dropna(subset=[field])
        series: list[str] = [data[field].sample().values[0] for _ in range(1, 10)]
        for serie in series:
            if not isinstance(serie, str): return False
            if not re.match(dateFormat, serie): return False
        return True
    
    def isNumber(self, field: str) -> bool:
        data: DataFrame = self.data.dropna(subset=[field])
        series: list[str] = [data[field].sample().values[0] for _ in range(1, 10)]
        for serie in series:
            try: float(serie) 
            except: return False
        return True


class Qlik(Tenant):

    def Qlik_users(self) -> None:
        users = self.get('users')
        for user in users: print(user['id'])

    def Qlik_spaces(self) -> None:
        spaces = self.get('spaces')
        for space in spaces: print(f"{space['name']} | {space['id']} | {space['type']}")

    def Qlik_Apps(self) -> None:
        apps = self.get('items', params={"resourceType":"app"})
        for app in apps: print(f"{app}")
    
    def app_info(self, app_name: str, space_id: str) -> dict[str: str] | bool:
        paramsTmp = {"resourceType":"app", "spaceID": f"{space_id}"}
        apps: list[dict[str: str]] = self.get('items', params=paramsTmp)
        for app in apps: 
            if app_name == app['name'] and space_id == app['spaceId']: return app
        return False

    def file_info(self, file_name: str) -> dict[str: str] | bool:
        paramsTmp = {"name": f"{file_name}"}
        files: list[dict[str: str]] = self.get('qix-datafiles', params=paramsTmp)
        for file in files:
            if file_name == file['name']: return file
        return False

    def space_info(self, space_name: str) -> dict[str: str] | bool:
        spaces: list[dict[str: str]] = self.get('spaces')
        for space in spaces:
            if space_name == space['name']: return space
        return False

    def Upload_File(self, file_path: str = False, 
                    file_name: str = False, 
                    file_extension: str = False) -> bool | ValueError:
        try:
            if file_extension: file_name = file_name + file_extension
            if file_path: 
                with open(os.path.join(file_path, file_name), 'rb') as f: file_content = f.read()
            else:
                with open(file_name, 'rb') as f: file_content = f.read()
            self.post('qix-datafiles', file_content, 
                           params={"name": file_name})
        except ValueError as e: return e 
        return True

    def updateFile(self, file_path: str = False, 
                    file_name: str = False, 
                    file_extension: str = False) -> bool | ValueError:
        args = (file_path, file_name, file_extension)
        file = self.file_info(file_name+file_extension)
        if file: self.Delete_File(file['id'])
        return self.Upload_File(*args)
    
    def Delete_File(self, file_id: str) -> bool | ValueError:
        try: self.delete(f'qix-datafiles/{file_id}')
        except ValueError as e: return e 
        return True
    
    def reload_App(self, app_info: dict[str: str]) -> tuple[str, str]:
        now: datetime = (datetime.now())
        reload_app: dict = self.post('reloads', json.dumps({"appId": app_info['resourceId']}))
        reload_id: str = reload_app['id']
        status: str = None
        i = 1
        while status not in ['SUCCEEDED', 'FAILED']:
            time.sleep(1)
            status = self.get('reloads/' + reload_id)['status']
            if status not in ['SUCCEEDED', 'FAILED']:
                print(f"The Status of {Fore.YELLOW + app_info['name'] + Style.RESET_ALL} is {Fore.LIGHTCYAN_EX + status + Style.RESET_ALL}, Elapsed Time (Sec): {datetime.now() - now}")
            i += 1
        status = self.get('reloads/' + reload_id)['status']
        print(self.get('reloads/' + reload_id)['log'])
        #if status == 'SUCCEEDED': status = Fore.GREEN + status
        #else: status = Fore.RED + status
        return (Fore.GREEN if status == 'SUCCEEDED' else Fore.RED ) + status + Style.RESET_ALL, self.get('reloads/' + reload_id)['log']

if __name__ == '__main__':
    file = r'C:\Users\adelgadillo\OneDrive\Esgari\Entorno de pruebas\XDO_4_TRACKING.csv'
    dateFormat: str = r"^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}\.\d{3}\+00:00$"
    nScript = Script('Prueba', file, 'Prueba', '.csv')
    nScript.mkQvs(dateFormat)