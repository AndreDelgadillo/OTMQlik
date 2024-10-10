import requests
import base64
import re
import gc
from xml.etree import ElementTree

def __decode_base64__(data, altchars: bytes = b'+/') -> bytes:
    data_bytes = data.encode('utf-8')

    data_bytes = re.sub(rb'[^a-zA-Z0-9%s]+' % altchars, b'', data_bytes)
    missing_padding = len(data_bytes) % 4
    if missing_padding:
        data_bytes += b'=' * (4 - missing_padding)
    return base64.b64decode(data_bytes, altchars)


def getFolderContents(patern_path: str,
                      headers: str,
                      server: str) -> tuple[list[str], list[str]] | Exception:
    SoapPathResponse: str = r'.//{http://xmlns.oracle.com/oxp/service/PublicReportService}'
    url: str = f"https://{server}.oraclecloud.com:443/xmlpserver/services/ExternalReportWSSService?WSDL"
    soapQueryRequestPath: str = f""" 
        <soap:Envelope xmlns:soap="http://www.w3.org/2003/05/soap-envelope" 
                    xmlns:pub="http://xmlns.oracle.com/oxp/service/PublicReportService">
        <soap:Header/>
        <soap:Body>
            <pub:getFolderContents>
                <pub:folderAbsolutePath>
                    {patern_path}
                </pub:folderAbsolutePath>
            </pub:getFolderContents>
        </soap:Body>
        </soap:Envelope>"""
    try:
        response: bytes = requests.post(url, data=soapQueryRequestPath.encode('utf-8'), headers=headers)
        absp: ElementTree = ElementTree.fromstring(response.content)
        paths: list[str] = [path.text for path in absp.findall(f'{SoapPathResponse}absolutePath')]
        names: list[str] = [name.text for name in absp.findall(f'{SoapPathResponse}displayName')]
        response.close()
        return paths, names
    except Exception as e: return e
    
def runReport(absolutepath: str,
              server: str,
              headers: dict,
              timezone: str = 'America/Mexico_City',
              sizeOfDataChunkDownload: int = -1) -> bytes | str | Exception:
    url = f"https://{server}.oraclecloud.com:443/xmlpserver/services/ExternalReportWSSService?WSDL"

    soapQueryRequest: str = f"""
        <soap:Envelope xmlns:soap="http://www.w3.org/2003/05/soap-envelope" 
                        xmlns:pub="http://xmlns.oracle.com/oxp/service/PublicReportService">
            <soap:Header/>
            <soap:Body>
                <pub:runReport>
                    <pub:reportRequest>
                        <pub:attributeFormat></pub:attributeFormat>
                        <pub:attributeTimezone>{timezone}</pub:attributeTimezone>
                        <pub:parameterNameValues>
                        <pub:item>
                            <pub:name></pub:name>
                            <pub:values>
                                <pub:item></pub:item>
                            </pub:values>
                        </pub:item>
                        </pub:parameterNameValues>
                        <pub:reportAbsolutePath>{absolutepath}</pub:reportAbsolutePath>
                        <pub:sizeOfDataChunkDownload>{sizeOfDataChunkDownload}</pub:sizeOfDataChunkDownload>
                    </pub:reportRequest>
                </pub:runReport>
            </soap:Body>
        </soap:Envelope>"""
    try:
        for _ in range(3):
            response = requests.post(url, data=soapQueryRequest.encode('utf-8'), headers=headers)
            if response.status_code == 200: break
            print(f'{absolutepath} [{response}]')
        root: ElementTree = ElementTree.fromstring(response.content)
        try:
            report_bytes: bytes = root.find('.//{http://xmlns.oracle.com/oxp/service/PublicReportService}reportBytes').text
            response.close()
            return __decode_base64__(report_bytes).decode('utf-8')
        except:
            content: str = response.content
            response.close()
            return content
    except Exception as e:
        print(f'Ha ocurrido el error {e}')
        return e

def getFolderReports(*, paths: list[str], names: list[str]) -> list[str]:
    return [paths[i] for i in range(len(names)) if paths[i].endswith('.xdo')], [names[i] for i in range(len(names)) if paths[i].endswith('.xdo')]

def headers(user: str, pas: str) -> dict[str, str]:
    return {
            'Content-Type': 'application/soap+xml;charset=UTF-8',
            'Accept-Encoding': 'gzip, deflate',
            'User-Agent': 'Apache-HttpClient/4.5.5 (Java/16.0.2)',
            'Authorization': 'Basic ' + base64.b64encode(f'{user}:{pas}'.encode()).decode()
    }

def loging(user: str, pas: str):
    """indev"""
    soapQueryRequest: str = f'''<soapenv:Envelope xmlns:soapenv="http://schemas.xmlsoap.org/soap/envelope/" xmlns:v2="http://xmlns.oracle.com/oxp/service/v2">
                                <soapenv:Header/>
                                <soapenv:Body>
                                    <v2:login>
                                        <v2:userID>{user}</v2:userID>
                                        <v2:password>{pas}</v2:password>
                                    </v2:login>
                                </soapenv:Body>
                                </soapenv:Envelope>'''
    url: str = 'https://otmgtm-a621157.otm.us2.oraclecloud.com:443/xmlpserver/services/v2/SecurityService'
    response = requests.post(url, data=soapQueryRequest.encode('utf-8'), headers=headers(user, pas))
    pass

gc.collect()

if __name__ == '__main__':
    loging('ADELGADILLO', 'StrawberrySwing1.')