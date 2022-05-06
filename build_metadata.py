import json
from psql.dblib import config
from utils import read_query
import sys

params = config('database.ini', 'project')
ASSET_UID = params['asset_uid']

def build_metadata():
    try:
        # query postgres to get the project info
        tableName = 'projects'
        query = f"SELECT id, name, country, sector, description, kind, asset_type, version_id FROM {tableName} WHERE uid= '{ASSET_UID}'"
        rawResult_fromProjects = read_query(query)
        print(rawResult_fromProjects)
        if rawResult_fromProjects == [] or rawResult_fromProjects is None:
            raise TypeError
        else:

            rawResult_fromProjects = rawResult_fromProjects[0]

            #Assigning variables from project info
            projectid = rawResult_fromProjects[0]

        # query postgres to get the data
        tableName = 'data'
        query = f"SELECT * FROM {tableName} WHERE project_id= '{projectid}' AND processed = 'FALSE' order by id"
        rawResult_fromData = read_query(query)

        if rawResult_fromData == [] or rawResult_fromData is None:
            raise TypeError
        else:
            metadata_data = []
            size = 0
            for i, rawResult in enumerate(rawResult_fromData):

                #Assigning variables from data info
                dataid = rawResult[0]
                projectid = rawResult[1]
                start_date = rawResult[2]
                end_date = rawResult[3]
                subscriberid = rawResult[4]
                deviceid = rawResult[5]
                Foto_Arbol = rawResult[6]
                PyeHassGps = rawResult[7]
                GPS = PyeHassGps.split(' ')
                UbicacioneGPS = {
                        "latitude": GPS[0],
                        "longitude": GPS[1],
                        "altitude": GPS[2],
                        "accuracy": GPS[3]
                }
                Evaluacion_de_aplicacion = rawResult[8]
                Tipo_de_plaga_Aplicacion = rawResult[9]
                Planta_afectada = rawResult[10]
                tipo_de_plaga = rawResult[11]
                Finca = rawResult[12]
                id = rawResult[13]
                uuid = rawResult[14]
                validation_status = rawResult[15]
                #TODO: validate if None then drop the key value
                metadata_data.append({
                    "start_date":  start_date.isoformat(),
                    "end_date":  end_date.isoformat(),
                    "subscriberid":  subscriberid,
                    "deviceid":  deviceid,
                    "Foto_Arbol": Foto_Arbol,
                    "UbicacioneGPS": UbicacioneGPS,
                    "Evaluacion_de_aplicacion": Evaluacion_de_aplicacion,
                    "Tipo_de_plaga_Aplicacion": Tipo_de_plaga_Aplicacion,
                    "Planta_afectada": Planta_afectada,
                    "tipo_de_plaga": tipo_de_plaga,
                    "Finca": Finca,
                    "id": id,
                    "uuid": uuid,
                    "validation_status": validation_status
                })
                # query postgres to get the picture
                tableName = 'pictures'
                query = f"SELECT * FROM {tableName} WHERE project_id= '{projectid}' AND data_id= '{dataid}';"
                rawResult_fromPicture = read_query(query)

                if rawResult_fromPicture == [] or rawResult_fromPicture is None:
                    raise TypeError
                else:
                    metadata_picture = []
                    for rawResult in rawResult_fromPicture:

                        #Assigning variables from data info
                        index = rawResult[0]
                        projectid = rawResult[1]
                        data_id = rawResult[2]
                        id = rawResult[3]
                        instance = rawResult[4]
                        name = rawResult[5]
                        url = rawResult[6]
                        ipfshash = rawResult[7]
                
                        metadata_picture.append({
                            "instance": instance,
                            "name": name,
                            "ipfshash": ipfshash,
                            "mediaType": "png/jpg"
                        })
                    metadata_data[i]["MainFiles"] = metadata_picture
                    if i > 63:
                        metadata_project = {
                            "294857930485":{
                                "Country": rawResult_fromProjects[2],
                                "Sector": rawResult_fromProjects[3],
                                "Description": rawResult_fromProjects[4],
                                'kind': rawResult_fromProjects[5],
                                'asset_type': rawResult_fromProjects[6],
                                'version_id': rawResult_fromProjects[7],
                                'data': metadata_data
                            }
                        }
                        with open ('./metadata.json', 'w') as file:
                            json.dump(metadata_project, file, indent=4, ensure_ascii=False)
                        break
    except TypeError:
        print('something for the time being')

if __name__ == '__main__':
    build_metadata()