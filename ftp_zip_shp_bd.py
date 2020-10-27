import ftplib
import csv
import json
import zlib
from io import BytesIO,StringIO
import shapefile
import zipfile
import psycopg2

f = ftplib.FTP('geoftp.ibge.gov.br')
f.login()
directory = 'recortes_para_fins_estatisticos/malha_de_setores_censitarios/censo_2010/base_de_faces_de_logradouros_versao_2019'
f.cwd(directory)
droot= f.pwd()
dir_list = f.nlst()
def download_send_bd(ft):
    arquivos = ft.nlst()
    print(arquivos)
    mem_zip = BytesIO()
    ft.retrbinary("RETR " + arquivos[0],  mem_zip.write)
    mem_zip.seek(0)
    with zipfile.ZipFile(mem_zip, 'r') as zip:
        listfiles = zip.infolist()
        shp_arquive = {str(i.filename).split('.')[0] for i in listfiles if str(i.filename).split('.')[1]=='shp'}
        for arq in shp_arquive:
            arquive_shp = zip.open(arq+'.shp')
            arquive_dbf = zip.open(arq+'.dbf')
            r = shapefile.Reader(shp=arquive_shp, dbf=arquive_dbf)
            campos = r.fields
            geom = r.shapes()[0].__geo_interface__
            campos =[i[0].lower() for i in campos if i[0]!= 'DeletionFlag']
            instancias=r.records()
            print(len(instancias))
            data = []
            for j in range(len(instancias)):
                instancias[j].append(json.dumps(r.shapes()[j].__geo_interface__))
                instancias_edit = []
                for i in instancias[j]:
                    if i == None:
                        i='0'
                    instancias_edit.append(str(i))
                data.append(";".join(instancias_edit))
            conn = psycopg2.connect("host='localhost' port='5432' dbname='mun_covid' user='usuarioexterno' password='usuarioOGLusuarioexterno'")
            cur = conn.cursor()
            buffer = StringIO()
            data_csv_format = '\n'.join(data)
            # with open("output.csv", "w") as txt_file:
            #     txt_file.write(data_csv_format)
            buffer.write(data_csv_format)
            buffer.seek(0)
            cur.copy_from(buffer, 'faces_de_logradouros_2019', sep=';')
            conn.commit()
            cur.close()
            conn.close()
for k in dir_list:
    try:
        print(k)
        f.cwd(k)
        download_send_bd(f)
    except Exception as inst:
        print (inst)
        pass
    f.cwd(droot)
f.close()
