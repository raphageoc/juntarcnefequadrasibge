#coding utf8
import ftplib
import csv
import zlib
from io import BytesIO,StringIO

import zipfile
import psycopg2

# f = ftplib.FTP()
# f.connect("ftp.ibge.gov.br")

f = ftplib.FTP('ftp.ibge.gov.br')
f.login()
directory = 'Censos/Censo_Demografico_2010/Cadastro_Nacional_de_Enderecos_Fins_Estatisticos'
f.cwd(directory)
droot= f.pwd()
dir_list = f.nlst()
f.cwd(dir_list[1])

arquivos = f.nlst()

mem_zip = BytesIO()

f.retrbinary("RETR " + arquivos[2],  mem_zip.write)
mem_zip.seek(0)
layout = {"codigo_da_uf":[1,2],
"codigo_do_municipio":[3,5],
"codigo_do_distrito":[8,2],
"codigo_do_subdistrito":[10,2],
"codigo_do_setor":[12,4],
"situacão_do_setor":[16,1	],
"tipo_do_logradouro":[17,20],
"titulo_do_logradouro":[37,30],
"nome_do_logradouro":[67,60],
"numero_no_logradouro":[127,8],
"modificador_do_numero":[135,7],
"elemento_1":[142,20],
"valor_1":[162,10],
"elemento_2":[172,20],
"valor_2":[192,10],
"elemento_3":[202,20],
"valor_3":[222,10],
"elemento_4":[232,20],
"valor_4":[252,10],
"elemento_5":[262,20],
"valor_5":[282,10],
"elemento_6":[292,20],
"valor_6":[312,10],
"latitude":[322,15],
"longitude":[337,15],
"localidade":[352,60],
"nulo":[412,60],
"especie_de_endereco":[472,2],
"identificacao_estabelecimento":[474,40],
"indicador_de_endereco":[514,1],
"identificacão_domicilio_coletivo":[515,30],
"numero_da_quadra":[545,3],
"numero_da_face":[548,3],
"cep":[551,8]}
with zipfile.ZipFile(mem_zip, 'r') as zip:
    listfiles = zip.infolist()
    name_arquive =listfiles[0].filename
    with zip.open(name_arquive) as f:
        data_list = []
        for line in f:
            linha_txt = str(line,'utf-8')
            line_csv = []
            for key, value in layout.items():
                position_char_ini = value[0]-1
                position_char_fin = position_char_ini+value[1]
                line_csv.append(linha_txt[position_char_ini:position_char_fin])
                line_csv.append(';')
            del line_csv[len(line_csv)-1]
            line_csv.append('\n')
            data_list.append(''.join(line_csv))
        data_csv_format = " ".join(data_list)

        conn = psycopg2.connect("host='localhost' port='5432' dbname='cnefe' user='raphael' password='rapha9182'")
        cur = conn.cursor()
        buffer = StringIO()
        buffer.write(data_csv_format)
        buffer.seek(0)
        # print(buffer.read())
        cur.copy_from(buffer, 'cnefe', sep=';')
        conn.commit()
        cur.close()
        conn.close()
        # with open("output.csv", "w") as txt_file:
            # txt_file.write(" ".join(data_list))
        # with open('cnefe.csv', 'w', newline='') as file:
        #     writer = csv.writer(file, delimiter=';')
        #     writer.writerows(data_list)




        # csvwriter.writerow(line_csv)
        # writer = csv.writer(file, delimiter='|')
        # writer.writerows(data_list)











# print (layout['codigo_da_uf'])
