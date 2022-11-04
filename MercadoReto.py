from datetime import datetime
from typing import List
import json
import sqlite3
import os
import csv
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
import smtplib

#Variables globales 
APP_PATH = os.getcwd()
CSV_PATH = APP_PATH+'\\user_manager.csv'
JSON_PATH = APP_PATH+'\\dblist.json'
DB_PATH = APP_PATH+'\\BDListUserManager.db'  
cnn = sqlite3.connect(DB_PATH)
FechaSistema = datetime.today().strftime('%Y-%m-%d %H:%M')

#Registrar errores en la BD del proceso
def registrar_errores_proceso(Nombre_Funcion,MensajeError,FechaRegistro):
    try:
        cur = cnn.cursor()
        sql=''' INSERT INTO registro_errores_proceso(Nombre_funcion,Mensaje_error,Fecha_registro) VALUES ('{}',"{}",'{}') '''.format(Nombre_Funcion,MensajeError,FechaRegistro)
        cur.execute(sql)
        if int(cur.rowcount) > 0:
            print("Error almacenado correctamente, " + "Funcion:" + Nombre_Funcion + " , Error: " + str(MensajeError))
        else:
            print("Error no almacenado correctamente, " + "Funcion:" + Nombre_Funcion + " , Error: " + str(MensajeError))
        cnn.commit()
        cur.close()

    except BaseException as err: 
        if  "MySQL Connection not available" in str(err.args[0]):
            print("No se almaceno en la BD el registro de error: " +err.args[0] + " " + FechaSistema)
        else:
            print("No se almaceno en la BD el registro de error: " +err.args[0] + " " + FechaSistema)
        cur.close()
        raise

#Crear estructura de la base de datos
def crearBDAux():
    try:
        filesize = os.path.getsize(DB_PATH)
        if filesize == 0:
            if not os.path.exists(CSV_PATH):
                print("No se encontro el CSV con nombre: user_manager.csv en la ruta: " + str(APP_PATH)+"\\tratamiento")
                return 0
            
            if not os.path.exists(JSON_PATH):
                print("No se encontro el json con nombre: dblist.json en la ruta: " + str(APP_PATH)+"\\tratamiento")
                return 0
            
            log_proceso= """
            CREATE TABLE "log_proceso" (
                    "Id"	INTEGER,
                    "Nombre_funcion"	TEXT NOT NULL,
                    "Mensaje"	TEXT NOT NULL,
                    "Fecha_registro"	TEXT NOT NULL,
                    PRIMARY KEY("Id" AUTOINCREMENT)
                );
            """
            registro_errores_proceso = """
            CREATE TABLE "registro_errores_proceso" (
                    "Id"	INTEGER NOT NULL,
                    "Nombre_funcion"	TEXT NOT NULL,
                    "Mensaje_error"	TEXT NOT NULL,
                    "Fecha_registro"	TEXT NOT NULL,
                    PRIMARY KEY("Id" AUTOINCREMENT)
                );
            """
            
            tbl_dbAttributes = """
            CREATE TABLE "tbl_dbAttributes" (
                    "int_iddb"	INTEGER NOT NULL,
                    "int_idOwner"	INTEGER NOT NULL,
                    "int_idConfidentiality_CriticalityLevel"	INTEGER NOT NULL,
                    "int_integrity_CriticalityLevel"	INTEGER NOT NULL,
                    "int_availability_CriticalityLevel"	INTEGER NOT NULL,
                    "int_Clasification"	INTEGER NOT NULL,
                    "bit_sendMail" INTEGER NOT NULL,
                    "Date_UpdateStatus"	TEXT NOT NULL,
                    PRIMARY KEY("int_iddb")
                );
            """
            
            tbl_dbOwner = """
            CREATE TABLE "tbl_dbOwner" (
                    "str_name"	TEXT NOT NULL,
                    "str_uid"	TEXT NOT NULL,
                    "str_email"	TEXT NOT NULL,
                    PRIMARY KEY("str_uid")
                );
            """
            
            tbl_db = """
            CREATE TABLE "tbl_db" (
                    "int_iddb"	INTEGER NOT NULL,
                    "str_name_db"	TEXT NOT NULL,
                    PRIMARY KEY("int_iddb" AUTOINCREMENT)
                );
            """
            
            tbl_CriticalityLevel = """
            CREATE TABLE "tbl_CriticalityLevel" (
                    "int_idCriticalityLevel"	INTEGER NOT NULL,
                    "str_Description"	TEXT NOT NULL,
                    PRIMARY KEY("str_Description")
                );
            """
            
            tbl_dbUserManager = """
            CREATE TABLE "tbl_dbUserManager" (
                    "int_rowID_UserManager"	INTEGER NOT NULL,
                    "str_userID"	TEXT NOT NULL,
                    "str_userState"	TEXT NOT NULL,
                    "str_userManager"	TEXT NOT NULL,
                    PRIMARY KEY("str_userID")
                );
            """
            
        
            cur = cnn.cursor()
            cur.execute(registro_errores_proceso)
            cur.execute(log_proceso)
            cur.execute(tbl_dbAttributes)
            cur.execute(tbl_dbOwner)
            cur.execute(tbl_db)
            cur.execute(tbl_CriticalityLevel)
            cur.execute(tbl_dbUserManager)
            
            
            #leer archivo cvs  
            archivo = open(CSV_PATH,encoding = 'utf-8')
            filas   = csv.reader(archivo,delimiter=",")
            lista = list(filas)
            tuplaa = tuple(lista)
            

            #insertar
            conexion = sqlite3.connect(DB_PATH)
            cursor   = conexion.cursor()

            cursor.executemany("""INSERT INTO tbl_dbUserManager
            ('int_rowID_UserManager','str_userID','str_userState','str_userManager')  
            VALUES  
            (?,?,?,?)""",tuplaa)
            
            cursor.execute("""INSERT INTO tbl_CriticalityLevel ('int_idCriticalityLevel','str_Description') 
            VALUES (0,'Not Defined'),
            (1,'low'),
            (2,'medium'),
            (3,'high')
            """)
            
            # Lectura Json
            json_datas = open(JSON_PATH)
            data = json.load(json_datas)
    
            for field_dict in data['db_list']:
                #para insertar las BD
                #para resolver el problema de los nombres de bases de datos repetidos ON CONFLICT
                fmt_strDB = "insert into %s (str_name_db) values('%s')"
                #fmt_strDB = "insert into %s (int_iddb,str_name_db) values(%s, '%s') ON CONFLICT(str_name_db) DO UPDATE SET int_iddb = int_iddb"
                target_sql = fmt_strDB % ('tbl_db',str(field_dict['dn_name']))
                cursor.execute(target_sql)

            conexion.commit()
            ProcesarJson()
            conexion.close()
            
            return 1
        else:
            return 2
    except BaseException as err:
        try:
            
            if "UNIQUE constraint failed: tbl_dbUserManager.str_userID" in str(err.args[0]):
                print("Asegurese que el archivo: " + str(CSV_PATH) + " ,en la columna: str_userID no contenga valores repetidos ya que esta es una Llave Primaria")
            else:
                print(err.args[0])
                registrar_errores_proceso("crearBDAux",err.args[0] ,FechaSistema)
            cnn.close()
            return -1
        except BaseException as err:   
             return -1

#Procesar archivo JSON
def ProcesarJson():
    try:
        conexion = sqlite3.connect(DB_PATH)
        cursor   = conexion.cursor()
        # Lectura Json
        json_datas = open(JSON_PATH)
        data = json.load(json_datas)
        
        for field_dict in data['db_list']:
    
            #para insertar los owners
            fmt_strOwner = "insert into %s (str_name,str_uid,str_email) values('%s', '%s', '%s') ON CONFLICT(str_uid) DO UPDATE SET str_uid = str_uid"
            #para resolver el problema de el Email por fuera del padre Owner
            if not 'email' in field_dict['owner']:
                email = field_dict['email']
                #resolver problema email vacio
                if len(email) == 0:
                    emailsplit = str(field_dict['owner']['name']).lower().split()
                    email = emailsplit[0] + '.' + emailsplit[1] + '@mercadolibre.com'
            else: 
                email = str(field_dict['owner']['email'])
            target_sql = fmt_strOwner % ('tbl_dbOwner',str(field_dict['owner']['name']),str(field_dict['owner']['uid']),str(email))
            cursor.execute(target_sql)
            conexion.commit()
        
            #inicializar variable de envio de correo
            var_envio = 0
            #para insertar los Attributes
            int_iddb = ConsultarCodigoBD(field_dict['dn_name'])
            int_idOwner = str(field_dict['owner']['uid'])
            int_idConfidentiality_CriticalityLevel = tipoCriticidad(field_dict['classification']['confidentiality'])
            int_integrity_CriticalityLevel = tipoCriticidad(field_dict['classification']['integrity'])
            int_availability_CriticalityLevel = tipoCriticidad(field_dict['classification']['availability'])
            int_Clasification = calcularCriticidad(int(int_idConfidentiality_CriticalityLevel),int(int_integrity_CriticalityLevel),int(int_availability_CriticalityLevel))
            #si es criticidad alta enviar correo
            if int_Clasification == 3:
                strManagerMail = ConsultarOwnerMail(int_idOwner)
                var_envio = enviarCorreo(field_dict['dn_name'],strManagerMail)
            Date_UpdateStatus = field_dict['time_stamp']
            bit_sendMail = var_envio
            fmt_strAtri = "insert into %s (int_iddb,int_idOwner,int_idConfidentiality_CriticalityLevel,int_integrity_CriticalityLevel,int_availability_CriticalityLevel,int_Clasification,bit_sendMail,Date_UpdateStatus) values(%s, '%s',%s, %s, %s, %s,%s, '%s') ON CONFLICT(int_iddb) DO UPDATE SET int_iddb = int_iddb"
            target_sql = fmt_strAtri % ('tbl_dbAttributes',str(int_iddb),str(int_idOwner),str(int_idConfidentiality_CriticalityLevel),str(int_integrity_CriticalityLevel),str(int_availability_CriticalityLevel),str(int_Clasification),str(bit_sendMail),str(Date_UpdateStatus))
            cursor.execute(target_sql)
    except BaseException as err:   
        return -1

#Enviar correo electronico
def enviarCorreo(bd,to):
    try:
        # create message object instance
        msg = MIMEMultipart()
        message = "Por favor aprueba que esta criticidad es la correspondiente a la BD del tipo Alta"
        # setup the parameters of the message
        password = "12345mercadolibre"
        msg['From'] = "mercadolibretest12131@gmail.com"
        msg['To'] = to
        msg['Subject'] = "Aprobar Criticidad BD: " + bd
        # add in the message body
        msg.attach(MIMEText(message, 'plain'))
        #create server
        server = smtplib.SMTP('smtp.gmail.com: 587')
        server.starttls()
        # Login Credentials for sending the mail
        server.login(msg['From'], password)
        # send the message via the server.
        server.sendmail(msg['From'], msg['To'], msg.as_string())
        server.quit()
        return 1
    except BaseException as err:   
        return 1
 
#definir tipo de criticidad
def tipoCriticidad(Criticidad):
 
    if ''  == Criticidad: 
            return '0'
    elif 'low'  == Criticidad: 
            return '1'
    elif 'medium'  == Criticidad: 
            return '2'
    elif 'high' == Criticidad: 
            return '3'
    return 'SIN_IDENTIFICAR'  

#calcularCriticidad
def calcularCriticidad(Confidentiality,integrity,availability):
    total = Confidentiality + integrity + availability
    if total <= 3:
        return 1 #bajo
    elif total >3 and total <=6:
        return 2 #medio
    elif total >6:
        return 3 #alto}
    
#ConsultarOwnerMail
def ConsultarOwnerMail(strUID):
    try:
        cur = cnn.cursor() 
        sql = '''SELECT str_userManager FROM tbl_dbUserManager 
        WHERE  str_userID= '{}' '''.format(strUID)
        cur.execute(sql)
        datos = cur.fetchall()
        cur.close()
        return datos[0][0]
    except BaseException as err:
        if  "MySQL Connection not available" in str(err.args[0]):
            registrar_errores_proceso("ConsultarOwnerMail",err.args[0] ,FechaSistema)#Este si atrapa bien el mensaje de error
        else:
            registrar_errores_proceso("ConsultarOwnerMail",err.args[0] ,FechaSistema)#Este si atrapa bien el mensaje de error
        cur.close()
        raise
    
#ConsultarCodigoBD
def ConsultarCodigoBD(bdName):
    try:      
        cur = cnn.cursor() 
        sql = '''SELECT int_iddb FROM tbl_db 
        WHERE  str_name_db= '{}' '''.format(bdName)
        cur.execute(sql)
        datos = cur.fetchall()
        cur.close()
        return datos[0][0]
    except BaseException as err:
        if  "MySQL Connection not available" in str(err.args[0]):
            registrar_errores_proceso("ConsultarCodigoBD",err.args[0] ,FechaSistema)#Este si atrapa bien el mensaje de error
        else:
            registrar_errores_proceso("ConsultarCodigoBD",err.args[0] ,FechaSistema)#Este si atrapa bien el mensaje de error
        cur.close()
        raise


#Metodo principal de procesamiento de datos
res = crearBDAux()
if res == 1:
    print("La Base de datos:  BDListUserManager.db se encuentra lista en la ruta: " + str(DB_PATH))
elif res == 2:
    print("Ya existe una bd creada con datos, si deseas comenzar el proceso de nuevo elimina el archivo: BDListUserManager.db")
elif res == -1:
    print("Error al procesar los datos")
