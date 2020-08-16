import pymysql
import os
import zipfile
from datetime import datetime

connection = pymysql.connect(host='localhost',
                             user='root',
                             password='Hs8Tw13kPx7uDPs',
                             db='bdsia',
                             charset='latin1',
                             cursorclass=pymysql.cursors.DictCursor)
programdatapath = 'C:/Programdata/Backup_Auto/'
path = programdatapath + 'tempback/'


def create_backup_paths():
    if not os.path.isdir(programdatapath):
        os.mkdir(programdatapath)
    if not os.path.isdir(path):
        os.mkdir(path)


def insert_empty_line():
    connection.cursor().execute('INSERT INTO `backup_auto` (`text_lines`) VALUES ("")')
    connection.commit()


def get_tables_names():
    with connection.cursor() as cursor:
        # Get list of tables
        cursor.execute('SELECT table_name FROM information_schema.`TABLES` WHERE table_schema = "bdsia" and '
                       'table_name <> "backup_auto"')
        return cursor.fetchall()


def create_sql_file():
    filepath = os.path.join(path, 'backup_auto.sql')
    if os.path.isfile(filepath):
        os.remove(filepath)
    with connection.cursor() as cursor:
        # Create backup_auto table ignoring warnings
        cursor.execute('SET sql_notes = 0')
        connection.commit()
        cursor.execute('CREATE TABLE IF NOT EXISTS `backup_auto` (`text_lines` BLOB)')
        connection.commit()
        cursor.execute('SET sql_notes = 1')
        connection.commit()
        # Empties backup_auto table
        cursor.execute('DELETE FROM `backup_auto`')
        connection.commit()
        # Inserts first default lines
        cursor.execute('INSERT INTO `backup_auto` (`text_lines`) VALUES ("set foreign_key_checks=0;")')
        insert_empty_line()
        cursor.execute('INSERT INTO `backup_auto` (`text_lines`) VALUES ("create database bdsia;")')
        insert_empty_line()
        cursor.execute('INSERT INTO `backup_auto` (`text_lines`) VALUES ("use bdsia;")')
        insert_empty_line()
        # Get list of procedures
        sql = 'SHOW PROCEDURE STATUS'
        cursor.execute(sql)
        procedureslist = cursor.fetchall()
        # Get list of procedure's create command
        procedurescreate = []
        sql = 'SHOW CREATE PROCEDURE '
        for i in range(0, len(procedureslist)):
            cursor.execute(sql + procedureslist[i]['Name'])
            procedurescreate.append(cursor.fetchone())
        # Inserts Procedures lines
        sql = 'INSERT INTO `backup_auto` (`text_lines`) VALUES ("'
        for i in range(0, len(procedurescreate)):
            cursor.execute('INSERT INTO `backup_auto` (`text_lines`) VALUES ("DELIMITER //")')
            cursor.execute(sql + str(procedurescreate[i]['Create Procedure']).replace('"', '\\"') + ' //")')
            connection.commit()
            cursor.execute('INSERT INTO `backup_auto` (`text_lines`) VALUES ("DELIMITER ;")')
            insert_empty_line()
        # Get list of table's create command
        tablescreate = []
        sql = 'SHOW CREATE TABLE '
        for i in range(0, len(get_tables_names())):
            cursor.execute(sql + get_tables_names()[i]['table_name'])
            tablescreate.append(cursor.fetchone())
        # Inserts Tables lines
        sql = 'INSERT INTO `backup_auto` (`text_lines`) VALUES ("'
        for i in range(0, len(tablescreate)):
            cursor.execute(sql + str(tablescreate[i]['Create Table']).replace('"', '\\"').replace('\\r\\n',
                                                                                            '\\\\r\\\\n') + ';")')
            connection.commit()
            insert_empty_line()
            tablename = str(tablescreate[i]['Table'])
            tablebackuppath = 'c:/tempback/' + tablename
            terminate = ';'
            cursor.execute(sql + 'load data infile \\"' + tablebackuppath + '\\" into table ' + tablename +
                                                                'fields terminated by \\"' + terminate + '\\";")')
            connection.commit()
            insert_empty_line()
            insert_empty_line()
        # Get list of triggers
        sql = 'SHOW TRIGGERS'
        cursor.execute(sql)
        procedureslist = cursor.fetchall()
        # Get list of triggers's create command
        procedurescreate = []
        sql = 'SHOW CREATE TRIGGER '
        for i in range(0, len(procedureslist)):
            cursor.execute(sql + procedureslist[i]['Trigger'])
            procedurescreate.append(cursor.fetchone())
        # Inserts triggers lines
        sql = 'INSERT INTO `backup_auto` (`text_lines`) VALUES ("'
        for i in range(0, len(procedurescreate)):
            cursor.execute('INSERT INTO `backup_auto` (`text_lines`) VALUES ("DELIMITER //")')
            cursor.execute(sql + str(procedurescreate[i]['SQL Original Statement']).replace('"', '\\"') + ' //")')
            connection.commit()
            cursor.execute('INSERT INTO `backup_auto` (`text_lines`) VALUES ("DELIMITER ;")')
            insert_empty_line()

        # Creates .Sql file
        cursor.execute('SELECT * from backup_auto INTO OUTFILE '
                       '%(path)s fields terminated BY ",'
                       '" OPTIONALLY ENCLOSED BY "" escaped BY "" LINES TERMINATED BY "\\n"', {'path': filepath})


def create_tables_files():
    with connection.cursor() as cursor:
        # Backups each table to a file
        for i in range(0, len(get_tables_names())):
            tablename = get_tables_names()[i]['table_name']
            tablefilepath = os.path.join(path, tablename)
            if os.path.isfile(tablefilepath):
                os.remove(tablefilepath)
            # Backups each table to a file
            cursor.execute('select * from ' + tablename + ' into outfile %(path)s fields terminated by ";"',
                           {'path': os.path.join(path, tablename)})



def compact_backup():
    now = datetime.now().strftime("%d%m%Y_%H%M%S")
    zipf = zipfile.ZipFile(programdatapath + 'backup_auto_' + now + '.zip', 'w', zipfile.ZIP_DEFLATED, compresslevel=1)
    for root, dirs, files in os.walk(path):
        for file in files:
            zipf.write(os.path.join(path, file))

create_backup_paths()
#create_sql_file()
#create_tables_files()
compact_backup()


connection.close()