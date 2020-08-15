import pymysql
import os

connection = pymysql.connect(host='localhost',
                             user='root',
                             password='Hs8Tw13kPx7uDPs',
                             db='bdsia',
                             charset='latin1',
                             cursorclass=pymysql.cursors.DictCursor)


def insert_empty_line():
    cursor.execute('INSERT INTO `backup_auto` (`text_lines`) VALUES ("")')
    connection.commit()


path = 'C:/Users/Fabio/Documents/Flynet/Backup_Auto/'
filepath = os.path.join(path, 'backup_auto.sql')
if os.path.isfile(filepath):
    os.remove(filepath)

try:
    with connection.cursor() as cursor:
        # Empties backup_auto table
        sql = 'DELETE FROM `backup_auto`'
        cursor.execute(sql)
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
        for i in range(1, len(procedureslist)):
            cursor.execute(sql + procedureslist[i - 1]['Name'])
            procedurescreate.append(cursor.fetchone())
        # Inserts Procedures lines
        sql = 'INSERT INTO `backup_auto` (`text_lines`) VALUES ("'
        for i in range(1, len(procedurescreate)):
            cursor.execute('INSERT INTO `backup_auto` (`text_lines`) VALUES ("DELIMITER //")')
            cursor.execute(sql + str(procedurescreate[i - 1]['Create Procedure']).replace('"', '\\"') + ' //")')
            connection.commit()
            cursor.execute('INSERT INTO `backup_auto` (`text_lines`) VALUES ("DELIMITER ;")')
            insert_empty_line()
        # Get list of tables
        sql = 'SELECT table_name FROM information_schema.`TABLES` WHERE table_schema = "bdsia"'
        cursor.execute(sql)
        tableslist = cursor.fetchall()
        # Get list of table's create command
        tablescreate = []
        sql = 'SHOW CREATE TABLE '
        for i in range(1, len(tableslist)):
            cursor.execute(sql + tableslist[i - 1]['table_name'])
            tablescreate.append(cursor.fetchone())
        # Inserts Tables lines
        sql = 'INSERT INTO `backup_auto` (`text_lines`) VALUES ("'
        for i in range(1, len(tablescreate)):
            cursor.execute(sql + str(tablescreate[i - 1]['Create Table']).replace('"', '\\"') + ';")')
            connection.commit()
            insert_empty_line()
            tablename = str(tablescreate[i - 1]['Table'])
            tablebackuppath = 'c:/tempback/' + tablename
            terminate = ';'
            cursor.execute(sql + 'load data infile \\"' + tablebackuppath + '\\" into table ' + tablename + ' fields terminated by \\"' + terminate + '\\";")')
            connection.commit()
            insert_empty_line()
        # Get list of triggers
        insert_empty_line()
        sql = 'SHOW TRIGGERS'
        cursor.execute(sql)
        procedureslist = cursor.fetchall()
        # Get list of triggers's create command
        procedurescreate = []
        sql = 'SHOW CREATE TRIGGER '
        for i in range(1, len(procedureslist)):
            cursor.execute(sql + procedureslist[i - 1]['Trigger'])
            procedurescreate.append(cursor.fetchone())
        # Inserts triggers lines
        sql = 'INSERT INTO `backup_auto` (`text_lines`) VALUES ("'
        for i in range(1, len(procedurescreate)):
            cursor.execute('INSERT INTO `backup_auto` (`text_lines`) VALUES ("DELIMITER //")')
            cursor.execute(sql + str(procedurescreate[i - 1]['SQL Original Statement']).replace('"', '\\"') + ' //")')
            connection.commit()
            cursor.execute('INSERT INTO `backup_auto` (`text_lines`) VALUES ("DELIMITER ;")')
            insert_empty_line()

        # Creates .Sql file
        cursor.execute('SELECT * from backup_auto INTO OUTFILE '
                       '%(path)s fields terminated BY ",'
                       '" OPTIONALLY ENCLOSED BY "" escaped BY "" LINES TERMINATED BY "\\n"', {'path': filepath})
finally:
    connection.close()
