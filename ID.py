import psycopg2, sys, datetime
from psycopg2 import sql

def ID(login, password):
    try:
        connection = psycopg2.connect(dbname='students', 
                                      user=login, 
                                      password=password, 
                                      host='students.ami.nstu.ru', 
                                      options='-c search_path=pmib8508,public')
    except Exception:
        sys.exit("Не удалось соединиться с базой данных")
		
    cur = connection.cursor()
	
    cur.execute("TRUNCATE t1, t2, t3, log RESTART IDENTITY")
	
    cur.execute('''
	INSERT INTO t1 (datetime, operation, instr_name, instr_family) 
	VALUES 
	(NOW(), 'Начальная вставка', 'Виолончель', 'Струнные смычковые'), 
	(NOW(), 'Начальная вставка', 'Контрабас', 'Струнные смычковые'), 
	(NOW(), 'Начальная вставка', 'Октобас', 'Струнные смычковые'), 
	(NOW(), 'Начальная вставка', 'Труба', 'Медные духовые'), 
	(NOW(), 'Начальная вставка', 'Геликон', 'Медные духовые'), 
	(NOW(), 'Начальная вставка', 'Туба', 'Медные духовые'), 
	(NOW(), 'Начальная вставка', 'Терменвокс', 'Клавишные ЭМИ'), 
	(NOW(), 'Начальная вставка', 'Arturia Microbrute', 'Клавишные ЭМИ'), 
	(NOW(), 'Начальная вставка', 'Поливокс', 'Клавишные ЭМИ'), 
	(NOW(), 'Начальная вставка', 'Пиколлогитара', 'Струнные щипковые')

	''')
	
    cur.execute('''
	INSERT INTO t2 (datetime, operation, instr_name, instr_family) 
	VALUES 
	(NOW(), 'Начальная вставка', 'Виолончель', 'Струнные смычковые'), 
	(NOW(), 'Начальная вставка', 'Контрабас', 'Струнные смычковые'), 
	(NOW(), 'Начальная вставка', 'Октобас', 'Струнные смычковые'), 
	(NOW(), 'Начальная вставка', 'Труба', 'Медные духовые'), 
	(NOW(), 'Начальная вставка', 'Геликон', 'Медные духовые'), 
	(NOW(), 'Начальная вставка', 'Туба', 'Медные духовые'), 
	(NOW(), 'Начальная вставка', 'Терменвокс', 'Клавишные ЭМИ'), 
	(NOW(), 'Начальная вставка', 'Arturia Microbrute', 'Клавишные ЭМИ'), 
	(NOW(), 'Начальная вставка', 'Поливокс', 'Клавишные ЭМИ'), 
	(NOW(), 'Начальная вставка', 'Пиколлогитара', 'Струнные щипковые')

	''')
	
    cur.execute('''
	INSERT INTO t3 (datetime, operation, instr_name, instr_family) 
	VALUES 
	(NOW(), 'Начальная вставка', 'Виолончель', 'Струнные смычковые'), 
	(NOW(), 'Начальная вставка', 'Контрабас', 'Струнные смычковые'), 
	(NOW(), 'Начальная вставка', 'Октобас', 'Струнные смычковые'), 
	(NOW(), 'Начальная вставка', 'Труба', 'Медные духовые'), 
	(NOW(), 'Начальная вставка', 'Геликон', 'Медные духовые'), 
	(NOW(), 'Начальная вставка', 'Туба', 'Медные духовые'), 
	(NOW(), 'Начальная вставка', 'Терменвокс', 'Клавишные ЭМИ'), 
	(NOW(), 'Начальная вставка', 'Arturia Microbrute', 'Клавишные ЭМИ'), 
	(NOW(), 'Начальная вставка', 'Поливокс', 'Клавишные ЭМИ'), 
	(NOW(), 'Начальная вставка', 'Пиколлогитара', 'Струнные щипковые')

	''')
    
    log = open('data.log', 'a')
    

    now = datetime.datetime.now()
    log.write("----------------- "+str(now)+" -----------------\n")
    log.write("Выполнена инициализация данных\n")

    
    bases = ['t1', 't2', 't3']
    
    for db in bases:
        log.write("\nСодержимое БД"+str(db[1])+"\n")
        query = sql.SQL("SELECT * FROM {table}").format(
            table = sql.Identifier(db)
        )
        cur.execute(query)
        res = cur.fetchall()
        for r in res:
            log.write(str(r[0])+", "+str(r[1])+", "+r[2]+", "+r[3]+", "+r[4]+"\n")
        
    log.write("\nКонец записи\n\n")
    
    log.close()
    
    connection.commit()
		
if __name__ == '__main__':
    login = input("login: ")
    password = input("password: ")
    ID(login, password)
    print("Инициализация данных завершена")
