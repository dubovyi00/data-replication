import psycopg2
from psycopg2 import sql
import random, time
from multiprocessing import Queue


def system_work_imitation(connection, count, time_between, q):
    # Определим возможные данные для внесения в БД
    possible_data = [
        ('Аккордеон', 'Клавишные язычковые'),
        ('Баян', 'Клавищные язычковые'),
        ('Треугольник', 'Ударные идиофоны'),
        ('Электрогитара', 'Струнные щипковые ЭМИ'),
        ('Электроскрипка', 'Струнные смычковые ЭМИ'),
        ('Меллотрон', 'Клавишные ЭМИ'),
    ]
    
    
    dbs = ('t1', 't2', 't3')
    acts = ('INSERT', 'UPDATE', 'DELETE')
    
    while True:
    	for i in range(count):
    	    # Рандомим базу и действие 
    	    database = dbs[random.randint(0, 2)]
    	    action =  acts[random.randint(0, 2)]
    	    #print(database)
    	    #print(action)
    	    
    	    # Начало транзакции и получение курсора
    	    cur = connection.cursor()
    	    
    	    # Выполняем действие внутри базы
    	    if action == 'INSERT':
    	        inserting = possible_data[random.randint(0, 5)]
    	        
    	        # Получаем данные об идентификаторах в данной БД и считаем их количество
    	        query = sql.SQL("SELECT MAX(id) FROM {table}").format(
    	            table = sql.Identifier(database)
    	        )
    	        cur.execute(query)
    	        
    	        
    	        try:
    	            id_of = cur.fetchone()[0]	
    	            #Добавляем данные в БД и помечаем это изменение в журнале
    	            query = sql.SQL("INSERT INTO {table} (id, datetime, operation, instr_name, instr_family) VALUES (%s, NOW(), %s, %s, %s) RETURNING id").format(
    	                table = sql.Identifier(database)
    	            )
    	            cur.execute(query, (str(id_of+1), "Добавление в БД"+database[1], inserting[0], inserting[1]))
    	            id_of = cur.fetchone()[0] 
    	        except psycopg2.errors.LockNotAvailable:
    	            print("Всё ещё выполняется репликация, данное изменение не будет отражено на данных в БД и записано в журнал")
    	            connection.commit()
    	            break
    	        
    	        query = sql.SQL("INSERT INTO log (datetime, database, data_id, old_data, new_data) VALUES (NOW(), %s, %s, NULL, %s)") 
    	        cur.execute(query, ["БД"+database[1], str(id_of), inserting[0]+", "+inserting[1]])          
    	        
    	    elif action == 'UPDATE':
    	        # Рандомим данные для обновления
    	        updating = possible_data[random.randint(0, 5)]
    	        
    	        # Получаем минимальный id
    	        query = sql.SQL("SELECT MIN(id) FROM {table}").format(
    	            table = sql.Identifier(database)
    	        )
    	        cur.execute(query)
    	        
    	        try:
    	            id_of = cur.fetchone()[0]
    	            
    	            # Запоминаем старые данные
    	            query = sql.SQL("SELECT instr_name, instr_family FROM {table} WHERE id=%s").format(
    	                table = sql.Identifier(database)
    	            )
    	            cur.execute(query, [str(id_of)])
    	            old_data = cur.fetchone()
    	        
    	        
    	            #Обновляем данные в БД и помечаем это изменение в журнале
    	            query = sql.SQL("UPDATE {table} SET (datetime, operation, instr_name, instr_family) = (NOW(), %s, %s, %s) WHERE id=%s").format(
    	                table = sql.Identifier(database)
    	            )
    	            cur.execute(query, ["Обновление в БД"+database[1], updating[0], updating[1], str(id_of)])
    	        except psycopg2.errors.LockNotAvailable:
    	            print("Всё ещё выполняется репликация, данное изменение не будет отражено на данных в БД и записано в журнал")
    	            connection.commit()
    	            break
    	        
    	        
    	        query = sql.SQL("INSERT INTO log (datetime, database, data_id, old_data, new_data) VALUES (NOW(), %s, %s, %s, %s)")
    	        cur.execute(query, ["БД"+database[1], str(id_of), old_data[0]+", "+old_data[1], updating[0]+", "+updating[1]])    
		        
    	    elif action == 'DELETE':
    	        # Получаем данные об идентификаторах в данной БД и считаем их количество
    	        query = sql.SQL("SELECT MAX(id) FROM {table}").format(
    	            table = sql.Identifier(database)
    	        )
    	        cur.execute(query)
    	        
    	        
    	        try:
    	            id_of = cur.fetchone()[0]
    	            
    	            # Запоминаем старые данные
    	            query = sql.SQL("SELECT instr_name, instr_family FROM {table} WHERE id=%s").format(
    	                table = sql.Identifier(database)
    	            )
    	            cur.execute(query, [str(id_of)])
    	            old_data = cur.fetchone()
    	            
    	            #Удаляем данные из БД и помечаем это изменение в журнале
    	            query = sql.SQL("DELETE FROM {table} WHERE id=%s").format(
    	                table = sql.Identifier(database)
    	            )
    	            cur.execute(query, [str(id_of)])
    	        except psycopg2.errors.LockNotAvailable:
    	            print("Всё ещё выполняется репликация, данное изменение не будет отражено на данных в БД и записано в журнал")
    	            connection.commit()
    	            break
    	        
    	        
    	        query = sql.SQL("INSERT INTO log (datetime, database, data_id, old_data, new_data) VALUES (NOW(), %s, %s, %s, NULL)")
    	        cur.execute(query, ["БД"+database[1], str(id_of), old_data[0]+", "+old_data[1]])    
		    
    	    # Конец транзакции
    	    connection.commit()
    	    print("Выполнена транзакция")
    	    if i == count-1:
    	        q.put(True)
    	    time.sleep(time_between)
    	
