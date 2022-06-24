import psycopg2
from psycopg2 import sql
import random, time, datetime
from multiprocessing import Queue


def data_replication(connection, count, q):
    while True:
        start = q.get()
        print("-----------------------------\nНачата репликация данных")
        # Начало транзакции
        cur = connection.cursor()
        
        # Блокируем таблицы, чтобы программа ИРС не смогла внести изменения во время работы
        cur.execute("LOCK TABLE t1, t2, t3, log IN EXCLUSIVE MODE")

        # Получаем последние выполненные перед запуском РД изменения из журнала
        query = sql.SQL("SELECT * FROM log LIMIT %s OFFSET (SELECT COUNT(*) FROM log)-%s")
        cur.execute(query, [str(count), str(count)])
        log = cur.fetchall()
        
        #for l in log:
        #    print(l)
        #print("---------")
        
        # Находим наиболее поздними манипуляции одной и той же записи внутри одной условной БД,
        without = []
        null_null = []
        for i, ri in enumerate(log):
            if i != count-1:
                latest = True
                for j, rj in enumerate(log[i+1:]):
                   
                    if ri[2] == rj[2] and ri[3] == rj[3]:
                        latest = False
                        if ri[4] == rj[5] == None:
                            null_null.append(j+i+1)

                if latest and i not in null_null:
                    without.append(ri)
            else:
                if i not in null_null:
                    without.append(ri)

        #for w in without:
        #    print(w)
        #print("---------")
        
        # Отсеиваем коллизии
        changes = []
        cllsn_to_delete = []
        for i, ri in enumerate(without):
            if i != count-1:
                more_prior = True
                for j, rj in enumerate(without[i+1:]):
                    if ri[3] == rj[3]:
                        if ri[2] > rj[2]:
                            more_prior = False
                        else:
                            cllsn_to_delete.append(j+i+1)                                                                                                                                                                                                                                          
                if more_prior and i not in cllsn_to_delete:
                    changes.append(ri)
            else:
                if i not in cllsn_to_delete:
                    changes.append(ri)
                    
        #for ch in changes:
        #    print(ch)
        
        # Проводим тиражирование каждого изменения по порядку и проверяем коллизии
        for _, ch in enumerate(changes):
            if ch[5] != None:
                instr_name, instr_family = ch[5].split(', ')
                if ch[4] == None:
                    operation = "INSERT" 
                else:
                    operation = "UPDATE" 
            else:
                operation = "DELETE"
            
            bases = ["t1", "t2", "t3"]
            
            dbfrom = "t"+ch[2][2]
            if operation == "INSERT":
                for db in bases:
                    if db != dbfrom or db == dbfrom:  # Может, потом уберу такую дичь, но пока для тесту вот так
                        query = sql.SQL("SELECT * FROM {table} WHERE id=%s").format(
                            table = sql.Identifier(db)
                        )
                        cur.execute(query,   [str(ch[3])])
                        res = cur.fetchall()
                        if len(res) != 0:
                            #print("["+str(db)+", "+str(ch[3])+"] Тиражирование добавления, запись в БД есть")
                            query = sql.SQL("DELETE FROM {table} WHERE id=%s").format(
                                table = sql.Identifier(db)
                            )
                            cur.execute(query, [str(ch[3])])
                            
                            query = sql.SQL("INSERT INTO {table} (id, datetime, operation, instr_name, instr_family) VALUES (%s, NOW(), %s, %s, %s)").format(
                                table = sql.Identifier(db)
                            )
                            cur.execute(query, (str(ch[3]), "Добавление в БД"+ch[2][2], instr_name, instr_family))
                            
                        else:
                            #print("["+str(db)+", "+str(ch[3])+"] Тиражирование добавления, записи в БД нет")
                            query = sql.SQL("INSERT INTO {table} (id, datetime, operation, instr_name, instr_family) VALUES (%s, NOW(), %s, %s, %s)").format(
                                table = sql.Identifier(db)
                            )
                            cur.execute(query, (str(ch[3]), "Добавление в БД"+ch[2][2], instr_name, instr_family))
            
            elif operation == "UPDATE":
                for db in bases:
                    if db != dbfrom:
                        query = sql.SQL("SELECT * FROM {table} WHERE id=%s").format(
                            table = sql.Identifier(db)
                        )
                        cur.execute(query,   [str(ch[3])])
                        res = cur.fetchall()
                        if len(res) == 0:
                        
                            query = sql.SQL("INSERT INTO {table} (id, datetime, operation, instr_name, instr_family) VALUES (%s, NOW(), %s, %s, %s)").format(
                                table = sql.Identifier(db)
                            )
                            cur.execute(query, (str(ch[3]), "Обновление в БД"+ch[2][2], instr_name, instr_family))
                        else:
                            query = sql.SQL("UPDATE {table} SET (datetime, operation, instr_name, instr_family) = (NOW(), %s, %s, %s) WHERE id=%s").format(
                                table = sql.Identifier(db)
                            )
                            cur.execute(query, ["Обновление в БД"+ch[2][2], instr_name, instr_family, str(ch[3])])
                        
                        
            elif operation == "DELETE":
                for db in bases:
                    if db != dbfrom:                    
                        query = sql.SQL("DELETE FROM {table} WHERE id=%s").format(
                            table = sql.Identifier(db)
                        )
                        cur.execute(query, [str(ch[3])])
        

        log = open('data.log', 'a')

        now = datetime.datetime.now()
        log.write("----------------- "+str(now)+" -----------------\n")
        log.write("Выполнена репликация данных\n")
  
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
    
        # Конец транзакции
        print("Репликация завершена\n-----------------------------")
        connection.commit()
        

