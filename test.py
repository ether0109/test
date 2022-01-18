import os
import pymysql
import pandas as pd
import math
import time

# 创建database
def createsql(f):
        # 获取数据框的标题行,作为sql语句中的字段名称。
        columns = f.columns.tolist()
        #print(columns)
        # 将csv文件中的字段类型转换成mysql中的字段类型
        types = f.dtypes
        #print(types)
        field = []  # 用来接收字段名称的列表
        table = []  # 用来接收字段名称和字段类型的列表
        for item in columns:
            if 'int' in str(types[item]):
                char = item + ' INT'
            elif 'float' in str(types[item]):
                char = item + ' FLOAT'
            elif 'object' in str(types[item]):
                char = item + ' TEXT'
            elif 'datetime' in str(types[item]):
                char = item + ' DATETIME'
            else:
                char = item + ' TEXT'
            table.append(char)
            field.append(item)
# 构建SQL语句片段
        # 将table列表中的元素用逗号连接起来，组成table_sql语句中的字段名称和字段类型片段，用来创建表。
        tables = ','.join(table)
        #print(tables)

        # 将field列表中的元素用逗号连接起来，组成insert_sql语句中的字段名称片段，用来插入数据。
        fields = ','.join(field)  # 字段名
        #print(fields)
# 创建数据库表
        # 如果数据库表已经存在，首先删除它
        cur.execute('drop table if exists {};'.format(filename))
        conn.commit()

# 构建创建表的SQL语句
        table_sql = 'CREATE TABLE IF NOT EXISTS ' + filename + '(' + 'id0 int PRIMARY KEY NOT NULL auto_increment,' + tables + ');'
        # print('table_sql is: ' + table_sql)

# 开始创建数据库表
        print('表:' + filename + ',开始创建…………')
        cur.execute(table_sql)
        conn.commit()
        print('表:' + filename + ',创建成功!')
# 向数据库表中插入数据
        print('表:' + filename + ',开始插入数据…………')

# 将数据框的数据读入列表。每行数据是一个列表，所有数据组成一个大列表。也就是列表中的列表，将来可以批量插入数据库表中。
        values = f.values.tolist()  # 所有的数据
        # print(values)

# 计算数据框中总共有多少个字段，每个字段用一个 %s 替代。
        s = ','.join(['%s' for _ in range(len(f.columns))])
        # print(s)

# 构建插入数据的SQL语句
        insert_sql = 'insert into {}({}) values({})'.format(filename, fields, s)
        # print('insert_sql is:' + insert_sql)

# 开始插入数据
        time_start = time.time()  # 使用time计时
        cur.executemany(insert_sql, values)  # 使用 executemany批量插入数据
        conn.commit()
        time_end = time.time()
        print('表:' + filename + ',数据插入完成！'+ '用时：'+ str(time_end-time_start) + 's')
        print(' ')


# 连接 Mysql 数据库
try:
    conn = pymysql.connect(
                            host='localhost',
                            user='root',
                            password='bjt200219?',
                            db='testnight',
                            charset='utf8'
    )
    cur = conn.cursor()
    print('数据库已连接！')
except:
    print('数据库连接失败！')


# 获取程序所在路径及该路径下所有文件名称
path = 'C:/Users/毕嘉栋/Desktop/test_cfps'
files = os.listdir(path)
#print(files)

# 遍历所有文件
for file in files:
    # 判断文件是不是csv文件
    if file.split('.')[-1] in ['csv']:

        # 使用pandas库读取csv文件的所有内容,结果f是一个数据框，保留了表格的数据存储方式，是pandas的数据存储结构。
        f = pd.read_csv(file, encoding="gbk", low_memory=False)
        # print(f)

# 切割数据
        # 读取主键
        keyname = f.columns[0]
        key = f.iloc[:,[0]]
        # print(f.iloc[:,[0]])
        columns_num = len(f.columns)
        # print(columns_num)
        if columns_num >= 200:
            flag = math.ceil(columns_num/200)
            for j in range(flag):
                f1 = f.iloc[:,(j*200):((j+1)*200)]
                print(f1)
                # 构建一个表名称，供后期SQL语句调用
                filename = file.split('.')[0]
                filename = 'tab_' + filename + '_'+str(j+1)
                createsql(f1)


cur.close()  # 关闭游标
conn.close()  # 关闭数据库连接
print('数据库录入完成！')