import os,sys,re
import psycopg2
import sqlite3
from getopt import getopt

class SqliteTable(object):
	def __init__(self, conn, name):
		self.conn = conn
		self.name = name		
	
	def multi_insert(self, cur, rows):
		q = ",".join(['?']*len(rows[0]))
		query = "INSERT INTO %s VALUES(%s)" % (self.name, q)
		cur.executemany(query, rows)
	def insert(self, data):
		rows = []
		cur = self.conn.cursor() 
		for row in data:
			rows.append(row)
			if(len(rows) == 1024):
				self.multi_insert(cur, rows)
				rows = []
				sys.stdout.write(".")
		if(len(rows) != 0):
			sys.stdout.write(".")
			self.multi_insert(cur, rows)
		print ""
	
class SqliteDB(object):
	def __init__(self, dbfile):
		self.conn = sqlite3.connect(dbfile)
	def create(self, name, sql):
		cur = self.conn.cursor()
		cur.execute(sql)
		return SqliteTable(self.conn, name)

class PostgresTable(object):
	def __init__(self, conn, name):
		self.name = name
		self.conn = conn
		self.read_desc()
	def read_desc(self):
		cur = self.conn.cursor()
		cur.execute("""
			select column_name as column,is_nullable as allow_null,data_type as type FROM information_schema.columns WHERE table_name=%(table)s order by ordinal_position
		""", {"table": self.name}
		)
		# name, nullable, type
		self.cols = [(rec[0], rec[1] == "YES", rec[2]) for rec in cur]
	def create_sql(self):
		allownull = lambda(t) : (t and " ") or "not null"
		col = lambda (x): "%s %s %s" % (x[0], x[2], allownull(x[1]))
		schema = " , ".join([col(c) for c in self.cols])
		return "CREATE TABLE IF NOT EXISTS %s ( %s ); " % (self.name, schema)
	def __repr__(self):
		return self.create_sql()
	def data(self):
		cur = self.conn.cursor()
		cur.execute("select * from %s" % self.name)
		return cur
	
class PostgresDB(object):
	def __init__(self, conn_string):
		self.conn = psycopg2.connect(conn_string)
		self.tables = self.read_tables()
	def read_tables(self):
		cur = self.conn.cursor()
		cur.execute("""
			SELECT table_name FROM information_schema.tables where table_schema = 'public';
		""")
		return [PostgresTable(self.conn, table_name) for (table_name,) in cur]
	
def main(args):
	options = "vc:o:"
	(args,opts) = getopt(args, options)	
	conn_string = "dbname=hdp2"
	out_file = "out.db"
	for k,v in args:
		if (k == "-c"):
			conn_string = v
		elif (k == "-o"):
			out_file = v
	pgdb = PostgresDB(conn_string)
	litedb = SqliteDB(out_file)
	for table in pgdb.tables:
		print "Creating %s" % (table.name),
		t = litedb.create(table.name, table.create_sql())
		t.insert(table.data())

main(sys.argv[1:])
