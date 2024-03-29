import unittest, os, sys
sys.path.insert(0, '../jsondbupload/')
from jsondbupload import JsonDBUpload

from zohavi.zdb.models import TBL_Default

from flask import Flask
from flask_sqlalchemy import SQLAlchemy, Model

from mclogger import MCLogger

#################################################################################
class Models_Book: 
	def __init__(self, db):
		class Author(db.Model, TBL_Default):
			__tablename__ = 'author' 
			id = db.Column(db.Integer(), primary_key=True)
			name = db.Column(db.Integer() )

		class Book(db.Model, TBL_Default):
			__tablename__ = 'book' 
			id = db.Column(db.Integer(), primary_key=True)
			name = db.Column(db.Integer() )
			author_id = db.Column( db.Integer()  , db.ForeignKey( 'author.id'  ) )
			_author = db.relationship("Author", backref=db.backref("author" ), lazy='joined')


		class Bookset(db.Model, TBL_Default):
			__tablename__ = 'bookset' 
			id = db.Column(db.Integer(), primary_key=True)
			name = db.Column(db.Integer() )
			
		class BooksetItems(db.Model, TBL_Default):
			__tablename__ = 'bookset_items' 
			id = db.Column(db.Integer(), primary_key=True)
			bookset_id = db.Column( db.Integer()  , db.ForeignKey( 'bookset.id'  ) )
			_bookset = db.relationship("Bookset", backref=db.backref("bookset" ), lazy='joined')
			book_id = db.Column( db.Integer()  , db.ForeignKey( 'book.id'  ) )
			_book = db.relationship("Book", backref=db.backref("book" ), lazy='joined')
		
		self.Author = Author
		self.Book = Book
		self.Bookset = Bookset
		self.BooksetItems = BooksetItems

		self.tables_dict = {table.__tablename__: table for table in db.Model.__subclasses__()}

class Models_Config: 
	def __init__(self, db):
		class TBL_Config(db.Model, TBL_Default):
			__tablename__ = 'config' 
			id = db.Column(db.Integer(), primary_key=True)
			area 		= db.Column(db.String(25) ) 
			category 	= db.Column(db.String(25) ) 
			sub_cat		= db.Column(db.String(25) ) 

		class TBL_ConfigItem(db.Model, TBL_Default):
			__tablename__ = 'config_item' 
			id = db.Column(db.Integer(), primary_key=True)
			config_id = db.Column(db.Integer(), db.ForeignKey( 'config.id'  ) )
			_config = db.relationship("TBL_Config", backref=db.backref("config" ), lazy='joined')
			env =  db.Column(db.String(10) ) 
			name = db.Column(db.String(100) ) 
			value = db.Column(db.String(255) ) 		

		self.TBL_Config = TBL_Config
		self.TBL_ConfigItem = TBL_ConfigItem

		self.tables_dict = {table.__tablename__: table for table in db.Model.__subclasses__()}

	#################################################################################
	def table_object(table_name):
		return self.tables_dict.get(table_name)

#################################################################################
class TestJsonDBUpload(unittest.TestCase):
	def _db_session(self):
		if os.path.exists('test.db'): os.remove('test.db')
		app = Flask(__name__)
		app.config['SQLALCHEMY_DATABASE_URI'] = 'sqlite:///test.db'
		app.config['SQLALCHEMY_TRACK_MODIFICATIONS'] = False
		db = SQLAlchemy(app)
		self._db_create_tables(db)
		return db
	
	#################################################################################
	def _db_create_tables(self, db):
		# db._models = {}
		db._models1 = Models_Book(db)
		db._models2 = Models_Config(db)
		db.create_all()

	#################################################################################
	def test_connect_to_db(self):
		db = self._db_session()
		self.assertEqual( db.session.is_active, True)

	#################################################################################
	def test_insert_recort(self):
		db = self._db_session()
		auth = db._models1.Author(name='John Smith')
		db.session.add(auth)
		db.session.commit()
		self.assertEqual( True, isinstance(auth.id, int))
	
	#################################################################################
	def _create_json_to_db_instance_from_dict(self, db, db_upload_dict):
		logger = MCLogger( 'test_log.text')
		j2db = JsonDBUpload( db, logger )
		j2db.update_tables_from_dict( db_upload_dict) 

	#################################################################################
	def _create_json_to_db_instance_from_file(self, db, db_file_name):
		logger = MCLogger( 'test_log.text')
		j2db = JsonDBUpload( db, logger )
		j2db.update_tables_from_file( db_file_name) 

	#################################################################################
	def test_json_insert_single(self):
		db_upload_dict = [
					{		
						"table_name":"author", 
						"data":[
									{"id":"", "name":"Harry"  },
									{"id":"", "name":"Sarah" },
									{"id":"", "name":"Julie"  } 
						]
					}
		]
		db = self._db_session()
		j2db = self._create_json_to_db_instance_from_dict( db, db_upload_dict )

		auth_list = db.session.query( db._models1.Author ).all()

		for index, item in enumerate(auth_list):
			self.assertEqual( auth_list[ index ].name, db_upload_dict[0]['data'][index ]['name'])
	
	#################################################################################
	def test_json_insert_linked_two_tables(self):
		db_upload_dict = [
					{		
						"table_name":"author", 
						"data":[
									{"id":"A_1", "name":"Homer"  },
									{"id":"A_2", "name":"Marge" },
									{"id":"A_3", "name":"Bart"  } 
						]
					},
					{		
						"table_name":"book", 
						"foreign_keys":[ { "author_id":"author.id"} ],
						"data":[
									{"id":"", "author_id":"A_1", "name":"Odessy"  },
									{"id":"", "author_id":"A_2", "name":"Cooking adventures" },
									{"id":"", "author_id":"A_3", "name":"Pranks"  } 
						]
					}
					
		]
		db = self._db_session()
		j2db = self._create_json_to_db_instance_from_dict( db, db_upload_dict )

		# auth_list = db.session.query( db._models.Author ).all()
		book_list = db.session.query( db._models1.Book ).all()

		for index, item in enumerate(book_list):
			# breakpoint()
			self.assertEqual( book_list[ index ].name, db_upload_dict[1]['data'][index ]['name'])
			self.assertEqual( book_list[ index ]._author.name, db_upload_dict[0]['data'][index ]['name'])
	
	

	#################################################################################
	def test_json_insert_linked_four_tables(self):
		db_upload_dict = [
					{		
						"table_name":"author", 
						"data":[
									{"id":"AA_1", "name":"James"  },
									{"id":"AA_2", "name":"Moneypenny" }
						]
					},
					{		
						"table_name":"book", 
						"foreign_keys":[ { "author_id":"author.id"} ],
						"data":[
									{"id":"BB_1", "author_id":"AA_1", "name":"Never say Never"  },
									{"id":"BB_2", "author_id":"AA_2", "name":"Goldeneye" }
						]
					},
					{		
						"table_name":"bookset", 
						"data":[
									{"id":"BS_1", "name":"Best of Bond"  }
						]
					},
					{		
						"table_name":"bookset_items", 
						"foreign_keys":[ { "bookset_id":"bookset.id"}, { "book_id":"book.id"},   ],
						"data":[
									{"id":"", "bookset_id":"BS_1", "book_id":"BB_1"  },
									{"id":"", "bookset_id":"BS_1", "book_id":"BB_2" }
						]
					},
					
					
					
		]
		db = self._db_session()
		j2db = self._create_json_to_db_instance_from_dict( db, db_upload_dict )

		# auth_list = db.session.query( db._models.Author ).all()
		booksets = db.session.query( db._models1.BooksetItems ).all()

		for index, item in enumerate(booksets):
			# breakpoint()
			self.assertEqual( booksets[ index ]._bookset.name , db_upload_dict[2]['data'][0 ]['name'])
			self.assertEqual( booksets[ index ]._book.name    , db_upload_dict[1]['data'][index ]['name'])
	
	
	#################################################################################
	def test_json_insert_from_file(self):
		db = self._db_session()
		j2db = self._create_json_to_db_instance_from_file( db, 'tests/db_config_defaults.json' )

		# auth_list = db.session.query( db._models.Author ).all()
		# booksets = db.session.query( db._models.BooksetItems ).all()

		# for index, item in enumerate(booksets):
		# 	# breakpoint()
		# 	self.assertEqual( booksets[ index ]._bookset.name , db_upload_dict[2]['data'][0 ]['name'])
		# 	self.assertEqual( booksets[ index ]._book.name    , db_upload_dict[1]['data'][index ]['name'])
	

if __name__ == '__main__':
    unittest.main()