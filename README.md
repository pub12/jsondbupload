# Json DB Upload: Upload to a database from a JSON file

Module enables a user to insert records into a database from a json file and also enables insertion into tables with foreign keys.  Hence, if you need to table A first, then key the primary keys from that to table B, this is possible from JsonDBUpload!


### What problem does this solve?
When you have a new application there are times where you need to insert some initial set of records.  Or there maybe a time where you need to synchronise data selectively between data stores of sorts. This is where this module can be of use.  It can help to insert records in any relational database which has a connection to it via sqlaclchemy

### How does it do this?
The module takes in an argument of a json file (or a list-dictionary), and then proceeds to insert records one by one.  The json file must contain the table name, and label and foreign keys.

There are two key paramaters:
1. A database session - this is from Flask SQLAlchemy
2. An optional logger module (e.g. such as https://pypi.org/project/mclogger/ )

### How to install?
JSONDBUpload is avaialble through PyPi or you may use git:

```
pip install jsondbupoad
```

Or, through git:
```
git clone https://github.com/pub12/jsondbupload.git
```

### How to use jsondbupload?
The module is relatively easy to use.  All that is required is a file, and a database session.  The file format is as follows:
```
[
	{		
		"table_name":"<table name>", 
		"foreign_keys":[ { "<field name in current table>":"<table name of foreign table>.<field name>"} ],
		"data":[
					{"<field name>":"<field value>", ... },
					....
		]
	}
]
```

There are three key fields:
* table_name: the name of the table to update
* foreign_keys: a list of foreign key lookup from current file - this entry is optional
* data: data to update

Here's a working example to update 3 tables.  Firstly this is the sqlalchmey schema:
```
class Author(db.Model):
	__tablename__ = 'author' 
	id = db.Column(db.Integer(), primary_key=True)
	name = db.Column(db.Integer() )

class Book(db.Model):
	__tablename__ = 'book' 
	id = db.Column(db.Integer(), primary_key=True)
	name = db.Column(db.Integer() )
	author_id = db.Column( db.Integer()  , db.ForeignKey( 'author.id'  ) )
	_author = db.relationship("Author", backref=db.backref("author" ), lazy='joined')


class Bookset(db.Model):
	__tablename__ = 'bookset' 
	id = db.Column(db.Integer(), primary_key=True)
	name = db.Column(db.Integer() )
```

And here's the json data to be used, this is in a file called `db_upload_file.json` (this can be any filename of course):
```
[
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
					{"id":"", "name":"Best of Bond"  }
		]
	}
]
```

In this example, we have:
* Updates to table `author` with two rows.  Please note, that in the database schema the field `id` is a primary key with automatic values so the entries will be ignored.
* Updates to table `book` has a foreign key linkage where `author_id` is supposed to link to table `book.id` and the temporary values are linked to the table `author` by the `foreign_keys` description.
* Update to table `bookset` is much simpler where the `name` field is specified, and the primary key `id` has no entry as any value given to it will be ignored anyway as it is a primary key.


Finally, this is the code:
```
from jsondbupload import JsonDBUpload
from flask import Flask
from flask_sqlalchemy import SQLAlchemy, Model

from mclogger import MCLogger

#Define the table methods
class Author(db.Model):
	__tablename__ = 'author' 
	id = db.Column(db.Integer(), primary_key=True)
	name = db.Column(db.Integer() )

class Book(db.Model):
	__tablename__ = 'book' 
	id = db.Column(db.Integer(), primary_key=True)
	name = db.Column(db.Integer() )
	author_id = db.Column( db.Integer()  , db.ForeignKey( 'author.id'  ) )
	_author = db.relationship("Author", backref=db.backref("author" ), lazy='joined')


class Bookset(db.Model):
	__tablename__ = 'bookset' 
	id = db.Column(db.Integer(), primary_key=True)
	name = db.Column(db.Integer() )
	
#Instantiate flask
app = Flask(__name__)
app.config['SQLALCHEMY_DATABASE_URI'] = 'sqlite:///test.db'
app.config['SQLALCHEMY_TRACK_MODIFICATIONS'] = False
db = SQLAlchemy(app)

#Create logger - this is from https://pypi.org/project/mclogger/
logger = MCLogger( 'test_log.text').getLogger()

#>>>> Two lines to ulaod json data!  It will also do the commit to the database.  The logger is optional and will show on screen what's happening under the hood.
j2db = JsonDBUpload( db, logger )
j2db.update_tables_from_file(  'db_upload_file.json' )
#>>>>>

#After inserts, this will print out the records updated
auth_list = db.session.query( Author ).all()
for item in auth_list:
	print( item.name )

```



### Class JsonDBUpload Methods overview

- #### __init__(db, logger=None)
	Create instance of the JsonDBUpload instance.  
	
	- *db*: A reference to the SQLAlchemy database object reference with link already open to the database
	- *logger*:  The logger is an optional entry of the module `logging` or any sub-class of that such as `mclogger`.  If provided, it'll show a color log in the console of all the inserts

- #### update_tables_from_file(filename )
	Update the database tables specfied in a given json file

	- *filename*: A string reference to the filename with relative or absolute path

- #### update_tables_from_dict( data )
	Update the database tables specfied in a given list of dictionaries

	- *data*: A list of dictionaries with table names for each.  Format must be as follows:
	```

	[
		{		
			"table_name":"<table name>", 
			"foreign_keys":[ { "<field name in current table>":"<table name of foreign table>.<field name>"} ],
			"data":[
						{"<field name>":"<field value>", ... },
						....
			]
		}
	]
	```

### Change Log
## Version 1.0.6
- Added ability to use single line and multiline comments in json file using jstyleson library
