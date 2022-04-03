import json

class JsonDBUpload(object):
	def __init__(self, db, logger=None):
		self.db = db
		self.logger = logger

		self.tables_dict = {table.__tablename__: table for table in db.Model.__subclasses__()}

	#########################################################################################################
	#	Load values from config files to database tables
	def update_tables_from_file(self, data_file):
		with open(data_file, 'r') as json_file:
			data = json.loads( json_file.read() )
		if self.logger: self.logger.debug( data )

		self.update_tables_from_dict(data)

	def update_tables_from_dict(self, data):
		updated_pks = {}  
		
		for table in data:		#Loop through each table record
			if 'table_name' in table:
				# table_class = self._dynamic_import( self._get_table_class_name( table['table_name'] ) ) 
				table_class =   self._get_table_class_name( table['table_name'] ) 	
				records = self.db.session.query( table_class).delete() 		#Delete any previous records
				self._add_table_records(  table , table_class, updated_pks) 

	def _get_table_class_name(self, table_name):
		return self.tables_dict.get(table_name)

	#########################################################################################################
	#	Add table records
	def _add_table_records(self,   table, table_class, updated_pks): 
		pkey_list = []
		updated_pks[ table['table_name'] ] = {}	#this will be used for feorieng key lookups later - the primary keys are auto incremented
		 
		for rec in table_class.__mapper__.primary_key:
			pkey_list.append(  rec.name ) 	#Get list of primary keys
			updated_pks[ table['table_name'] ][ rec.name ] = {}		#Initialize the primary key tables for later lookups
		
		for curr_table_record in table['data']:
			fkey_list = {}	#Get list of foriegn keys configured, if configured
			table_obj = table_class()	#Create instance of the table

			# table_obj
			if 'foreign_keys' in table: [ fkey_list.update(fkey) for fkey in table['foreign_keys'] ]
					
			if self.logger: self.logger.debug("processing:" + str(curr_table_record))
			self._add_table_record_fields( curr_table_record,  pkey_list, fkey_list, table_obj, updated_pks  )
			
			
			# for each item of primary key list, update the primary key with old and new value
			for pk in pkey_list:
				try:
					updated_pks[  table['table_name'] ][ pk ].update( { curr_table_record[pk] :   table_obj[pk] } )
				except:
					breakpoint()
					if self.logger: 
						# breakpoint()
						table_record_pk = curr_table_record.get(pk, 'undefined')
						table_obj_pk = table_obj.__dict__.get(pk, 'undefined' )
						self.logger.error(f"Error in updating primary key for table [{table['table_name']}] for key [{pk}] with fields:"\
													  f" curr_table_record[pk]=[{table_record_pk}] "\
													  f" table_obj[pk]=[{table_obj_pk}]")
						self.logger.error("Check previuos error to see record was successfully udpated or not")
					# assert(False)
					assert(False)

	#########################################################################################################
	#	Add table records
	def _add_table_record_fields( self, table_record,  pkey_list, fkey_list, table_obj, updated_pks): 
		#Now copy data values over field by field
		for field in table_record:
			if not field in table_obj.__mapper__.columns:  #Check if field exists
				if self.logger: self.logger.error(f"Key [{field}] does not exist in table [{table_obj.__class__.__name__}]")
				assert(False)
			else:
				val = table_record[field]
				# breakpoint()
				if field in pkey_list:	#If it's a primary key, skip as it'll auto-increment
					if self.logger: self.logger.debug(f'skipping primary_key:[{field}] for value [{val}] due to auto-gen ')
				elif field in fkey_list and val:		#If it's a forieng key, then get the foreign key updated value	
					
					lookup = fkey_list[field].split('.')	#value will be like 'Table.Field', hence spit by talbename and fieldname
					if self.logger: self.logger.debug(f"Lookup0:[{lookup[0]}] Lookup1:[{lookup[1]}] Field:[{field}] Value:[{val}]")
					
					# breakpoint()
					if not val in updated_pks[ lookup[0] ][ lookup[1] ]:  #check field exists
						if self.logger: self.logger.error(f"Key value [{val}] not found in foreign key [{lookup[0]}.{lookup[1]}] for local field [{table_obj.__class__.__name__}.{field}] ")
						# assert(False)

					new_val = updated_pks[ lookup[0] ][ lookup[1] ][ val ]	#Lookup the old value - lookup[0] = tablename, lookup[1] = field name
					# breakpoint()
					self._add_table_record_field_assign_value( table_obj, field, new_val )
				else:
					self._add_table_record_field_assign_value(table_obj, field, val)

		if self.logger: self.logger.info(f"Adding record {table_obj.to_str()}") 
		self._commit(table_obj)

	#########################################################################################################
	#	normalize any values to be udpated
	def _add_table_record_field_assign_value(self,  table_obj, field , new_value):
		update_value = new_value 

		# if field == 'arg_validation':
		# 	breakpoint()
		if table_obj.__mapper__.columns[field].type.python_type == bool:	#If type is boolean, then send boolean value
			if update_value in ['True', 'true']:
				update_value = True
			elif update_value in ['False', 'false']:
				update_value = False
		if table_obj.__mapper__.columns[field].type.python_type == int and update_value == '':
			update_value = None

		setattr(table_obj, field, update_value)

	#########################################################################################################
	#	Update database
	def _commit(self, obj ):
		try:
			self.db.session.add(obj)
			self.db.session.commit()
			self.db.session.refresh(obj) 
		except Exception as error:
			if self.logger: self.logger.error("DB Error:" + str(error)  )
			return False
		return True