import pandas as pd
import pyodbc
import logger
import numpy as np


class DBExecutor:

	def __init__(self): 
		self.conn = None
		self.log = logger.get_logger()

	def create_connection(self):
		if not self.conn: 
			self.log.info("Connecting to db. ")
			try:
				driver = 'driver'
				server = 'server'
				database = 'database'
				uid = 'userid'
				pwd = 'password'
				self.conn = pyodbc.connect(f"DRIVER={driver};SERVER={server};DATABASE={database};UID={uid};PWD={pwd}")
				self.log.info("Connection created successfully.")
			except ConnectionError:
				self.log.error(f"Connection to db failed: {ConnectionError}")
			except Exception as e:
				self.log.error(f"ERROR: {e}")
		else:
			self.log.warning("Connection already created. ")

	def execute_query(self, query):
		try:
			df = pd.read_sql(query, self.conn).replace(np.nan, 'NULL')
		except Exception as e:
			self.log.info(f"{e}")
		return df

	def close_connection(self):
		self.conn.close()
		self.log.info("Connection closed.")



new_conn = DBExecutor()
new_conn.create_connection()
surveys_query = f"""SELECT SurveyId FROM [Survey_Sample_A19].[dbo].[Survey]"""
questions_query = f"""SELECT * FROM ( 
										SELECT SurveyId, QuestionId, 1 as InSurvey
										FROM SurveyStructure 
										WHERE SurveyId = @currentSurveyId 
										UNION 
										SELECT @currentSurveyId as SurveyId, Q.QuestionId, 0 as InSurvey 
										FROM Question as Q 
										WHERE NOT EXISTS ( 
															SELECT * 
															FROM SurveyStructure as S
															WHERE S.SurveyId = @currentSurveyId 
															AND S.QuestionId = Q.QuestionId 
														) 
									) as t 
								ORDER BY QuestionId;"""

# @strQueryTemplateForAnswerColumn
query_template_for_answer_column = f""" COALESCE((
													SELECT a.Answer_Value 
													FROM Answer as a 
													WHERE a.UserId = u.UserId
													AND a.SurveyId = <SURVEY_ID> 
													AND a.QuestionId = <QUESTION_ID> 
												), -1) AS ANS_Q<QUESTION_ID> """

# @strQueryTemplateForNullColumnn
query_template_for_null_column = f""" NULL AS ANS_Q<QUESTION_ID> """

# @strQueryTemplateOuterUnionQuery
query_template_outer_union_query = f""" SELECT UserId , <SURVEY_ID> as SurveyId, <DYNAMIC_QUESTION_ANSWERS> 
										FROM [User] as u 
										WHERE EXISTS (
														SELECT * 
														FROM Answer as a 
														WHERE u.UserId = a.UserId 
														AND a.SurveyId = <SURVEY_ID>
													)"""


surveys = new_conn.execute_query(surveys_query)

# @strColumnsQueryPart
union_query_block = ''

final_query = ''
for index_s, survey_id in surveys.iterrows():
	questions = new_conn.execute_query(questions_query.replace('@currentSurveyId', str(survey_id['SurveyId'])))
	query_in_progress = ''
	for index_q, question_id in questions.iterrows():
		if question_id['InSurvey'] == 0:
			query_in_progress = query_in_progress + query_template_for_null_column.replace('<QUESTION_ID>', str(question_id['QuestionId']))
		else:
			query_in_progress = query_in_progress + query_template_for_answer_column.replace('<QUESTION_ID>', str(question_id['QuestionId']))

		if index_q != questions.index[-1]:
			query_in_progress = query_in_progress + ' , '

	# Replace <DYNAMIC_QUESTION_ANSWERS> with query_in_progress
	union_query_block = query_template_outer_union_query.replace('<DYNAMIC_QUESTION_ANSWERS>', query_in_progress)

	# Replace <SURVEY_ID> with survey_id['SurveyId']
	union_query_block = union_query_block.replace('<SURVEY_ID>', str(survey_id['SurveyId']))

	final_query = final_query + union_query_block
	if index_s != surveys.index[-1]:
		final_query = final_query + ' UNION '

final = new_conn.execute_query(final_query)

# Compare structure from db and csv
# if different trigger the function and store new structure into csv
# if same don't do anything???
db_survey_structure = new_conn.execute_query("SELECT * FROM [Survey_Sample_A19].[dbo].[SurveyStructure]")
current_survey_structure = pd.read_csv('current_survey_structure.csv')

if not current_survey_structure.equals(db_survey_structure):
	print("NOT EQUAL, downloading to Csv")
	db_survey_structure.to_csv('current_survey_structure.csv', index=False)

