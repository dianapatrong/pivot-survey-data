import pandas as pd
import logger
from db_executor import DBExecutor


class TriggerStoredProcedure:

    def __init__(self):
        self.db = DBExecutor()
        self.log = logger.get_logger()

    def compare_survey_structure_table(self):
        self.db.create_connection()
        db_survey_structure = self.db.execute_pandas_query(self._get_query('survey_structure'))
        current_survey_structure = None
        try:
            current_survey_structure = pd.read_csv('current_survey_structure.csv')
        except FileNotFoundError:
            self.log.error("File current_survey_structure.csv does not exists ")

        if not current_survey_structure.equals(db_survey_structure):
            new_data = self._get_new_data()
            self._create_or_alter_view(new_data)
            self._get_data_from_view()
            self.log.info("Dumping new survey structure into csv")
            db_survey_structure.to_csv('current_survey_structure.csv', index=False)
        else:
            self._get_data_from_view()

    def _get_data_from_view(self):
        self.log.info("Getting data from view: vw_AllSurveyData ")
        view_data = self.db.execute_pandas_query(self._get_query('vw_survey_data'))
        self.log.info("Dumping view data into csv")
        view_data.to_csv('fresh_survey_data.csv', index=False)
        self.db.close_connection()

    def _create_or_alter_view(self, survey_data):
        self.log.info("Creating or altering view vw_AllSurveyData ")
        edit_view = self._get_query('edit_view') + "( " + survey_data + " )"
        self.log.info("view is to be edited yet")
        self.db.execute_query(edit_view)
        self.log.info("view was edited successfully")

    def _get_query(self, query_name):
        query_dict = {
            'survey_structure': f"""SELECT * FROM [Survey_Sample_A19].[dbo].[SurveyStructure]""",
            'surveys_query': f"""SELECT SurveyId FROM [Survey_Sample_A19].[dbo].[Survey]""",
            'questions_query': f"""SELECT * FROM (
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
                                            ORDER BY QuestionId;""",
            'query_template_for_answer_column': f""" COALESCE((
                                                                SELECT a.Answer_Value 
                                                                FROM Answer as a 
                                                                WHERE a.UserId = u.UserId
                                                                AND a.SurveyId = <SURVEY_ID> 
                                                                AND a.QuestionId = <QUESTION_ID> 
                                                                ), -1) AS ANS_Q<QUESTION_ID> """,
            'query_template_for_null_column': f""" NULL AS ANS_Q<QUESTION_ID> """,
            'query_template_outer_union_query': f""" SELECT UserId , <SURVEY_ID> as SurveyId, <DYNAMIC_QUESTION_ANSWERS> 
                                                    FROM [User] as u 
                                                    WHERE EXISTS (
                                                                    SELECT * 
                                                                    FROM Answer as a 
                                                                    WHERE u.UserId = a.UserId 
                                                                    AND a.SurveyId = <SURVEY_ID>
                                                                )""",
            'vw_survey_data': f"""SELECT * FROM [Survey_Sample_A19].[dbo].[vw_AllSurveyData]""",
            'edit_view': f"""CREATE OR ALTER VIEW vw_AllSurveyData AS """

        }
        return query_dict.get(query_name)

    def _get_new_data(self):
        self.log.info("Get new query from db ")
        surveys = self.db.execute_pandas_query(self._get_query('surveys_query'))

        final_query = ''
        for index_s, survey_id in surveys.iterrows():
            questions = self.db.execute_pandas_query(self._get_query('questions_query').replace('@currentSurveyId', str(survey_id['SurveyId'])))
            query_in_progress = ''
            for index_q, question_id in questions.iterrows():
                if question_id['InSurvey'] == 0:
                    query_in_progress = query_in_progress + self._get_query('query_template_for_null_column').replace('<QUESTION_ID>', str(question_id['QuestionId']))
                else:
                    query_in_progress = query_in_progress + self._get_query('query_template_for_answer_column').replace('<QUESTION_ID>', str(question_id['QuestionId']))

                if index_q != questions.index[-1]:
                    query_in_progress = query_in_progress + ' , '

            # Replace <DYNAMIC_QUESTION_ANSWERS> with query_in_progress
            union_query_block = self._get_query('query_template_outer_union_query').replace('<DYNAMIC_QUESTION_ANSWERS>', query_in_progress)

            # Replace <SURVEY_ID> with survey_id['SurveyId']
            union_query_block = union_query_block.replace('<SURVEY_ID>', str(survey_id['SurveyId']))

            final_query = final_query + union_query_block
            if index_s != surveys.index[-1]:
                final_query = final_query + ' UNION '
        return final_query



