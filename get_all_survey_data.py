import pandas as pd
import pyodbc
import logger
import numpy as np
from db_executor import DBExecutor

'''
class DBConfigLoader:
    def __init__(self, config_path="~/db_config.yml"):
        try:
            with open(config_path, "r") as config_file:
                self.db_config = yaml.load(config_file, Loader=yaml.FullLoader)
        except OSError:
            print("Configuration file doesn not exist or has incorrect format")
'''


class TriggerStoredProcedure:

    def __init__(self):
        self.db = DBExecutor()
        print("conn", self.db)

    def compare_survey_structure_table(self):
        print("CREATING CONNECTION IN compare_survey_structure_table")
        self.db.create_connection()
        db_survey_structure = self.db.execute_pandas_query(self._get_query('survey_structure'))
        try:
            current_survey_structure = pd.read_csv('current_survey_structure.csv')
        except FileNotFoundError:
            print("ERROR: File current_survey_structure.csv does not exists ")

        if not current_survey_structure.equals(db_survey_structure):
            new_data = self._get_new_data()
            self._create_or_alter_view(new_data)
            self._get_data_from_view()
            print("DOWNLOADING DB SURVEY STRUCTURE")
            db_survey_structure.to_csv('current_survey_structure.csv', index=False)
        else:
            self._get_data_from_view()
        self.db.close_connection()

    def _get_data_from_view(self):
        print("Getting data fron view: vw_AllSurveyData ")
        view_data = self.db.execute_pandas_query(self._get_query('vw_survey_data'))
        print("Dumping data into csv")
        view_data.to_csv('fresh_data.csv', index=False)

    def _create_or_alter_view(self, survey_data):
        print("Creating or altering view vw_AllSurveyData ")
        edit_view = self._get_query('edit_view') + "( " + survey_data + " )"
        print("view is to be edited yet")
        self.db.execute_query(edit_view)
        print("view was edited successfully")

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
            # @strQueryTemplateForAnswerColumn
            'query_template_for_answer_column': f""" COALESCE((
                                                                SELECT a.Answer_Value 
                                                                FROM Answer as a 
                                                                WHERE a.UserId = u.UserId
                                                                AND a.SurveyId = <SURVEY_ID> 
                                                                AND a.QuestionId = <QUESTION_ID> 
                                                                ), -1) AS ANS_Q<QUESTION_ID> """,
            # @strQueryTemplateForNullColumnn
            'query_template_for_null_column': f""" NULL AS ANS_Q<QUESTION_ID> """,
            # @strQueryTemplateOuterUnionQuery
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
        print("Get new query from db ")
        surveys = self.db.execute_pandas_query(self._get_query('surveys_query'))

        # @strColumnsQueryPart
        union_query_block = ''

        final_query = ''
        for index_s, survey_id in surveys.iterrows():
            questions = self.db.execute_pandas_query(self._get_query('questions_query').replace('@currentSurveyId', str(survey_id['SurveyId'])))
            query_in_progress = ''
            for index_q, question_id in questions.iterrows():
                if question_id['InSurvey'] == 0:
                    query_in_progress = query_in_progress + self._get_query('query_template_for_null_column').replace('<QUESTION_ID>', str(question_id['QuestionId']))
                else:
                    query_in_progress = query_in_progress + self._get_query('query_template_for_answer_column').replace('<QUESTION_ID>',
                                                                                                     str(question_id['QuestionId']))

                if index_q != questions.index[-1]:
                    query_in_progress = query_in_progress + ' , '

            # Replace <DYNAMIC_QUESTION_ANSWERS> with query_in_progress
            union_query_block = self._get_query('query_template_outer_union_query').replace('<DYNAMIC_QUESTION_ANSWERS>',
                                                                         query_in_progress)

            # Replace <SURVEY_ID> with survey_id['SurveyId']
            union_query_block = union_query_block.replace('<SURVEY_ID>', str(survey_id['SurveyId']))

            final_query = final_query + union_query_block
            if index_s != surveys.index[-1]:
                final_query = final_query + ' UNION '
        return final_query


def main():
    x = TriggerStoredProcedure()
    x.compare_survey_structure_table()


if __name__ == '__main__':
    main()


