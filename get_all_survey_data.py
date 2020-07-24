import pandas as pd
import logger
import sys
from db_executor import DBExecutor


class PivotSurveyData:

    def __init__(self, current_survey_structure='survey_structure.csv'):
        self.db = DBExecutor()
        self.survey_structure = current_survey_structure
        self.log = logger.get_logger()

    def get_pivoted_survey_data(self):
        self.db.create_connection()
        db_survey_structure = self.db.execute_pandas_query(self._get_query('survey_structure'))
        try:
            current_survey_structure = pd.read_csv(self.survey_structure)
        except FileNotFoundError:
            self.log.error(f"File {self.survey_structure} does not exists ")
            sys.exit(1)
        except pd.errors.EmptyDataError:
            current_survey_structure = pd.DataFrame()

        is_equal = self._compare_survey_structures(db_survey_structure, current_survey_structure)
        if not is_equal:
            fresh_data = self._get_new_data()
            self._create_or_alter_view(fresh_data)
            self._get_data_from_view()
            self._export_data_to_csv(db_survey_structure, self.survey_structure)
        else:
            self._get_data_from_view()
        self.db.close_connection()

    def _compare_survey_structures(self, db_structure, local_structure):
        """
        Compares survey structure from file against the db, if it is not the dame it downloads it into the csv
        :param db_structure: DataFrame with survey structure taken from the db
        :param local_structure: DataFrame with last known survey structure
        :return:  True if the structures are equal, False otherwise
        """
        if not local_structure.equals(db_structure):
            self.log.info(f"Survey structure data is not consistent with the table in the db")
            return False
        else:
            return True

    def _get_data_from_view(self):
        """
        Executes the query to get the survey data from the view vw_AllSurveyData in the database
        :return:
        """
        self.log.info("Getting data from view: vw_AllSurveyData ")
        view_data = self.db.execute_pandas_query(self._get_query('vw_survey_data'))
        self._export_data_to_csv(view_data, 'fresh_survey_data.csv')

    def _export_data_to_csv(self, source, target):
        """
        Exports data into a csv file
        :param source: Dataframe to be dumped into the csv
        :param target: csv file name to dump the data
        :return: None
        """
        self.log.info(f"Dumping data into {target}")
        source.to_csv(target, index=False)

    def _create_or_alter_view(self, survey_data):
        """
        Executes the query to alter the view vw_AllSurveyData
        :param survey_data: query to be executed with new structure
        :return: None
        """
        self.log.info("Creating or altering view vw_AllSurveyData ")
        edit_view = self._get_query('edit_view') + "( " + survey_data + " )"
        self.db.execute_query(edit_view)
        self.log.info("View was edited successfully")

    def _get_new_data(self):
        """
        Generates and returns a dynamic SQL query string for extracting the pivoted survey answer data
        :return: a string containing the query for fresh data
        """
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

            union_query_block = self._get_query('query_template_outer_union_query').replace('<DYNAMIC_QUESTION_ANSWERS>', query_in_progress)
            union_query_block = union_query_block.replace('<SURVEY_ID>', str(survey_id['SurveyId']))
            final_query = final_query + union_query_block
            if index_s != surveys.index[-1]:
                final_query = final_query + ' UNION '
        return final_query

    @staticmethod
    def _get_query(query_name):
        """
        Selects the right query to be run against
        :param query_name: the name of the query that is going to be executed
        :return: a string containing the query corresponding to the parameters provided
        """
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
