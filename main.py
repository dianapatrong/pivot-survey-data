import subprocess


def main():
    from get_all_survey_data import PivotSurveyData

    data = PivotSurveyData()
    data.get_pivoted_survey_data()


if __name__ == '__main__':
    # This line will install all requirements listed in the requirements.txt file
    subprocess.call(['pip', 'install', '-r', 'requirements.txt'])
    main()
