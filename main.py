import subprocess
subprocess.call(['pip', 'install', '-r', 'requirements.txt'])


def main():
    from get_all_survey_data import PivotSurveyData

    data = PivotSurveyData()
    data.get_data_from_view()


if __name__ == '__main__':
    main()
