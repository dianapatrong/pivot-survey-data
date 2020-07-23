from get_all_survey_data import TriggerStoredProcedure


def main():
    x = TriggerStoredProcedure()
    x.compare_survey_structure_table()


if __name__ == '__main__':
    main()
