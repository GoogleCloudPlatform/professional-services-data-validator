""" A ResultHandler class is supplied to the DataValidation manager class.

    The execute function of any result handler is used to process
    the validation results.  It expects to receive the config
    for the vlaidation and Pandas DataFrame with the results
    from the validation run.
"""


class ResultHandler(object):
    def execute(self, config, result_df):
        print(result_df.to_string(index=False))

        return result_df
